# -*- coding: utf-8 -*-
'''
Buffer manager module for picoDAQ

Function based implementation instead of the original class implementation.
This is due to restriction of using complicated class objects as multi-threaded
process arguments in mac and windows environments. Note that also Python modules
are not approriate for windows processes, so also the usage of the buffer manager
instances must be changed to that they are not used as arguments in process/thread.

Author: Marko Manninen <elonmedia@gmail.com>
'''
#
from __future__ import print_function, division, unicode_literals, absolute_import

import numpy as np, sys, time, threading
from threading import Thread
from multiprocessing.queues import Queue as QueueX
from multiprocessing import Process, Array, Value, get_context
from multiprocessing.sharedctypes import RawValue, RawArray

from .mpBufManCntrl import *
from .mpOsci import *

class SharedCounter(object):
    """
    A synchronized shared counter

    The locking done by multiprocessing.Value ensures that only a single
    process or thread may read or write the in-memory ctypes object. However,
    in order to do n += 1, Python performs a read followed by a write, so a
    second process may read the old value before the new one is written by the
    first process. The solution is to use a multiprocessing.Lock to guarantee
    the atomicity of the modifications to Value.
    This class comes almost entirely from Eli Bendersky's blog:
    http://eli.thegreenplace.net/2012/01/04/shared-counter-with-pythons-multiprocessing/
    """

    def __init__(self, n=0):
        self.count = Value('i', n)

    def increment(self, n=1):
        """ Increment the counter by n (default = 1) """
        with self.count.get_lock():
            self.count.value += n

    @property
    def value(self):
        """ Return the value of the counter """
        return self.count.value


class Queue(QueueX):
    """
    A portable implementation of multiprocessing.Queue

    Because of multithreading / multiprocessing semantics, Queue.qsize() may
    raise the NotImplementedError exception on Unix platforms like Mac OS X
    where sem_getvalue() is not implemented. This subclass addresses this
    problem by using a synchronized shared counter (initialized to zero) and
    increasing / decreasing its value every time the put() and get() methods
    are called, respectively. This not only prevents NotImplementedError from
    being raised, but also allows us to implement a reliable version of both
    qsize() and empty().

    Note the implementation of __getstate__ and __setstate__ which help to
    serialize Queue when it is passed between processes. If these functions
    are not defined, MyQueue cannot be serialized, which will lead to the error
    of "AttributeError: 'Queue' object has no attribute 'size'".
    See the answer provided here: https://stackoverflow.com/a/65513291/9723036

    For documentation of using __getstate__ and __setstate__ to serialize objects,
    refer to here: https://docs.python.org/3/library/pickle.html#pickling-class-instances
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, ctx=get_context())
        self.size = SharedCounter(0)

    def __getstate__(self):
        """
        Help to make MyQueue instance serializable

        Note that we record the parent class state, which is the state of the
        actual queue, and the size of the queue, which is the state of Queue.
        self.size is a SharedCounter instance. It is itself serializable.
        """
        return {
            'parent_state': super().__getstate__(),
            'size': self.size
        }

    def __setstate__(self, state):
        super().__setstate__(state['parent_state'])
        self.size = state['size']

    def put(self, *args, **kwargs):
        super().put(*args, **kwargs)
        self.size.increment(1)

    def get(self, *args, **kwargs):
        item = super().get(*args, **kwargs)
        self.size.increment(-1)
        return item

    def qsize(self):
        """ Reliable implementation of multiprocessing.Queue.qsize() """
        return self.size.value

    def empty(self):
        """ Reliable implementation of multiprocessing.Queue.empty() """
        return not self.qsize()


verbose   = 1    # print detailed info if 1, 0 to print less info

NBuffers  = 16   # number of buffers
BMmodules = []   # display modules to start
LogFile   = None # log file name
flog      = None # file not yet open
logTime   = 60   # time between logging entries, once per 60 sec

DevConf   = None # picoscope device configuration
NChannels = None # number of channels in use
NSamples  = None # number of samples
TSampling = None # sampling interval

rawDAQproducer = None

CBMbuf     = None
CtimeStamp = None
CtrigStamp = None
BMbuf      = None
timeStamp  = None
trigStamp  = None
BMlock     = None

# global variables for producer statistics
ibufr    = RawValue('i', -1) # read index, synchronization with producer
Ntrig    = RawValue('i', 0)  # count number of readings
Ttrig    = RawValue('f', 0.) # time of last event
Tlife    = RawValue('f', 0.) # DAQ lifetime
readrate = RawValue('f', 0.) # current rate
lifefrac = RawValue('f', 0.) # current life-time
BMT0     = RawValue('d', 0.) # time of run-start

STARTED = RawValue('b', 0)
ACTIVE  = RawValue('b', 0)
RUNNING = RawValue('b', 0)
STOPPED = RawValue('b', 0)

# set up variables for buffer manager status and accounting
tPause  = 0. # time when last paused
dTPause = 0. # total time spent in paused state
tStop   = 0. # time when buffer manager is stopped

# consumer request to manageDataBuffer
# 0:  request event pointer, obligatory consumer
# 1:  request event data, random consumer
# 2:  request event data, obligatoray consumer
request_Ques = []
# data from manageDataBuffer to consumer
consumer_Ques = []

# keep track of sub-processes started by buffer manager
procs     = []
thrds     = []
mpQues    = []
logQ      = None
prod_Que  = None
kbdtxt    = ''

BMIinterval = 1000. # update interval in ms
maxBMrate   = 450.  # maximum buffer manager rate
start_manageDataBuffer = None


# 1st, remove pyhton 2 vs. python 3 incompatibility for keyboard input
if sys.version_info[:2] <= (2,7):
  get_input = raw_input
else:
  get_input = input


def BufferMan(BMdict, DevConfArg):
  '''
  A simple Buffer Manager

  Calls rawDAQproducer() to receive data from hardware device
  and stores them in the classes buffer space. Data are served
  from buffer to obligatory consumers (i.e. data qcquisition
  is paused until all consumcers are done) and random consumers
  (receive an event copy, data acquisition continues regardless
  of the consumers' progress)

  Args: configuration dictionary
        device Class
  '''
  global NBuffers, BMmodules, LogFile, verbose, logTime, DevConf, NChannels
  global NSamples, TSampling, rawDAQproducer, CBMbuf, CtimeStamp, CtrigStamp
  global BMbuf, timeStamp, trigStamp, prod_Que, BMlock
  # read configuration dictionary
  if "NBuffers" in BMdict:
    NBuffers = BMdict["NBuffers"]
  if "BMmodules" in BMdict:
    BMmodules = BMdict["BMmodules"]
  if "LogFile" in BMdict:
    LogFile = BMdict["LogFile"]
  if "verbose" in BMdict:
    verbose = BMdict["verbose"]
  if "logTime" in BMdict:
    logTime = BMdict["logTime"]
  # read device congiguration and set up Buffer space
  DevConf = DevConfArg
  NChannels = DevConf.NChannels
  NSamples = DevConf.NSamples
  TSampling = DevConf.TSampling
  # function collecting data from hardware device
  rawDAQproducer = DevConf.acquireDataBM
  # data structure for buffer manager in shared c-type memory
  CBMbuf = RawArray('f', NBuffers * NChannels * NSamples)
  CtimeStamp = RawArray('f', NBuffers)
  CtrigStamp = RawArray('i', NBuffers)
  # map to numpy arrays
  BMbuf = np.frombuffer(CBMbuf, 'f').reshape(NBuffers, NChannels, NSamples)
  timeStamp = np.frombuffer(CtimeStamp, 'f')
  trigStamp = np.frombuffer(CtrigStamp, 'f')
  # queues ( multiprocessing Queues for communication with sub-processes)
  # acquireData <-> manageDataBuffer
  prod_Que = Queue(NBuffers)
  # multiprocessing Queues for data transfer to subprocesses
  BMlock = threading.Lock()


# -- the raw data procuder
#
def acquireData():
  '''
  raw data procucer thread

  must run as a thread in the same process that initialized the hardware device

  - collects and stores data in buffers
  - provides all acquired data to manageDataBufer
  - count number of events and calculate life time

  Arg: funtion handling data acquisition from device

  Communicates with consumer via multiprocessing.Queue()
  '''
  global BMbuf, rawDAQproducer, Tlife, ACTIVE, verbose, RUNNING
  global timeStamp, trigStamp, Ttrig, Ntrig, prod_Que, NBuffers, ibufr
  prlog('*==* BufferManager.acquireData() starting')
  tlife = 0.
  # temporary variable
  ni = 0
  ts = time.time()
  # buffer index
  ibufw = -1
  while ACTIVE.value:
    # sample data from Picoscope handled by instance ps
    ibufw = (ibufw + 1) % NBuffers # next write buffer
    # wait for consumer done with this buffer
    while ibufw == ibufr.value:
      if not ACTIVE.value:
        return prlog('*==* BufferManager.acquireData() ended')
      time.sleep(0.0005)
    # wait for running status
    while not RUNNING.value:
      if not ACTIVE.value:
        return prlog('*==* BufferManager.acquireData() ended')
      time.sleep(0.01)

    # data acquisition from hardware
    e = rawDAQproducer(BMbuf[ibufw])
    if e == None:
      return prlog('*==* BufferManager.acquireData() ended')

    ttrg, tl = e
    tlife += tl
    Tlife.value += tl
    ttrg -= BMT0.value
    # store time when data became ready
    timeStamp[ibufw] = ttrg
    Ttrig.value = ttrg
    Ntrig.value += 1
    trigStamp[ibufw] = Ntrig.value
    prod_Que.put(ibufw)

    # wait for free buffer
    while prod_Que.qsize() == NBuffers:
      if not ACTIVE.value:
        return prlog('*==* BufferManager.acquireData() ended')
      time.sleep(0.0005)

    # calculate life time and read rate
    if (Ntrig.value - ni) == 10:
      dt = time.time() - ts
      ts += dt
      readrate.value = (Ntrig.value - ni) / dt
      lifefrac.value = (tlife / dt) * 100.
      tlife = 0.
      ni = Ntrig.value
  # --- end while
  prlog('*==* BufferManager.acquireData() ended')


def manageDataBuffer():
  '''
  main consumer runs as a thread or sub-process

  - receive data from procuder (acquireData):
  - provide all events for analysis to "obligatory" consumers
  - provide subset of events to "random" consumers (picoVMeter, oscilloscope)
  '''
  global ACTIVE, prod_Que, verbose, trigStamp, timeStamp, request_Ques
  global BMbuf, mpQues, logTime, ibufr
  t0 = time.time()
  n0 = 0
  n = 0
  while ACTIVE.value:
    # wait for pointer to data in producer queue
    while prod_Que.empty():
      if not ACTIVE.value:
        return prlog('*==* BufferManager.manageDataBuffer() ended 1')
      time.sleep(0.0005)
    ibufr.value = prod_Que.get()
    evNr = trigStamp[ibufr.value]
    evTime = timeStamp[ibufr.value]

    # check if other threads or sup-processes request data
    # next request treated as "done" for obligatory consumers
    l_obligatory = []
    if len(request_Ques):
      for i, Q in enumerate(request_Ques):
        if not Q.empty():
          req = Q.get()
        # return pointer to buffer
        if req == 0:
          consumer_Ques[i].put(ibufr.value)
          l_obligatory.append(i)
        # return a copy of data
        elif req == 1:
          consumer_Ques[i].put((evNr, evTime, BMbuf[ibufr.value]))
        # return copy and mark as obligatory
        elif req == 2:
          consumer_Ques[i].put((evNr, evTime, BMbuf[ibufr.value]))
          l_obligatory.append(i)
        else:
          prlog('!=! BufferManager.manageDataBuffer() invalid request mode', req)
          sys.exit(1)

    # provide data via multiprocessing queue at lower priority
    if len(mpQues):
      for Q in mpQues:
        # put an event in the Queue
        if Q.empty():
          Q.put((evNr, evTime, BMbuf[ibufr.value]))

    # wait until all obligatory consumers are done
    if len(l_obligatory):
      while ACTIVE.value:
        done = True
        for i in l_obligatory:
          if request_Ques[i].empty():
            done = False
        if done:
          break
        if not ACTIVE.value:
          return prlog('*==* BufferManager.manageDataBuffer() ended 2')
        time.sleep(0.0005)

    # signal to producer that all consumers are done with this event
    ibufr.value = -1

    # print event rate
    n += 1
    if time.time() - t0 >= logTime:
      t0 = time.time()
    prlog('evt %i:  rate: %.3gHz   life: %.2f%%' % (n, readrate.value, lifefrac.value))
    if evNr != n:
      prlog("!!! BufferManager.manageDataBuffer() error: ncnt != Ntrig: %i, %i" % (n, evNr))


def BMregister():
  '''
  register a client to buffer manager

  returns: client index
  '''
  global BMlock, request_Ques, consumer_Ques
  # called by many processes, needs protection...
  BMlock.acquire()
  request_Ques.append(Queue(1))
  consumer_Ques.append(Queue(1))
  client_index = len(request_Ques) - 1
  BMlock.release()
  prlog("*==* BMregister: new client id=%i" % client_index)
  return client_index


def BMregister_mpQ():
  '''
  register a subprocess to buffer manager

  copy of data will be transferred via a multiprocess Queue

  returns: client index, multiprocess Queue
  '''
  global mpQues
  mpQues.append(Queue(1))
  cid = len(mpQues) - 1
  prlog("*==* BufferManager.BMregister_mpQ() new subprocess client id=%i" % cid)
  return mpQues[-1]


def getEvent(client_index, mode = 1):
  '''
  request event from buffer manager

  encapsulates data access for obligatory and random clients

  Arguments:

  client_index:  index as returned by BMregister()
  mode:   0: event pointer (olbigatory consumer)
          1: copy of event data (random consumer)
          2: copy of event (olbigatory consumer)
  returns: event data
  '''
  global request_Ques, consumer_Ques, ACTIVE, trigStamp, timeStamp, BMbuf

  request_Ques[client_index].put(mode)
  cQ = consumer_Ques[client_index]

  while cQ.empty():
    if not ACTIVE.value:
      return
    time.sleep(0.0005)
  ibr = cQ.get()
  prlog('*==* getEvent: received event %i' % trigStamp[ibr])
  # received copy of the event data
  if mode != 0:
    return ibr
  # received pointer to event buffer
  else:
    evNr = trigStamp[ibr]
    evTime = timeStamp[ibr]
    evData = BMbuf[ibr]
    return evNr, evTime, evData


def add_process(name, target, args = ()):
  global procs
  procs.append(Process(name = name, target = target, args = args))


def start_processes():
  global procs, verbose
  for prc in procs:
    prc.daemon = True
    prc.start()
    if verbose:
      print('     BufferManager.start_processes() ', prc.name, ' PID=', prc.pid)


def add_thread(name, target, args = ()):
  global thrds
  thrd = Thread(name = name, target = target, args = args)
  thrd.daemon = True
  thrd.start()
  thrds.append(thrd)


def start():
  ''' set-up buffer manager processes '''
  global ACTIVE, STARTED, start_manageDataBuffer
  global logQ, procs, BMmodules, DevConf, BMIinterval, maxBMrate

  prlog('*==* BufferManager.start() acquisition threads')

  ACTIVE.value = True
  STARTED.value = False

  add_thread('acquireData', acquireData)
  # start manageDataBuffer as the last sub-process in run(),
  # connects daq producer and clients
  start_manageDataBuffer = True

  # buffer manager info and control always started
  logQ = Queue()

  add_process(
    'mpBufManCntrl',
    mpBufManCntrl,
    (getBMCommandQue(), logQ, getBMInfoQue(), maxBMrate, BMIinterval))

  # waveform display
  if 'mpOsci' in BMmodules:
    OSmpQ = BMregister_mpQ()
    add_process(
      'mpOsci',
      mpOsci,
      (OSmpQ, DevConf.OscConfDict, 100., 'event rate'))
  # start background processes
  start_processes()


def run():
  ''' start run - this must not be started before all clients have registered '''
  global STARTED, start_manageDataBuffer, flog, LogFile, verbose, BMT0, procs, RUNNING
  # start manageDataBuffer process and initialize run
  if STARTED.value:
    return prlog('*==* run already started - do nothing')

  tstart = time.time()
  if LogFile:
    datetime = time.strftime('%y%m%d-%H%M', time.localtime(tstart))
    flog = open(LogFile + '_' + datetime + '.log', 'w', 1)

  prlog('*==* BufferManager.run() T0')

  BMT0.value = tstart

  prlog('*==* BufferManager.run() running')

  # delayed start of manageDataBuffer
  if start_manageDataBuffer:
    procs.append(Process(name = 'manageDataBuffer', target = manageDataBuffer))
    procs[-1].start()
    start_manageDataBuffer = False
    if verbose:
      print('     BufferManager.run() process ', procs[-1].name, ' PID=', procs[-1].pid)

  STARTED.value = True
  RUNNING.value = True


def pause():
  ''' pause data acquisition - RUNNING flag evaluated by raw data producer '''
  global tPause, RUNNING, STOPPED, verbose, readrate
  if not RUNNING.value:
    return print('*==* BufferManager.pause() command recieved, but not running')
  if STOPPED.value:
    return print('*==* BufferManager.pause() from stopped state not possible')

  prlog('*==* BufferManager.pause()')

  RUNNING.value = False
  tPause = time.time()
  readrate.value = 0.


def resume():
  ''' resume data acquisition '''
  global RUNNING, STOPPED, verbose, dTPause, tPause

  if RUNNING.value:
    return print('*==* BufferManager.resume() command recieved, but already running')
  if STOPPED.value:
    return print('*==* BufferManager.resume() from Stopped state not possible')

  prlog('*==* BufferManager.resume()')

  RUNNING.value = True
  dTPause += (time.time() - tPause)
  tPause = 0.


def setverbose(vlevel):
  global verbose
  verbose = vlevel


def execCommand(c):
  ''' functions for buffer manager keyboard control '''
  if c == 'P':
    pause()
  elif c == 'R':
    resume()
  elif c == 'S':
    stop()
  elif c =='E':
    end()


def keyboardInput():
  ''' read keyboard input as thread, run in backround to avoid blocking '''
  global ACTIVE, kbdtxt
  while ACTIVE.value:
    kbdtxt = get_input(20 * ' ' + 'type -> E(nd), P(ause), S(top) or R(esume) + <ret> ')


def kbdCntrl():
  ''' control buffer manager via keyboard (stdin) '''
  global ACTIVE, kbdtxt

  kbdtxt = ''
  add_thread('keyboardInput', keyboardInput)

  while ACTIVE.value:
    if len(kbdtxt):
      cmd = kbdtxt
      kbdtxt = ''
      execCommand(cmd)
    time.sleep(0.1)


def readCommands_frQ(cmdQ):
  ''' a thread to receive control commands via multiprocessing queue '''
  global ACTIVE
  while ACTIVE.value:
    cmd = cmdQ.get()
    execCommand(cmd)
    time.sleep(0.1)


def getBMCommandQue():
  '''
  multiprocessing queue for commands

  starts a background process to read BMCommandQue

  returns: multiprocessing queue
  '''
  BMCommandQue = Queue(1)
  add_thread('BMreadCommand', readCommands_frQ, (BMCommandQue,))
  prlog("*==* BMCommandQue enabled")
  return BMCommandQue


def getStatus():
  '''
  collect status information - actual and integrated

  returns tuple:
    running status, number of events,
    time of last event, rate, life fraction and buffer level
  '''
  global tPause, dTPause, RUNNING, prod_Que, NBuffers, BMT0
  global Ntrig, Ttrig, Tlife, readrate, lifefrac
  bL = (prod_Que.qsize() * 100) / NBuffers
  if tPause != 0.:
    t = tPause
  else:
    t = time.time()
  return (RUNNING.value, t - BMT0.value - dTPause,
          Ntrig.value, Ttrig.value, Tlife.value,
          readrate.value, lifefrac.value, bL)


def getBMInfoQue():
  '''
  multiprocessing queue for status information

  starts a background process to fill BMInfoQue

  returns: multiprocess queue
  '''
  BMInfoQue = Queue(1)
  add_thread('BMreportStatus', reportStatus, (BMInfoQue,))
  prlog("*==* BMInfoQue enabled")
  return BMInfoQue


def reportStatus(Q):
  ''' report buffer manager status to a multiprocessing queue '''
  global STOPPED, prod_Que, BMIinterval
  while not STOPPED.value:
    if Q is not None and Q.empty() and prod_Que is not None:
      Q.put(getStatus())
    # onvert from milliseconds to seconds
    time.sleep(BMIinterval / 2000.)


def prlog(m):
  ''' send a message to screen, log queue, or file '''
  global flog, BMT0, logQ, verbose

  if verbose:
    # add time stamp to message
    t = time.time() - BMT0.value
    s = '%.2f ' % (t) + m
    if logQ is None or not logQ.empty():
      print(s)
    # is empty logQ
    else:
        logQ.put(s)
    # file log
    if flog != None:
      print(s, file = flog)


def print_summary():
  ''' prints end-of-run summary '''
  global flog, tStop, BMT0, dTPause, Ntrig, Tlife
  datetime = time.strftime('%y%m%d-%H%M', time.localtime(BMT0.value))
  if flog == None:
    flog = open('BMsummary_' + datetime + '.sum', 'w')

  print('')
  prlog('*==* BufferManager.print_summary()')
  print('')
  prlog('Trun=%.1fs  Ntrig=%i  Tlife=%.1fs\n' % (tStop - BMT0.value - dTPause, Ntrig.value, Tlife.value))

  flog.close()
  flog = None


def stop():
  '''
  put run in "stopped state" - BM processes remain active, no resume possible
  '''
  global BMT0, Ntrig, tStop, Tlife, tPause, dTPause, ACTIVE, RUNNING, STOPPED, verbose

  if not ACTIVE.value:
    return print('*==* BufferManager.stop() command recieved, but not running')

  # stopping from paused state
  if tPause != 0.:
    dTPause += (time.time() - tPause)

  if RUNNING.value:
    pause()

  tPause = 0.
  tStop = time.time()
  STOPPED.value = True
  # allow all events to propagate
  time.sleep(1.)

  prlog('*==* BufferManager.stop() reached stopped state')

  print_summary()

  if verbose:
    print('\n *==* BufferManager.stop() summary written')
    print('     Run Summary: Trun=%.1fs  Ntrig=%i  Tlife=%.1fs\n' % (tStop - BMT0.value - dTPause, Ntrig.value, Tlife.value))


def end():
  '''
  end BufferManager, stop all processes
  ACTIVE flag observed by all sub-processes
  '''
  global ACTIVE, RUNNING, procs, verbose

  if not ACTIVE.value:
    return print('*==* BufferManager.end() command recieved, but not active')
  # ending from RUNNING state, stop first
  if RUNNING.value:
    stop()

  ACTIVE.value = False
  time.sleep(0.3)

  # stop all sub-processes
  for prc in procs:
    if verbose:
      print('     BufferManager.end() terminating ' + prc.name)
    prc.terminate()
  time.sleep(0.3)


def __del__():
  global ACTIVE, RUNNING
  RUNNING.value = False
  ACTIVE.value  = False
