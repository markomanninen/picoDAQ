# -*- coding: utf-8 -*-
'''
BufferMan module for picoDAQ

Function based implementation instead of original class implementation.
This is due to restriction of using complicated class objects as multi-threaded
process arguments in mac and windows environments.

author: Marko Manninen <elonmedia@gmail.com>
'''
#
from __future__ import print_function, division, unicode_literals
from __future__ import absolute_import

# - class BufferMan
import numpy as np, sys, time, threading

from multiprocessing import Queue, Process, Array
from multiprocessing.sharedctypes import RawValue, RawArray

from .mpBufManCntrl import *
from .mpOsci import *

NBuffers  = 16   # number of buffers
BMmodules = []   # display modules to start
LogFile   = None
flog      = None # file not yet open
verbose   = 1    # print detailed info if 1, 0 to print less info
logTime   = 60   # time between logging entries, once per 60 sec

DevConf   = None
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

# global variables for producer statistics
ibufr    = RawValue('i', -1) # read index, synchronization with producer
Ntrig    = RawValue('i', 0)  # count number of readings
Ttrig    = RawValue('f', 0.) # time of last event
Tlife    = RawValue('f', 0.) # DAQ lifetime
readrate = RawValue('f', 0.) # current rate
lifefrac = RawValue('f', 0.) # current life-time
BMT0     = RawValue('d', 0.) # time of run-start

ACTIVE  = RawValue('b', 0)
RUNNING = RawValue('b', 0)
STOPPED = False

# set up variables for Buffer Manager status and accounting
tPause  = 0. # time when last paused
dTPause = 0. # total time spent in paused state

request_Ques = []  # consumer request to manageDataBuffer
           # 0:  request event pointer, obligatory consumer
           # 1:  request event data, random consumer
           # 2:  request event data, obligatoray consumer
consumer_Ques = [] # data from manageDataBuffer to consumer


# keep track of sub-processes started by BufferManager
procs     = [] # list of sub-processes started by BufferMan
thrds     = [] # list of sub-processes started by BufferMan
mpQues    = []
logQ      = None
prod_Que  = None

runStarted = None
BMIinterval = 1000.  # update interval in ms
start_manageDataBuffer = None
TStop = None


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
  # data structure for BufferManager in shared c-type memory
  CBMbuf = RawArray('f', NBuffers * NChannels * NSamples)
  CtimeStamp = RawArray('f', NBuffers )
  CtrigStamp = RawArray('i', NBuffers )
  #  ... and map to numpy arrays
  BMbuf = np.frombuffer(CBMbuf, 'f').reshape(NBuffers, NChannels, NSamples)
  timeStamp = np.frombuffer(CtimeStamp, 'f')
  trigStamp = np.frombuffer(CtrigStamp, 'f')
  # queues ( multiprocessing Queues for communication with sub-processes)
  prod_Que = Queue(NBuffers) # acquireData <-> manageDataBuffer
  # multiprocessing Queues for data transfer to subprocesses
  BMlock = threading.Lock()


# -- the raw data procuder
#   must run as a thread in the same process that initialized the hardware device
def acquireData():
  '''
   Procucer Thread

     - collects and stores data in buffers
     - provides all acquired data to manageDataBufer
     - count number of events and calculate life time

     Arg: funtion handling data acquisition from device

  Communicates with consumer via multiprocessing.Queue()
  '''
  global BMbuf, rawDAQproducer, Tlife, ACTIVE, verbose, RUNNING
  global timeStamp, trigStamp, Ttrig, Ntrig, prod_Que, NBuffers
  prlog('*==* BufMan:  !!! acquireData starting')
  tlife = 0.
  ni = 0 # temporary variable
  ts = time.time()
  ibufw = -1   # buffer index
  while ACTIVE.value:
  # sample data from Picoscope handled by instance ps
    ibufw = (ibufw + 1) % NBuffers # next write buffer
    while ibufw == ibufr.value:  # wait for consumer done with this buffer
      if not ACTIVE.value:
        if verbose:
          prlog ('*==* BufMan.acquireData()  ended')
        return
      time.sleep(0.0005)
    while not RUNNING.value:   # wait for running status
      if not ACTIVE.value:
        if verbose:
          prlog('*==* BufMan.acquireData()  ended')
        return
      time.sleep(0.01)

  # data acquisition from hardware
    e = rawDAQproducer(BMbuf[ibufw])
    if e == None:
      if verbose:
        prlog('*==* BufMan.acquireData()  ended')
      return

    ttrg, tl = e
    tlife += tl
    Tlife.value += tl
    ttrg -= BMT0.value
    timeStamp[ibufw] = ttrg  # store time when data became ready
    Ttrig.value = ttrg
    Ntrig.value += 1
    trigStamp[ibufw] = Ntrig.value
    prod_Que.put(ibufw)

  # wait for free buffer
    while prod_Que.qsize() == NBuffers:
      if not ACTIVE.value:
        if verbose:
          prlog('*==* BufMan.acquireData()  ended')
        return
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
  if verbose:
    prlog('*==* BufMan.acquireData()  ended')


# -- the central Buffer Manager - runs as a thread or sub-process
def manageDataBuffer():
  '''main Consumer Thread

     - receive data from procuder (acquireData):
     - provide all events for analysis to "obligatory" consumers
     - provide subset of events to "random" consumers (picoVMeter, oscilloscope)

  '''
  global ACTIVE, prod_Que, verbose, trigStamp, timeStamp, request_Ques
  global BMbuf, mpQues, logTime
  t0 = time.time()
  n0 = 0
  n = 0
  while ACTIVE.value:
    # wait for pointer to data in producer queue
    while prod_Que.empty():
      if not ACTIVE.value:
        if verbose:
          prlog('*==* BufMan ended')
        return
      time.sleep(0.0005)
    ibufr.value = prod_Que.get()
    evNr = trigStamp[ibufr.value]
    evTime = timeStamp[ibufr.value]

  # check if other threads or sup-processes request data
  #   next request treated as "done" for obligatory consumers
    l_obligatory = []
    if len(request_Ques):
      for i, Q in enumerate(request_Ques):
        if not Q.empty():
          req = Q.get()
        if req == 0: # return poiner to Buffer
          consumer_Ques[i].put(ibufr.value)
          l_obligatory.append(i)
        elif req == 1: # return a copy of data
          consumer_Ques[i].put((evNr, evTime, BMbuf[ibufr.value]))
        elif req == 2: # return copy and mark as obligatory
          consumer_Ques[i].put((evNr, evTime, BMbuf[ibufr.value]))
          l_obligatory.append(i)
        else:
          prlog('!=! manageDataBuffer: invalid request mode', req)
          sys.exit(1)

    # provide data via a mp-Queue at lower priority
    if len(mpQues):
      # only if Buffer is not full
      # if len(mpQues) and prod_Que.qsize() <= NBuffers/2 :
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
          if verbose:
            prlog('*==* BufMan ended')
          return
        time.sleep(0.0005)

    #  signal to producer that all consumers are done with this event
    ibufr.value = -1

    # print event rate
    n += 1
    if time.time() - t0 >= logTime:
      t0 = time.time()
    if verbose:
      prlog('evt %i:  rate: %.3gHz   life: %.2f%%' % (n, readrate.value, lifefrac.value))
    if evNr != n:
      prlog("!!! manageDataBuffer error: ncnt != Ntrig: %i, %i" % (n, evNr))
  # end while ACTIVE
  if verbose:
    prlog('*==* BufMan ended')


# -- helper functions for interaction with BufferManager
def BMregister():
  '''
  register a client to Buffer Manager

  Returns: client index
  '''
  global BMlock, request_Ques, consumer_Ques, verbose
  BMlock.acquire() # called by many processes, needs protection ...
  request_Ques.append(Queue(1))
  consumer_Ques.append(Queue(1))
  client_index = len(request_Ques) - 1
  BMlock.release()

  if verbose:
    prlog("*==* BMregister: new client id=%i" % client_index)
  return client_index


# multiprocessing Queue
def BMregister_mpQ():
  '''
  register a subprocess to Buffer Manager

  copy of data will be transferred via a multiprocess Queue

  Returns: client index
           multiprocess Queue
  '''
  global mpQues, verbose
  mpQues.append(Queue(1))
  cid = len(mpQues) - 1

  if verbose:
    prlog("*==* BMregister_mpQ: new subprocess client id=%i" % cid)
  return cid, mpQues[-1]


# -- encapsulates data access for obligatory and random clients
def getEvent(client_index, mode = 1):
  '''
  request event from Buffer Manager

    Arguments:

    client_index client:  index as returned by BMregister()
    mode:   0: event pointer (olbigatory consumer)
            1: copy of event data (random consumer)
            2: copy of event (olbigatory consumer)
    Returns: event data
  '''
  global request_Ques, consumer_Ques, ACTIVE, trigStamp, timeStamp, BMbuf

  request_Ques[client_index].put(mode)
  cQ = consumer_Ques[client_index]
  
  while cQ.empty():
    if not ACTIVE.value:
      return
    time.sleep(0.0005)
  prlog('*==* getEvent: received event %i' % evNr)
  if mode != 0: # received copy of the event data
    return cQ.get()
  # received pointer to event buffer
  else:
    ibr = cQ.get()
    evNr = trigStamp[ibr]
    evTime = timeStamp[ibr]
    evData = BMbuf[ibr]
    return evNr, evTime, evData


# Run control fuctions
# set-up Buffer Manager processes
def start():
  global verbose, ACTIVE, runStarted, start_manageDataBuffer
  global logQ, procs, BMmodules, DevConf
  if verbose > 1:
    prlog('*==* BufferMan  starting acquisition threads')
  ACTIVE.value = True
  runStarted = False

  thr_acquireData = threading.Thread(target = acquireData)
  thr_acquireData.setName('acquireData')
  thr_acquireData.daemon = True
  thr_acquireData.start()
  thrds.append(thr_acquireData)
  # test: try as sub-process
  #  prc_acquireData=Process(name='acquireData', target=acquireData)
  #  prc_cquireData.start()

  #  thr_manageDataBuffer=threading.Thread(target=manageDataBuffer)
  #  thr_manageDataBuffer.setName('manageDataBuffer')
  #  thr_manageDataBuffer.daemon=True
  #  thr_manageDataBuffer.start()

  # start manageDataBuffer as the last sub-process in run(),
  #   connects daq producer and clients)
  start_manageDataBuffer = True

  # BufferMan Info and control always started
  logQ = Queue()
  maxBMrate = 450.
  #BMIinterval = 1000.  # update interval in ms
  procs.append(
    Process(name = 'BufManCntrl',
            target = mpBufManCntrl,
            args = (getBMCommandQue(), logQ,     getBMInfoQue(), maxBMrate, BMIinterval)))
  #                 cmdQ               BM_logQue BM_InfoQue      max_rate   update_interval

  # waveform display
  if 'mpOsci' in BMmodules:
    OScidx, OSmpQ = BMregister_mpQ()
    procs.append(
      Process(name = 'Osci',
              target = mpOsci,
              args = (OSmpQ, DevConf.OscConfDict, 100., 'event rate')))
  #                                               interval
  # start BufferMan background processes
  for prc in procs:
    # prc.deamon = True
    prc.start()
    if verbose:
      print('    BufferMan: starting process ', prc.name, ' PID =', prc.pid)


# start run - this must not be started before all clients have registred
def run():
  global runStarted, start_manageDataBuffer, flog, LogFile, verbose, BMT0, procs, RUNNING
  # start manageDataBuffer process and initialize run
  if runStarted:
    prlog('*==* run already started - do nothing')
    return

  tstart = time.time()
  if LogFile:
    datetime = time.strftime('%y%m%d-%H%M', time.localtime(tstart))
    flog = open(LogFile + '_' + datetime + '.log', 'w', 1)

  if verbose:
    prlog('*==* BufferMan T0')

  BMT0.value = tstart

  if verbose:
    prlog('*==* BufferMan start running')

  if start_manageDataBuffer: # delayed start of manageDataBuffer
    procs.append(Process(name = 'manageDataBuffer', target = manageDataBuffer))
    procs[-1].start()
    start_manageDataBuffer = False
    if verbose:
      print('    BufferMan: starting process ', procs[-1].name, ' PID =', procs[-1].pid)

  runStarted = True
  RUNNING.value = True


# pause data acquisition - RUNNING flag evaluated by raw data producer
def pause():
  global tPause, RUNNING, STOPPED, verbose, readrate
  if not RUNNING.value:
    print('*==* BufferMan: Pause command recieved, but not running')
    return
  if STOPPED:
    print('*==* BufferMan: Pause from Stopped state not possible')
    return
  if verbose:
    prlog('*==* BufferMan  pause')

  RUNNING.value = False
  tPause = time.time()
  readrate.value = 0.


# resume data acquisition
def resume():
  global tPause, dTPause, RUNNING, STOPPED, verbose, dTPause, tPause
  if RUNNING.value:
    print('*==* BufferMan: Resume command recieved, but already running')
    return
  if STOPPED:
    print('*==* BufferMan: Resume from Stopped state not possible')
    return

  if verbose:
    prlog('*==* BufferMan  resume')

  RUNNING.value = True
  dTPause += (time.time() - tPause)
  tPause = 0.


def setverbose(vlevel):
  global verbose
  verbose = vlevel


# functions for Buffer Manager control
def execCommand(c):
  if c == 'P':
    pause()
  elif c == 'R':
    resume()
  elif c == 'S':
    stop()
  elif c =='E':
    end()


def kbdin():
  '''
    read keyboard input, run as backround-thread to aviod blocking
  '''
  global ACTIVE
  # 1st, remove pyhton 2 vs. python 3 incompatibility for keyboard input
  if sys.version_info[:2] <= (2,7):
    get_input = raw_input
  else:
    get_input = input
  # keyboard input as thread
  while ACTIVE.value:
    kbdtxt = get_input(30 * ' ' + 'type -> E(nd), P(ause), S(top) or R(esume) + <ret> ')


def kbdCntrl():
  '''
    Control Buffer Manager via keyboard (stdin)
  '''
  global ACTIVE
  kbdtxt = ''
  # set up a thread to read from keyboad without blocking
  kbd_thrd = threading.Thread(target = kbdin)
  kbd_thrd.daemon = True
  kbd_thrd.start()
  thrds.append(kbd_thrd)

  while ACTIVE.value:
    if len(kbdtxt):
      cmd = kbdtxt
      kbdtxt = ''
      execCommand(cmd)
    time.sleep(0.1)


def readCommands_frQ(cmdQ):
  global ACTIVE
  # a thread to receive control commands via a mp-Queue
  while ACTIVE.value:
    cmd = cmdQ.get()
    execCommand(cmd)
    time.sleep(0.1)


def getBMCommandQue():
  '''multiprocessing Queue for commands

     starts a background process to read BMCommandQue

     Returns: multiprocess Queue
  '''
  global verbose
  BMCommandQue = Queue(1)
  # start a background thread for reporting
  thr_BMCommandQ = threading.Thread(target = readCommands_frQ, args = (BMCommandQue,)  )
  thr_BMCommandQ.daemon = True
  thr_BMCommandQ.setName('BMreadCommand')
  thr_BMCommandQ.start()
  thrds.append(thr_BMCommandQ)

  if verbose:
    prlog("*==* BMCommandQue enabled")

  return BMCommandQue


# collect status information - actual and integrated
def getStatus():
  ''' Returns:
      tuple: Running status, number of events,
         time of last event, rate, life fraction and buffer level
  '''
  global tPause, dTPause, RUNNING, prod_Que, NBuffers, BMT0
  global Ntrig, Ttrig, Tlife, readrate, lifefrac
  bL = (prod_Que.qsize() * 100) / NBuffers
  stat = RUNNING.value
  if tPause != 0.:
    t = tPause
  else:
    t = time.time()
  return (stat, t - BMT0.value - dTPause,
          Ntrig.value, Ttrig.value, Tlife.value,
          readrate.value, lifefrac.value, bL)


# mp-Qeueu for information, starts getStatus as thread
def getBMInfoQue():
  '''multiprocessing Queue for status information

     starts a background process to fill BMInfoQue

     Returns: multiprocess Queue
  '''
  global verbose
  BMInfoQue = Queue(1)
  # start a background thread for reporting
  thr_BMInfoQ = threading.Thread(target = reportStatus, args = (BMInfoQue,))
  thr_BMInfoQ.daemon = True
  thr_BMInfoQ.setName('BMreportStatus')
  thr_BMInfoQ.start()
  thrds.append(thr_BMInfoQ)

  if verbose:
    prlog("*==* BMInfoQue enabled")
  return BMInfoQue


def reportStatus(Q):
  '''report Buffer manager staus to a multiprocessing Queue'''
  global STOPPED, prod_Que, BMIinterval
  while not STOPPED:
    if Q is not None and Q.empty() and prod_Que is not None:
      Q.put(getStatus())
    time.sleep(BMIinterval / 2000.) # ! convert from ms to s


def prlog(m):
  ''' send a Message, to screen or to LogQ'''
  global dTPause, flog, BMT0, logQ
  t = time.time() - BMT0.value # add time stamp to message
  s = '%.2f ' % (t) + m
  if logQ is None:
    print(s)
  else:
    if logQ.empty():
      logQ.put(s)
    else:
      print(s)
  if flog != None:
    print(s, file = flog)


# prints end-of-run summary
def print_summary():
  global flog, TStop, BMT0, dTPause, Ntrig, Tlife
  datetime = time.strftime('%y%m%d-%H%M', time.localtime(BMT0.value))
  if flog == None:
    flog = open('BMsummary_' + datetime + '.sum', 'w')
  prlog('Run Summary: started ' + datetime)
  prlog('  Trun=%.1fs  Ntrig=%i  Tlife=%.1fs\n' % (TStop - BMT0.value - dTPause, Ntrig.value, Tlife.value))
  flog.close()
  flog = None


# put run in "stopped state" - BM processes remain active, no resume possible
def stop():
  global BMT0, Ntrig, TStop, Tlife, tPause, dTPause, ACTIVE, RUNNING, STOPPED, procs, verbose
  if not ACTIVE.value:
    print('*==* BufferMan: Stop command recieved, but not running')
    return

  if tPause != 0. :  # stopping from paused state
    dTPause += (time.time() - tPause)
  if RUNNING.value:
    pause()
  tPause = 0.

  TStop = time.time()
  STOPPED = True

  time.sleep(1.) # allow all events to propagate

  if verbose:
    prlog('*==* BufferMan ending - reached stopped state')

  print_summary()

  if verbose:
    print('\n *==* BufferMan ending, RunSummary written')
    print('  Run Summary: Trun=%.1fs  Ntrig=%i  Tlife=%.1fs\n' % (TStop - BMT0.value - dTPause, Ntrig.value, Tlife.value))


# end BufferManager, stop all processes
# ACTIVE flag observed by all sub-processes
def end():
  global ACTIVE, RUNNING, procs, verbose
  if not ACTIVE.value:
    print('*==* BufferMan: End command recieved, but not active')
    return

  if RUNNING.value: # ending from RUNNING state, stop first
    stop()

  ACTIVE.value = False
  time.sleep(0.3)

  # stop all sub-processes
  for prc in procs:
    if verbose:
      print('  BufferMan: terminating ' + prc.name)
    prc.terminate()
  time.sleep(0.3)


def __del__():
  global ACTIVE, RUNNING
  RUNNING.value = False
  ACTIVE.value  = False
