#!/usr/bin/python
# -*- coding: utf-8 -*-
# script runDAQ.py
'''
  **runDAQ** run Data Aquisition with Picoscpe

  Based on python drivers by Colin O'Flynn and Mark Harfouche,
  https://github.com/colinoflynn/pico-python

  relies on package *picodaqa*:
    - instance BM of BufferManager class and
    - device initialisation as defined in picoConfig class

  tested with  PS2000a, PS3000a and PS4000

  Functions:

    - set up PicoScope channel ranges and trigger
    - PicoScope configuration optionally from yaml file
    - acquire data (implemented as thread)
    - manage event data and distribute to obligatory and random consumers
    - analyse and plot data:

      - obligatoryConsumer test speed of data acquisition
      - randomConsumer     test concurrent access
      - VMeter             effective Voltages with bar graph display
      - Osci               simple waveform display
      - RMeter             displays event rate as a function of time
      - Histogram          histogramms event variables

  graphics implemented with matplotlib

  For Demo Mode: Connect output of signal generator to channel B,
  and an open cable to Channel A
'''

from __future__ import print_function, division, unicode_literals, absolute_import

import sys, time, yaml, numpy as np, threading
#from multiprocessing import Process, Queue
import multiprocessing as mp

# import relevant pieces from picodaqa
import picodaqa.picoConfig
#import picodaqa.BufferMan as BMan
import picodaqa.BufferManF as BM

# animated displays running as background processes/threads
from picodaqa.mpOsci import mpOsci
from picodaqa.mpVMeter import mpVMeter
from picodaqa.mpRMeter import mpRMeter
from picodaqa.read_config import read_yaml_configuration,\
                                 read_yaml_configuration_with_argv

# !!!!
# import matplotlib.pyplot as plt
# !!!! matplot can only be used if no other thread using it is active

# --------------------------------------------------------------
#     scope settings defined in .yaml-File, see picoConfig
# --------------------------------------------------------------

def obligConsumer(cId):
  '''
    test readout speed: do nothing, just request data from buffer manager
      - an example of an obligatory consumer, sees all data
        (i.e. data acquisition is halted when no data is requested)

      Args:
        BM:   Buffer Manager instance
        cId:  Buffer Manager client id (from main process)

  '''
  global BM
  if not BM.ACTIVE.value:
    sys.exit(1)
# register with Buffer Manager
  mode = 0    # obligatory consumer, data in evdata transferred as pointer

  evcnt=0
  while BM.ACTIVE.value:
    e = BM.getEvent(cId, mode = mode)
    if e != None:
      evNr, evtime, evData = e
      evcnt += 1
      print('*==* obligConsumer: event Nr %i, %i events seen' % (evNr, evcnt))

#    introduce random wait time to mimick processing activity
    time.sleep(-0.25 * np.log(np.random.uniform(0.,1.)) )
  return
#-end def obligComsumer

def randConsumer(cId):
  '''
    test readout speed:
      does nothing except requesting random data samples from buffer manager
  '''
  global BM
  if not BM.ACTIVE.value:
    sys.exit(1)
  mode = 1    # random consumer, eventdata as copy

  evcnt=0
  while BM.ACTIVE.value:
    e = BM.getEvent(cId, mode=mode)
    if e != None:
      evNr, evtime, evData = e
      evcnt+=1
      print('*==* randConsumer: event Nr %i, %i events seen' % (evNr, evcnt))
# introduce random wait time to mimick processing activity
    time.sleep(np.random.randint(100,1000)/1000.)
# - end def randConsumer()
  return


def subprocConsumer(Q):
  '''
    test consumer in subprocess using simple protocol
      reads event data from multiprocessing.Queue()
  '''
  global BM
  if not BM.ACTIVE.value:
    sys.exit(1)

  cnt = 0
  try:
    while True:
      evN, evT, evBuf = Q.get()
      cnt += 1
      print('*==* mpQ: got event %i'%(evN) )
      if cnt <= 3:
        print('     event data \n', evBuf)
      time.sleep(1.)
  except:
    print('subprocConsumer: signal recieved, ending')

# some helper functions

def stop_processes(proclst):
  '''
  Close all running processes at end of run
  '''
  # stop all sub-processes
  for p in proclst:
    if p.is_alive():
      print('    terminating ' + p.name)
      p.terminate()
      time.sleep(1.)

if __name__ == "__main__": # - - - - - - - - - - - - - - - - - - - - - -

  print('\n*==* script ' + sys.argv[0] + ' running \n')

  DAQconfdict = read_yaml_configuration_with_argv('DAQconfig.yaml')

  if "DeviceFile" in DAQconfdict:
    DeviceFile = DAQconfdict["DeviceFile"] # configuration file for scope
  else:
    print('     no device configuration file - exiting')
    exit(1)

  if "BMfile" in DAQconfdict:
    BMfile = DAQconfdict["BMfile"] # Buffer Manager configuration file
  else:
    print('     no BM configuration file - exiting')
    exit(1)

  if "ANAscript" in DAQconfdict:
    ANAscript = DAQconfdict["ANAscript"] # configuration file for user analysis
  else:
    ANAscript = None

  if 'DAQmodules' in DAQconfdict:
    modules = DAQconfdict["DAQmodules"]
  else:
    modules = []
  if "verbose" in DAQconfdict:
    verbose = DAQconfdict["verbose"]
  else:
    verbose = 1 # print (detailed) info if >0

  # read scope configuration file
  print('    Device configuration from file ' + DeviceFile)
  try:
    PSconfdict = read_yaml_configuration(DeviceFile)
  except:
    print('     failed to read scope configuration file ' + DeviceFile)
    exit(1)

  # read Buffer Manager configuration file
  try:
    BMconfdict = read_yaml_configuration(BMfile)
  except:
    print('     failed to read BM input file ' + BMfile)
    exit(1)

  # initialisation
  print(' -> initializing PicoScope')

  # configure and initialize PicoScope
  PSconf = picodaqa.picoConfig.PSconfig(PSconfdict)
  PSconf.init()
  # copy some of the important configuration variables
  NChannels = PSconf.NChannels # number of channels in use
  TSampling = PSconf.TSampling # sampling interval
  NSamples  = PSconf.NSamples  # number of samples

  # configure Buffer Manager  ...
  print(' -> initializing BufferMan')
  #BM = BMan.BufferMan(BMconfdict, PSconf)
  BM.BufferMan(BMconfdict, PSconf)
  # ... tell device what its buffer manager is ...
  PSconf.setBufferManagerPointer(BM)

  # ... and start data acquisition thread.
  if verbose:
    print(" -> starting Buffer Manager Threads")
  BM.start() # set up buffer manager processes

  if 'DAQmodules' in BMconfdict:
    modules = modules + BMconfdict["DAQmodules"]

  # list of modules (= backgound processes) to start
  if type(modules) != list:
    modules = [modules]
# modules to be run as sub-processes
# these use multiprocessing.Queue for data transfer
  thrds = []
  procs = []

  # rate display
  if 'mpRMeter' in modules:
    RMcidx, RMmpQ = BM.BMregister_mpQ()
    procs.append(mp.Process(name = 'RMeter', target = mpRMeter,
                 args=(RMmpQ, 75.,    2500.,   'trigger rate history') ) )
#                      queue  maxRate interval name

  # Voltmeter display
  if 'mpVMeter' in modules:
    VMcidx, VMmpQ = BM.BMregister_mpQ()
    procs.append(mp.Process(name = 'VMeter', target = mpVMeter,
                 args=(VMmpQ, PSconf.OscConfDict, 500.,    'effective Voltage') ) )
#                      queue  configuration       interval name

# ---> put your own code here

  if ANAscript:
    try:
      print('    including user analysis from file ' + ANAscript )
      exec( open(ANAscript).read() )
    except:
      print('     failed to read analysis script ' + ANAscript)
      exit(1)

# <---

  if len(procs)==0 and len(thrds)==0 :
    print ('!!! nothing to do - running BM only')
# start all background processes
  for prc in procs:
    prc.deamon = True
    prc.start()
    print(' -> starting process ', prc.name, ' PID=', prc.pid)
  time.sleep(1.)
# start threads
  for thrd in thrds:
    thrd.daemon = True
    thrd.start()
  time.sleep(1.) # wait for all threads to start, then ...
# ...start run
  BM.run()

# --- LOOP
  try:
# ->> read keyboard (control Buffermanager) <<-
    BM.kbdCntrl()
    print(sys.argv[0]+': End command received - closing down ...')

# ---> user-specific end-of-run code could go here
    print('Data Acquisition ended normally')
# <---

  except KeyboardInterrupt:
    print(sys.argv[0]+': keyboard interrupt - closing down ...')
    BM.end() # shut down BufferManager

  finally:
# END: code to clean up
    PSconf.closeDevice() # close down hardware device
    stop_processes(procs) # termnate background processes
    print('finished cleaning up \n')
