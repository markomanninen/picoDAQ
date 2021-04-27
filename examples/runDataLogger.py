#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# script runDataLogger.py
'''
This script reads samples from PicoScope and display averages as voltage history

Usage: ./runDataLogger.py [<Oscilloscpope_config>.yaml Interval]
'''

from __future__ import print_function, division, unicode_literals
from __future__ import absolute_import

import sys, time, yaml, numpy as np, threading, multiprocessing as mp

# import relevant pieces from picodaqa
import picodaqa.picoConfig
from picodaqa.mpDataLogger import mpDataLogger
from picodaqa.read_config import read_yaml_configuration,\
                                 read_yaml_configuration_with_argv
from functions import stop_processes, threaded_keyboard_input

def kbdInput(cmdQ, info_text):
  queued_input = threaded_keyboard_input(cmdQ)
  # active state comes from the main function
  while ACTIVE:
    queued_input(info_text)

if __name__ == "__main__": # - - - - - - - - - - - - - - - - - - - - - -

  print('\n*==* script ' + sys.argv[0] + ' running \n')

  PSconfDict = read_yaml_configuration_with_argv('PSdataLogger.yaml')

  interval = 0.2
  if len(sys.argv) == 3:
    interval = float(sys.argv[2])
    if interval < 0.05:
      print(" !!! read-out intervals < 0.05 s not reliable, setting to 0.05 s")
      interval = 0.05

  # configure and initialize PicoScope
  PSconf = picodaqa.picoConfig.PSconfig(PSconfDict)
  PSconf.init()

  # copy some of the important configuration variables
  NChannels = PSconf.NChannels # number of channels in use
  TSampling = PSconf.TSampling # sampling interval
  NSamples  = PSconf.NSamples  # number of samples
  buf = np.zeros( (NChannels, NSamples) ) # data buffer for PicoScope driver

  thrds = []
  procs = []
  deltaT = interval * 1000 # minimal time between figure updates in ms
  cmdQ = mp.Queue(1) # queue for command input
  DLmpQ = mp.Queue(1) # queue for data transfer to sub-process

  procs.append(mp.Process(name = 'DataLogger', target = mpDataLogger,
               args = (DLmpQ, PSconf.OscConfDict, deltaT,  '(Volt)', cmdQ) ) )
#                      queue  configuration       interval name      queue

  thrds.append(threading.Thread(name = 'kbdInput', target = kbdInput,
               args = (cmdQ, 'type -> P(ause), R(esume), E(nd) or s(ave) + <ret> ') ) )
#                      queue info_text

  # start subprocess(es)
  for prc in procs:
    prc.daemon = True
    prc.start()
    print(' -> starting process ', prc.name, ' PID=', prc.pid)
    sig = np.zeros(NChannels)

  ACTIVE = True # thread(s) active
  # start threads
  for thrd in thrds:
    print(' -> starting thread ', thrd.name)
    thrd.daemon = True
    thrd.start()

  DAQ_ACTIVE = True # Data Acquistion active
  cnt = 0
  # LOOP
  try:
    while True:
      if DAQ_ACTIVE:
        cnt += 1
        PSconf.acquireData(buf) # read data from PicoScope
        for i, b in enumerate(buf): # process data
          # sig[i] = np.sqrt (np.inner(b, b) / NSamples) # eff. Voltage
          sig[i] = b.sum() / NSamples # average
        DLmpQ.put(sig)
      # check for keboard input
      if not cmdQ.empty():
        cmd = cmdQ.get()
        if cmd == 'E': # E(nd)
          DLmpQ.put(None) # send empty "end" event
          print('\n' + sys.argv[0] + ': End command recieved - closing down')
          ACTIVE = False
          break
        elif cmd == 'P': # P(ause)
          DAQ_ACTIVE = False
        elif cmd == 'R': # R(esume)
          DAQ_ACTIVE = True
        elif cmd == 's': # s(ave)
          DAQ_ACTIVE = False
          ACTIVE = False
          print('\n storing data to file, ending')
          pass # to be implemented ...
          break

  except KeyboardInterrupt:
     print(sys.argv[0] + ': keyboard interrupt - closing down ...')
     DAQ_ACTIVE = False
     ACTIVE = False

  finally:
    PSconf.closeDevice() # close down hardware device
    stop_processes(procs) # stop all sub-processes in list
    print('*==* ' + sys.argv[0] + ': normal end \n')
