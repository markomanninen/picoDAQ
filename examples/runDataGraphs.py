#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# script runDataGraphs.py
'''
This script reads data samples from PicoScope and
displays data as effective voltage, history display and xy plot

Usage: ./runDataGraphs.py [<Oscilloscpope_config>.yaml Interval]
'''

from __future__ import print_function, division, unicode_literals, absolute_import

import sys, time, yaml, numpy as np, threading, multiprocessing as mp

# import relevant pieces from picodaqa
import picodaqa.picoConfig
from picodaqa.mpDataGraphs import mpDataGraphs
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

  PSconfDict = read_yaml_configuration_with_argv('PSVoltMeter.yaml')

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
  deltaT = interval * 1000. # update interval in ms
  cmdQ =  mp.Queue(1) # queue for command input
  DGmpQ =  mp.Queue(1) # queue for data transfer to sub-process
  XY = PSconf.XY # display Channel A vs. B if True
  procs.append(mp.Process(name = 'DataGraphs', target = mpDataGraphs,
               args = (DGmpQ, PSconf.OscConfDict, deltaT,  '(Volt)', XY,   cmdQ) ) )
#                      queue  configuration       interval name      graph queue

  thrds.append(threading.Thread(name = 'kbdInput', target = kbdInput,
               args = (cmdQ, 'type -> P(ause), R(esume), E(nd) or s(ave) + <ret> ') ) )
#                      queue info_text

# start subprocess(es)
  for prc in procs:
    prc.deamon = True
    prc.start()
    print(' -> starting process ', prc.name, ' PID=', prc.pid)

  ACTIVE = True # thread(s) active
  # start threads
  for thrd in thrds:
    print(' -> starting thread ', thrd.name)
    thrd.deamon = True
    thrd.start()

  DAQ_ACTIVE = True  # Data Acquisition active
  sig = np.zeros(NChannels)
  # LOOP
  try:
    cnt = 0
    T0 = time.time()
    while True:
      if DAQ_ACTIVE:
        cnt +=1
        PSconf.acquireData(buf) # read data from PicoScope
        # construct an "event" like BufferMan.py does and send via Queue
        for i, b in enumerate(buf): # process data
          # sig[i] = np.sqrt (np.inner(b, b) / NSamples)    # eff. Voltage
          sig[i] = b.sum() / NSamples # average
        DGmpQ.put(sig)

      # check for keyboard input
      if not cmdQ.empty():
        cmd = cmdQ.get()
        if cmd == 'E': # E(nd)
          DGmpQ.put(None) # send empty "end" event
          print('\n' + sys.argv[0] + ': End command recieved - closing down')
          ACTIVE = False
          break
        elif cmd == 'P': # P(ause)
          DAQ_ACTIVE = False
        elif cmd == 'R': # R(esume)
          DAQ_ACTIVE = True
        elif cmd == 's': # s(ave)
          DGmpQ.put(None) # send empty "end" event
          DAQ_ACTIVE = False
          ACTIVE = False
          print('\n storing data to file, ending')
          pass # to be implemented ...
          break

  except KeyboardInterrupt:
    DAQ_ACTIVE = False
    ACTIVE = False
    print('\n' + sys.argv[0] + ': keyboard interrupt - closing down ...')

  finally:
    PSconf.closeDevice() # close down hardware device
    stop_processes(procs) # stop all sub-processes in list
    print('*==* ' + sys.argv[0] + ': normal end')
