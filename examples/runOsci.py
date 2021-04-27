#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# script runOsci.py
'''
This script reads data from PicoScope and displays them in oscilloscope mode

Usage: ./runOsci.py [<Oscilloscpope_config>.yaml]
'''

from __future__ import print_function, division, unicode_literals, absolute_import

import sys, time, yaml, numpy as np, threading, multiprocessing as mp

# import relevant pieces from picodaqa
import picodaqa.picoConfig
from picodaqa.mpOsci import mpOsci
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

  PSconfDict = read_yaml_configuration_with_argv('PSOsci.yaml')

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
  deltaT = 10.  # max. update interval in ms
  cmdQ =  mp.Queue(1) # Queue for command input
  datQ =  mp.Queue(1) # Queue for data transfer to sub-process
  XY = True  # display Channel A vs. B if True
  name = 'Oscilloscope'
  procs.append(mp.Process(name = name, target = mpOsci,
               args = (datQ, PSconf.OscConfDict, deltaT,  name) ) )
#                      queue configuration       interval name

  thrds.append(threading.Thread(name = 'kbdInput', target = kbdInput,
               args = (cmdQ, 'type -> P(ause), R(esume), or (E)nd + <ret> ') ) )
#                      queue info_text

  # start subprocess(es)
  for prc in procs:
    prc.daemon = True
    prc.start()
    print(' -> starting process ', prc.name, ' PID=', prc.pid)

  ACTIVE = True # thread(s) active
  # start threads
  for thrd in thrds:
    print(' -> starting thread ', thrd.name)
    thrd.daemon = True
    thrd.start()

  DAQ_ACTIVE = True  # Data Acquisition active
  # LOOP
  sig = np.zeros(NChannels)
  try:
    cnt = 0
    T0 = time.time()
    while True:
      if DAQ_ACTIVE:
        cnt +=1
        PSconf.acquireData(buf) # read data from PicoScope
        # construct an "event" like BufferMan.py does and send via Queue
        datQ.put( (cnt, time.time() - T0, buf) )

      # check for keyboard input
      if not cmdQ.empty():
        cmd = cmdQ.get()
        if cmd == 'E': # E(nd)
          print('\n' + sys.argv[0] + ': End command recieved - closing down')
          ACTIVE = False
          break
        elif cmd == 'P': # P(ause)
          DAQ_ACTIVE = False
        elif cmd == 'R': # R(esume)
          DAQ_ACTIVE = True

  except KeyboardInterrupt:
    DAQ_ACTIVE = False
    ACTIVE = False
    print('\n' + sys.argv[0] + ': keyboard interrupt - closing down ...')

  finally:
    PSconf.closeDevice() # close down hardware device
    time.sleep(1.)
    stop_processes(procs) # stop all sub-processes in list
    print('*==* ' + sys.argv[0] + ': normal end')
