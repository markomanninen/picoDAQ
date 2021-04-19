# -*- coding: utf-8 -*-

import sys, time

# helper function for example runners

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

def threaded_keyboard_input(cmdQ):
  '''
  Read keyboard input, run as backround-thread to aviod blocking
  '''
  # 1st, remove pyhton 2 vs. python 3 incompatibility for keyboard input
  if sys.version_info[:2] <= (2,7):
    get_input = raw_input
  else:
    get_input = input
  # set up callback function to use input getter and command queue
  # from runner scripts with the given info text
  def callback(info_text):
      keyboard_input_text = get_input(20 * ' ' + info_text)
      cmdQ.put(keyboard_input_text)
      keyboard_input_text = ''

  return callback
