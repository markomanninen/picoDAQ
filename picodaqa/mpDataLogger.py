# -*- coding: utf-8 -*-

'''Signal history in TKinter window'''

from __future__ import print_function, division, unicode_literals
from __future__ import absolute_import

import sys, time, numpy as np

import matplotlib
matplotlib.use('TkAgg')
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
if sys.version_info[0] < 3:
  import Tkinter as Tk
  import tkMessageBox as mbox
  from tkFileDialog import asksaveasfilename
else:
  import tkinter as Tk
  from tkinter import messagebox as mbox
  from tkinter.filedialog import asksaveasfilename

import matplotlib.pyplot as plt, matplotlib.animation as anim

# import Voltmeter class
from .DataLogger import *

def mpDataLogger(Q, conf, WaitTime = 100., name = '(Veff)', cmdQ = None):
  '''effective Voltage of data passed via multiprocessing.Queue
    Args:
      Q:         multiprocessing.Queue()
      conf:      picoConfig object
      WaitTime:  time between updates in ms
      name:      axis label
      cmdQ:      multiprocessing.Queue() for commands
  '''

  # generator to provide data to animation
  def yieldEvt_fromQ():
  # receives data via Queue from package mutiprocessing
    interval = WaitTime/1000.  # in s
    cnt = 0
    lagging=False
    while True:
      T0 = time.time()
      if not Q.empty():
        evData = Q.get()
        # print ('Q filled ')
        if type(evData) != np.ndarray:
          break
        cnt+=1
        #print('*==* yieldEvt_fromQ: received event %i' % cnt)
        yield (cnt, evData)
      else:
        yield None # send empty event if no new data

# guarantee correct timing
      dtcor = interval - time.time() + T0
      if dtcor > 0. :
        time.sleep(dtcor)
        if lagging:
          LblStatus.config(text=' OK ', fg = 'green')
          lagging=False
      else:
        lagging=True
        LblStatus.config(text='! lagging !', fg='red')

    # print('*==* yieldEvt_fromQ: received END event')
    sys.exit()

  def cmdResume():
    cmdQ.put('R')
    buttonP.config(text='Pause', fg='blue', state=Tk.NORMAL)
    buttonR.config(state=Tk.DISABLED)

  def cmdPause():
    cmdQ.put('P')
    buttonP.config(text='paused', fg='grey', state=Tk.DISABLED)
    buttonR.config(state=Tk.NORMAL)

  def cmdEnd():
    cmdQ.put('E')

  def cmdSave():
    try:
      filename = asksaveasfilename(initialdir='.',
               initialfile='DataLogger.png',
               title='select file name')
      figDL.savefig(filename)
    except:
      pass

# ------- executable part --------
#  print(' -> mpDataLogger starting')

  DL = DataLogger(WaitTime, conf, name)
  figDL = DL.fig

# generate a simple window for graphics display as a tk.DrawingArea
  root = Tk.Tk()
  root.wm_title("Data Logger")

# handle destruction of top-level window
  def _delete_window():
    if mbox.askokcancel("Quit", "Really destroy  main window ?"):
       print("Deleting main window")
       root.destroy()
  root.protocol("WM_DELETE_WINDOW", _delete_window)

# command buttons
  frame = Tk.Frame(master=root)
  frame.grid(row=0, column=8)
  frame.pack(padx=5, side=Tk.BOTTOM)

  buttonE = Tk.Button(frame, text='End', fg='red', command=cmdEnd)
  buttonE.grid(row=0, column=8)

  blank = Tk.Label(frame, width=7, text="")
  blank.grid(row=0, column=7)

  clock = Tk.Label(frame)
  clock.grid(row=0, column=5)

  buttonSv = Tk.Button(frame, text='save', width=8, fg='purple', command=cmdSave)
  buttonSv.grid(row=0, column=4)

  buttonP = Tk.Button(frame, text='Pause', width=8, fg='blue', command=cmdPause)
  buttonP.grid(row=0, column=3)

  buttonR = Tk.Button(frame, text='Resume', width=8, fg='blue', command=cmdResume)
  buttonR.grid(row=0, column=2)
  buttonR.config(state=Tk.DISABLED)

  LblStatus = Tk.Label(frame, width=13, text="")
  LblStatus.grid(row=0, column=0)

  canvas = FigureCanvasTkAgg(figDL, master=root)
  canvas.draw()
  canvas.get_tk_widget().pack(side=Tk.TOP, fill=Tk.BOTH, expand=1)
  canvas._tkcanvas.pack(side=Tk.TOP, fill=Tk.BOTH, expand=1)

# set up matplotlib animation
  tw = max(WaitTime - 20., 0.5) # smaller than WaitTime to allow for processing
  VMAnim = anim.FuncAnimation(figDL, DL, yieldEvt_fromQ,
                         interval = tw , init_func=DL.init,
                         blit=True, fargs=None, repeat=True, save_count=None)
                       # save_count=None is a (temporary) work-around
                       #     to fix memory leak in animate
  try:
    Tk.mainloop()

  except:
    print('*==* mpDataLogger: termination signal recieved')
  sys.exit()
# -- end def mpDataLogger()
