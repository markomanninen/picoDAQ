# -*- coding: utf-8 -*-
# code fragment  testDAQ.py to run inside runDAQ.py
'''
  code fragment to embed user code (in exampleComsumers.py) into
   script runDAQ.py
'''

from exampleConsumers import *
# ->>> code from here inserted as 'testDAQ.py' in runDAQ.py

# get Client Id from BufferManager (must be done in mother process)
cId_o = BM.BMregister()
procs.append(mp.Process(name = 'randConsumer', target = randConsumer, args = (BM, cId_o,) ) )
# client Id for random consumer
cId_r = BM.BMregister()
procs.append(mp.Process(name = 'obligConsumer', target = obligConsumer, args = (BM, cId_r,) ) )

# <<< - end of inserted code
