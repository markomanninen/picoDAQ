'''
.. module:: _version_info
   :platform: python 2.7, 3.4
   :synopsis: Version 0.5beta of picodaq, rel. October 2017

.. moduleauthor:: Guenter Quast <guenter.quast@online.de>
'''

major = 0
minor = 5
revision = 0

def _get_version_tuple():
  '''
  version as a tuple
  '''
  return (major, minor, revision)

def _get_version_string():
  '''
  version as a string
  '''
  return "%d.%d.%d" % _get_version_tuple()

