# -*- coding: utf-8 -*-
'''read a json file (with comments marked with '#')'''

from __future__ import division
from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import json, sys, yaml

def read_config(jsonfile):
  # -- helper function to filter input lines
  def filter_lines(f, cc = '#'):
    """ remove charcters after comment character cc from file
      Args:
        * file f:  file
        * char cc:   comment character
      Yields:
        * string
    """
    jtxt = ''
    while True:
      line = f.readline()
      if (not line): return jtxt # EOF
      if cc in line:
        line = line.split(cc)[0] # ignore everything after comment character
        if (not line): continue # ignore comment lines
      if (not line.isspace()):  # ignore empty lines
        jtxt += line
#   -- end filter_lines
  jsontxt = filter_lines(jsonfile)
  return json.loads(jsontxt)

def read_yaml_configuration(configuration_file):
  '''
  read DAQ configuration file
  '''
  with open(configuration_file) as f:
    return yaml.load(f, Loader = yaml.FullLoader)

def read_yaml_configuration_with_argv(configuration_file = ''):
  '''
  check for / read command line arguments
  read DAQ configuration file
  '''
  if len(sys.argv) > 1:
    configuration_file = sys.argv[1]
  print('    DAQconfiguration from file ' + configuration_file)
  try:
    configuration_dictionary = read_yaml_configuration(configuration_file)
  except:
    print('     failed to read DAQ configuration file ' + configuration_file)
    exit(1)
  return configuration_dictionary
