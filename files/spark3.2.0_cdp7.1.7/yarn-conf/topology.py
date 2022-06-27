#!/usr/bin/env python
#
# Copyright (c) 2021 Cloudera, Inc. All rights reserved.
#

'''
This script is provided by CMF for hadoop to determine network/rack topology.
It is automatically generated and could be replaced at any time. Any changes
made to it will be lost when this happens.
'''

import sys, os
from string import join
import ConfigParser


DATA_FILE_NAME = '{{CMF_CONF_DIR}}/topology.map'
DEFAULT_RACK = '/default'
SECTION_NAME = "network_topology"

if 'CMF_CONF_DIR' in DATA_FILE_NAME:
  # variable was not substituted. Use this file's dir
  DATA_FILE_NAME = os.path.dirname(os.path.abspath(__file__)) + "/topology.map"


class TopologyScript():

  def load_rack_map(self):
    try:
      #RACK_MAP contains both host name vs rack and ip vs rack mappings
      mappings = ConfigParser.ConfigParser()
      mappings.read(DATA_FILE_NAME)
      return dict(mappings.items(SECTION_NAME))
    except ConfigParser.NoSectionError:
      return {}

  def get_racks(self, rack_map, args):
    if len(args) == 1:
      return DEFAULT_RACK
    else:
      return join([self.lookup_by_hostname_or_ip(input_argument, rack_map) for input_argument in args[1:]],)

  def lookup_by_hostname_or_ip(self, hostname_or_ip, rack_map):
    #try looking up by hostname
    rack = rack_map.get(hostname_or_ip)
    if rack is not None:
      return rack
    #try looking up by ip
    rack = rack_map.get(self.extract_ip(hostname_or_ip))
    #try by localhost since hadoop could be passing in 127.0.0.1 which might not be mapped
    return rack if rack is not None else rack_map.get("localhost.localdomain", DEFAULT_RACK)

  #strips out port and slashes in case hadoop passes in something like 127.0.0.1/127.0.0.1:50010
  def extract_ip(self, container_string):
    return container_string.split("/")[0].split(":")[0]

  def execute(self, args):
    rack_map = self.load_rack_map()
    rack = self.get_racks(rack_map, args)
    print rack

if __name__ == "__main__":
  TopologyScript().execute(sys.argv)
