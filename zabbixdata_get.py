#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = "ToySun"
__date__ = "2017-12-20"
#version:1.0

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from zabbix_client import ZabbixServerProxy
import json,sys,time,copy
reload(sys)
sys.setdefaultencoding('utf-8')

wait_time = 60

class Zabbix():
  def __init__(self):
    self.zb = ZabbixServerProxy("http://192.168.10.143/zabbix")
    self.zb.user.login(user="Admin",password="zabbix")#auth

  def get_hosts(self):
    host_data = {
    "output": ["hostid", "name"]
    }
    host_ret = self.zb.host.get(**host_data)
    return host_ret

  def item_get(self):
    for host in  self.get_hosts():
      key_dict = {}
      item_data = {
      "output":["itemids","key_"],
      "hostids": host['hostid'],
      }
      item_ret = self.zb.item.get(**item_data)
      for item in item_ret:
        if self.history_get(item['itemid']) is None:
          pass
        else:
          key_dict[item['key_']] = int(self.history_get(item['itemid']))
        key_dict['hostid'] = int(host['hostid'])
        key_dict['name'] = host['name']
        key_dict['@timestamp'] = time.strftime('%Y-%m-%dT%H:%M:%S+08:00')
      self.post_data(json.dumps(key_dict))
    time.sleep(wait_time)
    return ''

  def history_get(self, itemid):
        #history:Possible values: 0 - numeric float; 1 - character; 2 - log;
        #3 - numeric unsigned; 4 - text. Default: 3
        #limit:the number of value. Default/Must: 1
    data = { "output": "extend",
           "history": 3,
           "itemids": itemid,
           "sortfield": "clock",
           "sortorder": "DESC",
           "limit": 1
           }
    history_ret = self.zb.history.get(**data)
    #由于history值为 3
    if history_ret:
      return history_ret[0]['value']
    else:
      pass

  def post_data(self, info):
    es = Elasticsearch("192.168.10.125")
    esindex_prefix = "cc-monitor"
    esindex = "%s-%s" % (esindex_prefix, time.strftime('%Y.%m.%d'))
    values = []
    info_deepcopy = copy.deepcopy(info)
    values.append({
                  "_index": esindex,
                  "_type": 'test',
                  "_source": info_deepcopy
                  })
    helpers.bulk(es, values)
    print info

if __name__ == "__main__": 
    zabbix_start = Zabbix()
    print "zabbix_post start!"
    while 1:
        zabbix_start.item_get()
    #zabbix_start.item_get()
