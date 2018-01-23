# -*- coding: utf-8 -*-

from neo4j.v1 import GraphDatabase

import requests
import json

class ExeGraph:
  """ 对graph的操作类 """

  def __init__(self, config):
    """" 初始化参数 """

    self.url = config.get('url')
    self.user = config.get('user')
    self.password = config.get('password')
    self.driver = None
    self.uri = 'http://{}:{}@localhost:7474/db/data/cypher'.format(config.get('user'),config.get('password'))

  def connect_db(self):
    """ 数据库连接 """

    self.driver = GraphDatabase.driver(self.url, auth=(self.user, self.password))
    

  def create(self, cypher):
    """create 命令"""
    """ no return """

    try:
        with self.driver.session() as session:  
            with session.begin_transaction() as tx:
                tx.run(cypher)

    except Exception as e:  
        raise e

  def search(self, cypher):
    """search 命令"""
    """ return list """

    try:
      with self.driver.session() as session:
        with session.begin_transaction() as tx:
          result = tx.run(cypher)
    except Exception as e:  
        raise e
              
    return [record for record in result.records()]

  def search_api(self,query):
    """ 通过api的方式进行数据查询 """
    """ params: query"""
    """ query = {
        "query" : "match (p:Phone) where p.phone in { phones } return p.phone, p.label, p.level",
        "params" : {
            "phones" : phone_list
        }
    }
    """
    """ return """
    try:
        res = requests.post(self.uri, data = json.dumps(query))
        res = json.loads(res.text)
    except Exception as e:  
        raise e

    return res


