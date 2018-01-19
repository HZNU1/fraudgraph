# -*- coding: utf-8 -*-

from neo4j.v1 import GraphDatabase 
import pymysql.cursors

import requests
from lxml import etree

class ExeGraph:
  """ 对graph的操作类 """

  def __init__(self, config):
    """" 初始化参数 """

    self.url = config.get('url')
    self.user = config.get('user')
    self.password = config.get('password')
    self.driver = None

  def connect(self):
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

def execute_select(config, sql):
    """ 查询数据 mysql """

    connection = pymysql.connect(**config)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
    except Exception as e:  
       print(e)
    finally:
        connection.close()

    return result

def search_label_inbaidu(phone):
    """ 查询号码标签 """

    url = 'http://www.baidu.com/s?ie=utf-8&f=8&rsv_bp=0&rsv_idx=1&tn=baidu&wd={}'
    label = 'no sign'

    if not (phone.startswith('4') or phone.startswith('0')):
        if '10086' in phone:
            label = '中国移动'
            return label
            
        elif len(phone) == 11:
                phone = phone
        else:
            phone = '0' + phone
    else:
        phone = phone

    search_url = url.format(phone)
    respose = requests.get(search_url)
    content = respose.text
    tree = etree.HTML(content)
    nodes = tree.xpath(u"//div[@class='op_fraudphone_word']/strong")

    if nodes:
        label = nodes[0].text

    if u'逾期' in content or u'催收' in content or u'还款' in content or u'贷款' in content or u'借贷' in content or u'信用' in content:
        label = '消金'

    return label

