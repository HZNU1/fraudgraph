# -*- coding: utf-8 -*-
import pymysql.cursors

import requests
from lxml import etree

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
        if phone.endswith('10086'):
            label = '中国移动'
            return label
        elif phone.endswith('10010'):
            label = '中国联通'
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

