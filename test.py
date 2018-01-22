# -*- coding: utf-8 -*-
import pandas as pd
import requests
import json

from libfile import dbconfig
from libfile import equery
from libfile.equery import ExeGraph

def main():

    user_id = 'ce6dff30e5524c78a5325cdbfdf7ffef'
    phoneId = '13982232482'

    sql = """
        SELECT user_id,receive_phone,count(*) 'num'
        FROM third_mobile_tel_data
        WHERE user_id = '{}' AND call_type = 1 AND receive_phone IS NOT NULL
        GROUP BY receive_phone
        HAVING count(*) >= 2
    """.format(user_id)

    userInfo = equery.execute_select(dbconfig.RISK_CONFIG, sql)
    content_list = [(item.get('receive_phone'), item.get('num')) for item in userInfo]

    content_dict = {}
    for phone, num in content_list:
        plabel = phone if not phone.startswith('0') else phone[1:]
        content_dict[plabel] = num

    #record = find_label(content_dict, phone)
    record = find_api(content_dict, phoneId)
    print(record)


#查找label
def find_label(content_dict, phone):

    phone_list = content_dict.keys()

    cypher = """
    	match (p:Phone)
    	where p.phone in ['{}']
    	return p.phone,p.label
    """.format("','".join(phone_list))

    eg = ExeGraph(dbconfig.GRAPH_CONFIG)
    eg.connect()
    records = eg.search(cypher)

    label_dict = {}
    if records:
        for record in records:
            if label_dict.get(record['p.label'], None):
                label_dict[record['p.label']]['nums'] = label_dict[record['p.label']]['nums'] + content_dict[record['p.phone']]
                label_dict[record['p.label']]['times'] = label_dict[record['p.label']]['times'] + 1
            else:
                label_dict[record['p.label']] = {}
                label_dict[record['p.label']]['nums'] = content_dict[record['p.phone']]
                label_dict[record['p.label']]['times'] = 1


    record_dict = {}
    record_dict['bad'] = label_dict.get('bad', None)
    record_dict['black'] = label_dict.get('black', None)
    record_dict['blackgroup'] = label_dict.get('blackgroup', None)
    record_dict['interblack1'] = label_dict.get('interblack1', None)
    record_dict['interblack2'] = label_dict.get('interblack2', None)

    record_dict['web'] = label_dict.get('web', None)
    record_dict['bank'] = label_dict.get('bank', None)

    cypher = """
        match (p:Phone)
        where p.phone = '{}'
        return p.label
    """.format(phone)
    records = eg.search(cypher)
    if records:
        record_dict['label'] = records[0]['p.label']
    else:
        record_dict['label'] = ''

    return record_dict

#以api的方式
def find_api(content_dict, phone):

    uri = 'http://localhost:7474/db/data/cypher'
    phone_list = list(content_dict.keys())

    query = {
        "query" : "match (p:Phone) where p.phone in { phones } return p.phone, p.label",
        "params" : {
            "phones" : phone_list
        }
    }

    res = requests.post('http://neo4j:25041@localhost:7474/db/data/cypher', data = json.dumps(query))
    res = json.loads(res.text)
    datalist = res['data']

    label_dict = {}
    if datalist:
        for record in datalist:
            if label_dict.get(record[1], None):
                label_dict[record[1]]['nums'] = label_dict[record[1]]['nums'] + content_dict[record[0]]
                label_dict[record[1]]['times'] = label_dict[record[1]]['times'] + 1
            else:
                label_dict[record[1]] = {}
                label_dict[record[1]]['nums'] = content_dict[record[0]]
                label_dict[record[1]]['times'] = 1


    record_dict = {}
    record_dict['bad'] = label_dict.get('bad', None)
    record_dict['black'] = label_dict.get('black', None)
    record_dict['blackgroup'] = label_dict.get('blackgroup', None)
    record_dict['interblack1'] = label_dict.get('interblack1', None)
    record_dict['interblack2'] = label_dict.get('interblack2', None)

    record_dict['web'] = label_dict.get('web', None)
    record_dict['bank'] = label_dict.get('bank', None)

    query = {
        "query" : " match (p:Phone) where p.phone = { phones } return p.label",
        "params" : {
            "phones" : phone
        }
    }

    res = requests.post('http://neo4j:25041@localhost:7474/db/data/cypher', data = json.dumps(query))
    res = json.loads(res.text)
    datalist = res['data']

    if datalist:
        record_dict['label'] = datalist[0][0]
    else:
        record_dict['label'] = None

    return record_dict
    

if __name__ == '__main__':
    main()