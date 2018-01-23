# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import requests
import json

from libfile import config
from libfile import dbconfig
from libfile import equery
from libfile.graphQuery import ExeGraph

def query_one():

    user_id = '7336dfc7597d46bfab2c68f841ef479d'
    phoneId = '15077694366'

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

    #record = find_label(content_dict, phoneId)
    record = find_api(content_dict, phoneId)
    print(record)

def query_maney():

    user = pd.read_csv('data/testuser.csv')

    for i in range(len(user)):
        userId = user.ix[i]['id']
        phoneId = user.ix[i]['phone']

        sql = """
            SELECT user_id,receive_phone,count(*) 'num'
            FROM third_mobile_tel_data
            WHERE user_id = '{}' AND call_type = 1 AND receive_phone IS NOT NULL
            GROUP BY receive_phone
            HAVING count(*) >= 2
        """.format(userId)

        userInfo = equery.execute_select(dbconfig.RISK_CONFIG, sql)
        content_list = [(item.get('receive_phone'), item.get('num')) for item in userInfo]

        content_dict = {}
        for phone, num in content_list:
            plabel = phone if not phone.startswith('0') else phone[1:]
            content_dict[plabel] = num

        #record = find_label(content_dict, phoneId)
        jsondict = find_api(content_dict, phoneId)
        print(jsondict)


def query_group():

    user_id = '6bad659bb6234b08b24787d11e5762a0'
    phoneId = '15777670597'

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

    jsondict = findgroup_api(content_dict, phoneId)
    json.dump(jsondict,open('html/temp.json','w'))

def findgroup_api(content_dict, phone):
    phone_list = list(content_dict.keys())

    query = {
        "query" : "match (ps:Phone)-[r:CONTACTS]->(pe:Phone) where ps.phone in { phones } return ps.phone, ps.label, r.num , pe.phone, pe.label",
        "params" : {
            "phones" : phone_list
        }
    }

    eg = ExeGraph(dbconfig.GRAPH_CONFIG)
    records = eg.search_api(query)
    datalist = records['data']

    node_dict = {}
    relationship_dict = []

    nodecf = config.NODECONFIG
    if datalist:
        for item in datalist:
            if item[2] > 5:
                if not node_dict.get(item[3], None):
                    node2 = {
                        "id": item[3],
                        "label": item[4] if item[4] else '',
                        "color": nodecf.get(item[4], nodecf.get('none')).get('color'),
                        "size": nodecf.get(item[4], nodecf.get('none')).get('size')
                    }
                    node_dict[item[3]] = node2
                relationship1 = {
                    "source": item[0],
                    "target": item[3],
                    "value": item[2]
                }
                relationship_dict.append(relationship1)

    query = {
        "query" : " match (p:Phone) where p.phone = { phones } return p.label",
        "params" : {
            "phones" : str(phone)
        }
    }

    res = eg.search_api(query)
    datalist = res['data']
    node = {
        "id": str(phone),
        "label": '',
        "color": nodecf.get('center').get('color'),
        "size": nodecf.get('center').get('size')
    }
    if datalist:
        node["label"] = datalist[0][0] if datalist[0][0] else ''
    node_dict[str(phone)] = node

    query = {
        "query" : "match (p:Phone) where p.phone in { phones } return p.phone, p.label, p.level",
        "params" : {
            "phones" : phone_list
        }
    }
    res = eg.search_api(query)
    datalist = res['data']
    if datalist:
        for item in datalist:
            if not node_dict.get(item[0], None):
                node = {
                    "id": item[0],
                    "label": item[1] if item[1] else '',
                    "color": nodecf.get(item[1], nodecf.get('none')).get('color'),
                    "size": nodecf.get(item[1], nodecf.get('none')).get('size')
                }
                node_dict[item[0]] = node

    for targetphone, num in content_dict.items():
        relationship = {
            "source": str(phone),
            "target": targetphone,
            "value": num
        }
        relationship_dict.append(relationship)

    jsondict = {}
    jsondict["nodes"] = list(node_dict.values())
    jsondict["links"] = relationship_dict

    return jsondict
            

#查找label
def find_label(content_dict, phone):

    phone_list = content_dict.keys()

    cypher = """
    	match (p:Phone)
    	where p.phone in ['{}']
    	return p.phone,p.label
    """.format("','".join(phone_list))

    eg = ExeGraph(dbconfig.GRAPH_CONFIG)
    eg.connect_db()
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

    eg = ExeGraph(dbconfig.GRAPH_CONFIG)
    phone_list = list(content_dict.keys())

    query = {
        "query" : "match (p:Phone) where p.phone in { phones } return p.phone, p.label, p.level",
        "params" : {
            "phones" : phone_list
        }
    }
    res = eg.search_api(query)
    datalist = res['data']

    label_dict = {}
    if datalist:
        for record in datalist:
            if label_dict.get(record[1], None):
                label_dict[record[1]]['nums'] = label_dict[record[1]]['nums'] + 1
                label_dict[record[1]]['times'] = label_dict[record[1]]['times'] + content_dict[record[0]]
                label_dict[record[1]]['levels'] = np.max(label_dict[record[1]]['levels'], record[2])
            else:
                label_dict[record[1]] = {}
                label_dict[record[1]]['nums'] = 1
                label_dict[record[1]]['times'] = content_dict[record[0]]
                label_dict[record[1]]['levels'] = record[2]


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
            "phones" : str(phone)
        }
    }

    res = eg.search_api(query)
    datalist = res['data']

    if datalist:
        record_dict['label'] = datalist[0][0]
    else:
        record_dict['label'] = None

    return record_dict
    

if __name__ == '__main__':
    query_group()