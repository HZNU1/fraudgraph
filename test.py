# -*- coding: utf-8 -*-
import pandas as pd

from libfile import dbconfig
from libfile import equery
from libfile.equery import ExeGraph

def main():

    user_id = 'ce6dff30e5524c78a5325cdbfdf7ffef'
    phone = '18679217572'

    sql = """
        SELECT user_id,receive_phone,count(*) 'num'
        FROM third_mobile_tel_data
        WHERE user_id = '{}' AND call_type = 1 AND receive_phone IS NOT NULL
        GROUP BY receive_phone
        HAVING count(*) >= 2
    """.format(user_id)

    userInfo = equery.execute_select(dbconfig.RISK_CONFIG, sql)
    content_list = [item.get('receive_phone') for item in userInfo]
    content_list = [phone if not phone.startswith('0') else phone[1:] for phone in content_list]

    record = find_label(content_list, phone)
    print(record)


#查找label
def find_label(content_list, phone):

    cypher = """
    	match (p:Phone)
    	where p.phone in ['{}']
    	return p
    """.format("','".join(content_list))

    eg = ExeGraph(dbconfig.GRAPH_CONFIG)
    eg.connect()
    records = eg.search(cypher)

    record_dict = {}
    if records:
        records = [record['p']['label'] for record in records]

        record_dict['bad'] = records.count('bad')
        record_dict['black'] = records.count('black')
        record_dict['blackgroup'] = records.count('blackgroup')
        record_dict['interblack1'] = records.count('interblack1')
        record_dict['interblack2'] = records.count('interblack2')

        record_dict['web'] = records.count('web')
        record_dict['bank'] = records.count('bank')

    cypher = """
        match (p:Phone)
        where p.phone = '{}'
        return p
    """.format(phone)
    records = eg.search(cypher)
    if records:
        record_dict['label'] = records[0]['p']['label']
    else:
        record_dict['label'] = ''

    return record_dict
    

if __name__ == '__main__':
    main()