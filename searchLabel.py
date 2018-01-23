# -*- coding: utf-8 -*-

import requests
import pandas as pd
from lxml import etree
import time
import json

from libfile import dbconfig
from libfile import equery
from libfile.equery import ExeGraph


#计算词频
def count_phone_num():

	cql = """
		match(ps:Phone)-[r:CONTACTS]->(pe:Phone)
		return count(r) as num
	"""
	eg = ExeGraph(dbconfig.GRAPH_CONFIG)
	eg.connect_db()
	res = eg.search(cql)
	num = res[0]['num']
	snum = list(range(0,num,1000000))

	count_dict = {}
	for sn in snum:
		print(str(sn) + '---' + str(sn + 1000000))
		cql = """
			match(ps:Phone)-[r:CONTACTS]->(pe:Phone)
			with pe.phone as phone,ps.phone as target skip {} limit 1000000
			return phone,count(target) AS phonenum
		""".format(sn)

		reslist = eg.search(cql)
		for res in reslist:
			count_dict[res['phone']] = count_dict.get(res['phone'], 0) + res['phonenum']

	json.dump(count_dict,open('data/phoneCount.json','w'))

#查询标签
def search():

	count_dict = json.load(open('data/phoneCount.json'))
	sumnum = sum([x >= 15 for x in count_dict.values()])
	search_dict = json.load(open('data/phoneLabel.json'))

	for phone, num in count_dict.items():

		if num >= 15 and phone not in search_dict:
			label = equery.search_label_inbaidu(phone)
			search_dict[phone] = label

			json.dump(search_dict,open('data/phoneLabel.json','w'))

			print('已完成：{}/{}'.format(len(search_dict), sumnum))

# 规范化标签
def compare():

	search_dict = json.load(open('data/phoneLabel.json'))
	phone_dict = {}

	for phone, label in search_dict.items():
		if label is not None:
			print(label)
			if u'中国移动' in label or u'中国电信' in label or u'中国联通' in label:
				if not phone_dict.get('yys'):
					phone_dict['yys'] = [phone]
				else:
					phone_dict['yys'].append(phone)
			elif u'银行' in label or u'邮政' in label:
				if not phone_dict.get('bank'):
					phone_dict['bank'] = [phone]
				else:
					phone_dict['bank'].append(phone)
			elif u'金' in label or u'科技' in label or u'网络' in label or u'贷' in label or u'钱' in label or u'分期' in label:
				if not phone_dict.get('web'):
					phone_dict['web'] = [phone]
				else:
					phone_dict['web'].append(phone)
			elif label == u'no sign':
				if not phone_dict.get('nosign'):
					phone_dict['nosign'] = [phone]
				else:
					phone_dict['nosign'].append(phone)
			else:
				if not phone_dict.get('company'):
					phone_dict['company'] = [phone]
				else:
					phone_dict['company'].append(phone)

	json.dump(phone_dict,open('data/phoneLabels.json','w'))


#从数据库里获得label
def webLabel():

    sql = """
        SELECT number FROM risk_number_label
    """
    num = equery.execute_select(dbconfig.RISK_CONFIG, sql)
    numlist = [str(int(number['number']))  for number in num]
    update_label(numlist, 'web')


#标签更新
def update_label(phonelist, label):

	phonel = "','".join(phonelist)
	cypher = """
		MATCH(p:Phone)
		WHERE p.phone in ['{}']
		set p.label = '{}'
		return count(p.label) as num 
	""".format(phonel, label)

	eg = ExeGraph(dbconfig.GRAPH_CONFIG)
	eg.connect_db()
	res = eg.search(cypher)
	print('已更新{}个{}'.format(res[0]['num'], label))


if __name__ == '__main__':
	#count_phone_num()
	#search()
	#compare()
	webLabel()
	# search_dict = json.load(open('data/phoneLabels.json'))
	# for label, phonelist in search_dict.items():
	# 	update_label(phonelist, label)

