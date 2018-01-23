# -*- coding: utf-8 -*-

from neo4j.v1 import GraphDatabase
import queue
import json

from libfile import dbconfig
from libfile.graphQuery import ExeGraph

CYPHER = """ 
    match (ps1:Phone)-[r1:CONTACTS]->(pe1:Phone),(ps2:Phone)-[r2:CONTACTS]->(pe2:Phone)
    where ps1.phone = '{}' and pe2.phone = '{}' and ps1.phone = pe2.phone and pe1.phone = ps2.phone
    return ps1,r1,pe1
"""

def search_one():

	queues = queue.Queue()
	queues.put('18788539539')

	circle_list = []
	runed_list= []

	eg = ExeGraph(dbconfig.GRAPH_CONFIG)
	eg.connect_db()

	while not queues.empty():

		phone = queues.get()

		if phone in set(runed_list):
			continue

		cypher = CYPHER.format(phone,phone)
		records = eg.search(cypher)
		for record in records:
			queues.put(record[2].get('phone'))
			temp = {
				'st': record[0].get('phone'),
				'et': record[2].get('phone'),
				'num': record[1].get('num')
			}

			circle_list.append(temp)
		runed_list.append(phone)

		print('用户: {},互通人数: {}'.format(phone, len(records)))

	print(circle_list)

	json.dump(circle_list,open('data/circle.json','w'))


def all_circle():

	eg = ExeGraph(dbconfig.GRAPH_CONFIG)
	eg.connect_db()

	cypher = """ 
	    match (ps1:Phone)-[r1:CONTACTS]->(pe1:Phone),(ps2:Phone)-[r2:CONTACTS]->(pe2:Phone)
	    where ps1.phone = pe2.phone and pe1.phone = ps2.phone and ps1.type in ['0','1'] and pe1.type in ['0','1']
	    return ps1,r1,pe1
	"""
	circle_list = []
	records = eg.search(cypher)

	for record in records:
		temp = {
			'st': record[0].get('phone'),
			'et': record[2].get('phone'),
			'num': record[1].get('num')
		}
		circle_list.append(temp)

	json.dump(circle_list,open('data/circle_all.json','w'))

def gene_label():

	circledict = {}
	circle_list = json.load(open('data/circle_all.json'))

	for item in circle_list:
		print(item)

		ext = False
		st = item.get('st')
		et = item.get('et')
		for key, ilist in circledict.items():
			if st in set(ilist) or et in set(ilist):
				ilist.extend([st, et])
				circledict[key] = list(set(ilist))
				ext = True
				continue
		if not ext:
			circledict['group' + str(len(circledict))] = [st, et]

	json.dump(circledict,open('data/circledict.json','w'))

def execute_cql(filepath, label):

	circledict = json.load(open(filepath))
	black_label = []

	eg = ExeGraph(dbconfig.GRAPH_CONFIG)
	eg.connect_db()

	cql = """
		MATCH (p:Phone)
		WHERE p.phone in ['{}']
		SET p.level = {}
	"""
	for groupkey, members in circledict.items():
		print(groupkey)
		level = len(members)
		runcql = cql.format("','".join(members), level)
		eg.create(runcql)

		black_label.extend(members)

	cql = """
		MATCH (p:Phone)
		WHERE p.phone in ['{}']
		SET p.label = '{}'
	"""
	cql = cql.format("','".join(black_label), label)
	eg.create(cql)

if __name__ == '__main__':
	#all_circle()
	#gene_label()
	execute_cql('data/circledict.json', 'blackgroup')