# -*- coding: utf-8 -*-

import pandas as pd
import os

#小文件合并
def conbinedata():

    filelist = os.listdir('data/bad_contact/')
    file = []
    for filename in filelist:
        print(filename)
        filepath = 'data/bad_contact/' + filename
        ftemp = pd.read_csv(filepath)
        if not ftemp.empty:
            file.append(ftemp)
    file = pd.concat(file)
    file.to_csv('data/badcall.csv',index=False)

    filelist = os.listdir('data/black_contact/')
    file = []
    for filename in filelist:
        print(filename)
        filepath = 'data/black_contact/' + filename
        ftemp = pd.read_csv(filepath)
        file.append(ftemp)
    file = pd.concat(file)
    file.to_csv('data/blackcall.csv',index=False)

#文件对于手机和身份证进行基本处理
def dealdata():
    
    file = pd.read_csv('data/badcall.csv')
    info = pd.read_csv('data/badinfo.csv')

    file = pd.merge(file,info[['id','phone']], left_on='user_id', right_on='id')
    del file['user_id']
    del file['id']
    file.to_csv('data/badcallinfo.csv', index=False)

    file = pd.read_csv('data/blackcall.csv')
    info = pd.read_csv('data/blackinfo.csv')

    file = pd.merge(file,info[['id','phone']], left_on='user_id', right_on='id')
    del file['user_id']
    del file['id']
    file.to_csv('data/blackcallinfo.csv', index=False)

def gene_node():

    badinfo = pd.read_csv('data/badinfo.csv')
    badinfo[u'label'] = 'bad'
    badinfo[u'type'] = 0
    badcall = pd.read_csv('data/badcallinfo.csv')
    badcall[u'label'] = ''
    badcall[u'type'] = 2

    blackinfo = pd.read_csv('data/blackinfo.csv')
    blackinfo[u'label'] = 'black'
    blackinfo[u'type'] = 1
    blackcall = pd.read_csv('data/blackcallinfo.csv')
    blackcall[u'label'] = ''
    blackcall[u'type'] = 2

    #user node
    user_node = []
    user_node.append(badinfo[[u'id', u'NAME', u'age', u'date_created']])
    user_node.append(blackinfo[[u'id', u'NAME', u'age', u'date_created']])
    user_node = pd.concat(user_node)
    user_node = user_node.reset_index(drop=True)
    user_node = user_node.drop_duplicates([u'id'], keep='first')

    #user_node[u'NAME'] = ['"' + str(name) + '"' for name in user_node[u'NAME']]
    #user_node[u'date_created'] = ['"' + str(name) + '"' for name in user_node[u'date_created']]
    user_node[u'label'] = 'User'
    user_node[u'age'] = [int(age) for age in user_node[u'age']]
    user_node.columns = [u'userId:ID', u'name', u'age:int', u'date_created', u':LABEL']
    user_node.to_csv('import/user_node.csv', index=False, sep=',', encoding='utf-8')

    #phone node
    phone_node1 = []
    phone_node1.append(badinfo[[u'phone', u'type', u'label']])
    phone_node1.append(blackinfo[[u'phone', u'type', u'label']])
    phone_node1 = pd.concat(phone_node1)
    phone_node1 = phone_node1.reset_index(drop=True)

    phone_node2 = []
    phone_node2.append(badcall[[u'receive_phone', u'type', u'label']])
    phone_node2.append(blackcall[[u'receive_phone', u'type', u'label']])
    phone_node2 = pd.concat(phone_node2)
    phone_node2 = phone_node2.reset_index(drop=True)
    phone_node2['phone'] = phone_node2[u'receive_phone']
    del phone_node2[u'receive_phone']

    phone_node = pd.concat([phone_node1, phone_node2])
    phone_node = phone_node.reset_index(drop=True)
    phone_node = phone_node.drop_duplicates([u'phone'], keep='first')

    phone_node[u'phoneId:ID'] = phone_node[u'phone']
    #phone_node[u'phone'] = ['"' + str(phone) + '"' for phone in phone_node[u'phone']]
    phone_node[u':LABEL'] = 'Phone'
    phone_node = phone_node[[u'phoneId:ID', u'type', u'label', u'phone', u':LABEL']]
    phone_node.to_csv('import/phone_node.csv', index=False, sep=',', encoding='utf-8')

    #idnum node
    idnum_node = []
    idnum_node.append(badinfo[[u'id_num']])
    idnum_node.append(blackinfo[[u'id_num']])
    idnum_node = pd.concat(idnum_node)
    idnum_node = idnum_node.reset_index(drop=True)
    idnum_node = idnum_node.drop_duplicates([u'id_num'], keep='first')
    idnum_node[u'idnumId:ID'] = idnum_node[u'id_num']
    idnum_node[u'idnum'] = [str(id_num) for id_num in idnum_node[u'id_num']]
    del idnum_node[u'id_num']
    idnum_node[u':LABEL'] = 'Idnum'
    idnum_node = idnum_node[[u'idnumId:ID', u'idnum', u':LABEL']]
    idnum_node.to_csv('import/idnum_node.csv', index=False, sep=',', encoding='utf-8')


def gene_relationship():
    
    badinfo = pd.read_csv('data/badinfo.csv')
    badcall = pd.read_csv('data/badcallinfo.csv')

    blackinfo = pd.read_csv('data/blackinfo.csv')
    blackcall = pd.read_csv('data/blackcallinfo.csv')

    #use to phone
    #header :START_ID;:END_ID;:TYPE
    #content 666;139;OWNS
    up = [badinfo[[u'id', u'phone']], blackinfo[[u'id', u'phone']]]
    up = pd.concat(up)
    up = up.reset_index(drop=True)
    up = up.drop_duplicates([u'id',u'phone'], keep='first')
    up[u':TYPE'] = 'OWNS'
    up.columns = [u':START_ID', u':END_ID', u':TYPE']
    up.to_csv('import/userphone_relationships.csv', index=False, sep=',', encoding='utf-8')

    #user to id_num
    #header :START_ID;:END_ID;:TYPE
    #content 666;6666;CONTACTS
    ui = [badinfo[[u'id', u'id_num']], blackinfo[[u'id', u'id_num']]]
    ui = pd.concat(ui)
    ui = ui.reset_index(drop=True)
    ui = ui.drop_duplicates([u'id',u'id_num'], keep='first')
    ui[u':TYPE'] = 'OWNS'
    ui.columns = [u':START_ID', u':END_ID', u':TYPE']
    ui.to_csv('import/useridnum_relationships.csv', index=False, sep=',', encoding='utf-8')

    #relationships
    #header :START_ID;num:int;:END_ID;:TYPE
    #content 666;10;6666;CONTACTS
    pr = [badcall[[u'phone', u'num', u'receive_phone']], blackcall[[u'phone', u'num', u'receive_phone']]]
    pr = pd.concat(pr)
    pr = pr.reset_index(drop=True)
    pr = pr.drop_duplicates([u'phone',u'receive_phone'], keep='first')
    pr[u':TYPE'] = 'CONTACTS'
    pr.columns = [u':START_ID', u'num:int', u':END_ID', u':TYPE']
    pr.to_csv('import/usercallphone_relationships.csv', index=False, sep=',', encoding='utf-8')

if __name__ == '__main__':
    #conbinedata()
    #dealdata()
    #gene_node()
    gene_relationship()