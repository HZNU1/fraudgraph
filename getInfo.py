# -*- coding: utf-8 -*-

import pandas as pd
import os
from multiprocessing import Pool

from libfile import dbconfig
from libfile import equery

def get_userInfo(sql, filename):
    """ 获取用户基本信息 """

    userInfo = equery.execute_select(dbconfig.MAIN_CONFIG,sql)
    userdata = pd.DataFrame(userInfo)
    userdata.to_csv(filename, index=False, encoding = "utf-8")

    userdata = pd.read_csv(filename, encoding = "utf-8")
    userdata['phone'] = [phone[2:-1] for phone in userdata['phone']]
    userdata['id_num'] = [id_num[2:-1] for id_num in userdata['id_num']]
    userdata.to_csv(filename, index=False, encoding = "utf-8")

def get_contactInfo(userfile, savepath):
    """ 获取运营商数据 """

    userdata = pd.read_csv(userfile, encoding = "utf-8")
    downloadlist = set([filename[:-4] for filename in os.listdir(savepath)])
    userlist = [(user_id, savepath) for user_id in userdata['id'] if user_id not in downloadlist]

    #分段使用多线程获取数据 同时计数
    filelist = [userlist[i:i+500] for i in range(0,len(userlist),500)]
    dealnum = 0
    for filelistitem in filelist:

        print('已处理 {}/{}'.format(dealnum, len(userlist)))

        try:
            pool = Pool(processes = 5)
            pool.map(contact_item_info, filelistitem)
            pool.close()
            pool.join()
        except Exception as e:
            pass
        finally:
            dealnum += len(filelistitem)  

def contact_item_info(user):
    """ 获取每个人的运营商数据 """

    sql = """
        SELECT user_id,receive_phone,count(*) 'num'
        FROM third_mobile_tel_data
        WHERE user_id = '{}' AND call_type = 1 and receive_phone is not null
        GROUP BY receive_phone
        HAVING count(*) >= 2 
    """.format(user[0])

    filename = user[1] + user[0] + '.csv'
    userContact = equery.execute_select(dbconfig.RISK_CONFIG, sql)
    if userContact:
        contactdata = pd.DataFrame(userContact)
        contactdata.to_csv(filename, index=False)

    
if __name__ == '__main__':
    sql = """
        SELECT u.id,u.NAME,AES_DECRYPT(u.phone, '1zhida**') 'phone',
            AES_DECRYPT(u.id_num, '1zhida**') 'id_num',uu.age,uu.date_created
        FROM _user u,(
            SELECT DISTINCT userSid FROM loan_repaying l
            WHERE compatibleStatus NOT IN ('CANCEL')AND termDate < now()
                AND IF (repaidTime IS NULL,
                        DATEDIFF(DATE_FORMAT(now(), '%Y-%m-%d'),DATE_FORMAT(termDate, '%Y-%m-%d')),
                        DATEDIFF(DATE_FORMAT(now(), '%Y-%m-%d'),DATE_FORMAT(repaidTime, '%Y-%m-%d'))
                        ) >= 60
            ) ll, user uu
        WHERE u.id = ll.userSid AND uu.id = u.id and u.phone is not null and u.id_num is not null and uu.age is not null and uu.date_created is not null
    """
    get_userInfo(sql, 'data/badinfo.csv')
    #get_contactInfo('data/badinfo.csv', 'data/bad_contact/')

    sql = """
        SELECT u.id,u. NAME,AES_DECRYPT(u.phone, '1zhida**') 'phone',
            AES_DECRYPT(u.id_num, '1zhida**') 'id_num',uu.age,uu.date_created
        FROM _user u,(
            SELECT DISTINCT phone FROM phone_black_list l
            WHERE LEVEL = 1 AND remark IS NOT NULL
            ) ll,USER uu
        WHERE ll.phone = AES_DECRYPT(u.phone, '1zhida**')
            AND uu.id = u.id and u.phone is not null and u.id_num is not null and uu.age is not null and uu.date_created is not null
    """
    get_userInfo(sql, 'data/blackinfo.csv')
    #get_contactInfo('data/blackinfo.csv', 'data/black_contact/')