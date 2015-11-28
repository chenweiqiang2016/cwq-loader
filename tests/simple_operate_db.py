# -*- coding: utf-8 -*-

#测试为什么不能往master数据库写入中文


##############################################################################
#与load.py进行对比测试
#修改MySQLdb的cursors.py的execute方法
#该方法最终执行r = self._query(query) 于是直接在execute方法内给query赋值
#query="""INSERT INTO
#                    categories (name, level, parent_id, merchant_id)
#                values
#                   ('运动跟踪器', 3, 123456867, '3C070')"""
#分别执行load.py与simple_operate_db.py发现结果不一
#通过load.py调用execute()方法插入的数据不能正常显示
#通过simple_operate_db.py调用execute()方法插入的数据可以正常显示
#比较发现load.py中连接数据库时多了charset='utf8'删除这个设置,可以正常插入数据库
###############################################################################

import MySQLdb

conn = MySQLdb.connect(host="10.5.17.188",
                       user="litb_merchadmin",
                       passwd="0643CABB-971E-47F1-9B09-6FAE2B1B5D2E",
                       db="aims")


cursor = conn.cursor()

sql = """INSERT INTO
                    categories (name, level, parent_id, merchant_id)
                values
                    ('运动跟踪器', 3, 123456867, '3C070')"""

cursor.execute(sql)

conn.commit()

conn.close()




