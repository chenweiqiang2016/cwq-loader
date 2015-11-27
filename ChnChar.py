# -*- coding: utf-8 -*-

##########################################################################################
#测试结论：
#连接数据库时：配置charset='utf8'
#SHOW VARIABLES LIKE 'character%'; set character_set_database=utf-8
#更改数据表默认的默认编码alter table products character set ‘utf8’;
#此时字段会发生变化需要做alter table categories modify url varchar(256) character set utf8;更改多个字段
###########################################################################################
import MySQLdb

conn = MySQLdb.connect(host='localhost',
                       user='root',
                       passwd='',
                       db='aims',
                       charset='utf8')
cursor = conn.cursor()

id = '1234567810'
name = '数码'
merchant_id = '3C070'

sql = """insert into categories (id, name, merchant_id) values(%s, %s, %s)
      """
print sql %(id, name, merchant_id)
cursor.execute(sql, (id, name, merchant_id))

#insert into categories (id, name, merchant_id) values (123456789, '数码', '3C070')
conn.commit();

print 'OK'