  # ----import----
from sqlite3 import *
import os
os.chdir('C:\\Users\\ak66h_000\\Documents\\db\\')
conn = connect('tse.sqlite3')
c = conn.cursor()
from pandas import *

sql = "SELECT * FROM '%s'" % ('大盤統計資訊')
df = read_sql_query(sql, conn)
df=df.pivot(index='年月日', columns='指數', values='收盤指數').reset_index()
df.columns.name=None
df=df.drop_duplicates(['年月日'])

conn = connect('tse.sqlite3')
c = conn.cursor()
tablename='index'
sql='ALTER TABLE `%s` RENAME TO `%s0`'%(tablename, tablename)
c.execute(sql)
sql='create table `%s` (`%s`, PRIMARY KEY (%s))'%(tablename, '`,`'.join(list(df)), '`年月日`')
c.execute(sql)
sql='insert into `%s`(`%s`) values(%s)'%(tablename, '`,`'.join(list(df)), ','.join('?'*len(list(df))))
c.executemany(sql, df.values.tolist())
conn.commit()
sql="drop table `%s0`"%tablename
c.execute(sql)
print('finish')