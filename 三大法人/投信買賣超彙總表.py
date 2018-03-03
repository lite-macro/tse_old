##投信買賣超彙總表

# sqlite3 can only run in console
from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\TEJ.sqlite3')
c = conn.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *
from functools import *
import re

def mymerge(x, y):
    m = merge(x, y, how='outer')
    return m

#----create table----

url = 'http://www.twse.com.tw/ch/trading/fund/TWT44U/TWT44U.php'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'qdate': '105/03/15', 'sorting': 'by_stkno'}
source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
source_code.encoding = 'utf8'
plain_text = source_code.text
soup = BeautifulSoup(plain_text, 'html.parser')
date = soup.find_all('thead')[0].find_all('tr')[0].find_all('td')[0].text
ymd = re.findall(r"\d\d\d?", date)
h = ['年月日', '鉅額交易']
for th in soup.find_all('thead')[0].find_all('tr')[1].find_all('td')[1:]:
    h.append(th.text)
l = [h]
for tr in soup.find_all('tbody')[0].find_all('tr'):
    r = [str(int(ymd[0]) + 1911) + '/' + ymd[1] + '/' + ymd[2]]
    for td in tr.find_all('td'):
        r.append(td.string)
    l.append(r)
df = DataFrame(l)
df.columns = df.ix[0, :]
df = df.ix[1:len(df), :]
df = df.replace(',', '', regex=True)
names = list(df)
# sql = "create table `" + date.split()[1] + "`(" + "'" + names[0] + "'"
sql = "create table `" + '投信買賣超彙總表 (股)' + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ',PRIMARY KEY (`年月日`,`證券代號`))'
c.execute(sql)

# --- update ---
import datetime

tablename='投信買賣超彙總表 (股)'
startdate = datetime.datetime(2016, 4, 7)
delta = datetime.datetime.now() - startdate
for t in range(delta.days):
    date = startdate + datetime.timedelta(days=t + 1)
    try:
        url = 'http://www.twse.com.tw/ch/trading/fund/TWT44U/TWT44U.php'
        if len(str(date.month)) == 1:
            input_date= str(date.year-1911)+'/'+'0'+str(date.month)+'/'+str(date.day)
        if len(str(date.month)) == 2:
            input_date= str(date.year-1911)+'/'+str(date.month)+'/'+str(date.day)
        print(date.year, date.month, date.day, input_date)
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
        payload = {'download':'','qdate': input_date, 'sorting': 'by_stkno'}
        source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
        source_code.encoding = 'utf8'
        plain_text = source_code.text
        soup = BeautifulSoup(plain_text, 'html.parser')
        title=soup.find_all('thead')[0].find_all('tr')[0].find_all('td')[0].text
        ymd = re.findall(r"\d\d\d?", title)
        h = ['年月日', '鉅額交易']
        for th in soup.find_all('thead')[0].find_all('tr')[1].find_all('td')[1:]:
            h.append(th.text)
        l=[h]
        for tr in soup.find_all('tbody')[0].find_all('tr'):
            r = [str(int(ymd[0]) + 1911) + '/' + ymd[1] + '/' + ymd[2]]
            for td in tr.find_all('td'):
                r.append(td.string)
            l.append(r)
        df = DataFrame(l)
        df.columns=df.ix[0,:]
        df=df.ix[1:len(df),:]
        df = df.replace(',', '', regex=True)
        df['證券代號'], df['證券名稱'] = df['證券代號'].str.strip(), df['證券名稱'].str.strip()
        if list(df)==['年月日', '鉅額交易', '證券代號', '證券名稱', '買進股數', '賣出股數', '買賣超股數']:
            c.executemany('INSERT INTO `' + tablename + '` VALUES (?,?,?,?,?,?,?)', df.values.tolist())
            conn.commit()
            print(title)
        else:
            print(input_date+' not match')
    except Exception as e:
        print(e)
        pass


#---read csv---
import re
import os
os.getcwd()
dir()
os.listdir()
path = 'C:/Users/ak66h_000/OneDrive/webscrap/tse/三大法人/三大法人/'
os.chdir(path)
l = os.listdir()
L = []
for i in l:
    df = read_csv(i, encoding='cp950')
    # df1 = DataFrame()
    # df = concat([df1, df], axis=1)
    L.append(df)
df1 = reduce(mymerge, L)
print(df1)
df1.to_csv('C:/Users/ak66h_000/OneDrive/webscrap/tse/三大法人/三大法人買賣超日報(股).csv',index=False)
df1=read_csv('C:/Users/ak66h_000/OneDrive/webscrap/tse/三大法人/三大法人買賣超日報(股).csv',encoding='cp950')
df1=df1[['年月日', '證券代號', '證券名稱', '外資買進股數', '外資賣出股數', '投信買進股數', '投信賣出股數', '自營商買進股數(自行買賣)', '自營商賣出股數(自行買賣)', '自營商買進股數(避險)', '自營商賣出股數(避險)', '自營商買進股數', '自營商賣出股數', '三大法人買賣超股數']]

#----create table----
tablename='三大法人買賣超日報(股)'
names=list(df1)
sql = "create table `" + tablename + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ',PRIMARY KEY (`年月日`,`證券代號`))'
c.execute(sql)

#---insert into sqlite---
df1=df1.drop_duplicates(subset=['年月日','證券代號'])
df1=df1.drop_duplicates()
c.executemany('INSERT INTO `' + tablename + '` VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)', df1.values.tolist())
conn.commit()

du=df1.duplicated(subset=['年月日','證券代號']).tolist()
[x for x in du if x == True]

#---insert into sqlite by name(very slow)---
tablename='三大法人買賣超日報(股)'
names=list(df1)
c = conn.cursor()
for i in range(0,len(df1)):
    sql = "insert into `" +tablename + "`(" + "'" + names[0] + "'"
    for n in names[1:len(names)]:
        sql = sql + ',' + "'" + n + "'"
    sql = sql + ')'

    sql1 = "VALUES (" + "'" + df1.values.tolist()[i][0] + "'"
    for n in df1.values.tolist()[i][1:len(df1.values.tolist()[i])]:
        sql1 = sql1 + ',' + "'" + str(n) + "'"
    sql1 = sql1 + ')'

    sql2=sql+sql1
    c.execute(sql2)
    conn.commit()
print('finish')



