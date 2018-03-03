
##自營商買賣超彙總表 (股)

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

#----create table----自營商買賣超彙總表 (股)-before20141201
url = 'http://www.twse.com.tw/ch/trading/fund/TWT43U/TWT43U.php'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'qdate': '99/03/16', 'sorting': 'by_stkno'}
source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
source_code.encoding = 'utf8'
plain_text = source_code.text
soup = BeautifulSoup(plain_text, 'html.parser')
date = soup.find_all('thead')[0].find_all('tr')[0].find_all('td')[0].text
ymd = re.findall(r"\d\d\d?", date)
h = ['年月日']
for th in soup.find_all('thead')[0].find_all('tr')[1].find_all('td'):
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
sql = "create table `" + '自營商買賣超彙總表 (股)-before20141201' + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ',PRIMARY KEY (`年月日`,`證券代號`))'
c.execute(sql)

def f1(tablename,y,m,d):
    for year in y:
        for month in m:
            for day in d:
                try:
                    url = 'http://www.twse.com.tw/ch/trading/fund/TWT43U/TWT43U.php'
                    input_date=year+'/'+month+'/'+day
                    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
                    payload = {'download':'','qdate': input_date, 'sorting': 'by_stkno'}
                    source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
                    source_code.encoding = 'utf8'
                    plain_text = source_code.text
                    soup = BeautifulSoup(plain_text, 'html.parser')
                    date=soup.find_all('thead')[0].find_all('tr')[0].find_all('td')[0].text
                    print(input_date)
                    ymd = re.findall(r"\d\d\d?", date)
                    h = ['年月日']
                    for th in soup.find_all('thead')[0].find_all('tr')[1].find_all('td'):
                        h.append(th.text)
                    l = [h]
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
                    if list(df)==['年月日', '證券代號', '證券名稱', '買進股數', '賣出股數', '買賣超股數']:
                        c.executemany('INSERT INTO `' + tablename + '` VALUES (?,?,?,?,?,?)', df.values.tolist())
                        conn.commit()
                        print(date)
                    else:
                        print(input_date+' not match')
                except Exception as e:
                    print(e)
                    pass
    print('1done')

tablename='自營商買賣超彙總表 (股)-before20141201'
y=['93']
m=['12']
d=['17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']
f1(tablename,y,m,d)
y=['94', '95', '96', '97', '98', '99','100', '101', '102', '103']
m=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
d=['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']
f1(tablename,y,m,d)

#----create table----
url = 'http://www.twse.com.tw/ch/trading/fund/TWT43U/TWT43U.php'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'qdate': '105/03/15', 'sorting': 'by_stkno'}
source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
source_code.encoding = 'utf8'
plain_text = source_code.text
soup = BeautifulSoup(plain_text, 'html.parser')
date = soup.find_all('thead')[0].find_all('tr')[0].find_all('td')[0].text
ymd = re.findall(r"\d\d\d?", date)
h = ['年月日']
for th in soup.find_all('thead')[0].find_all('tr')[1].find_all('td'):
    h.append(th.text)
h1=[]
for th in soup.find_all('thead')[0].find_all('tr')[2].find_all('td'):
    h1.append(th.text)
l = [[h[0],h[1],h[2],h[3]+h1[1],h[3]+h1[2],h[3]+h1[3],h[4]+h1[1],h[4]+h1[2],h[4]+h1[3],h[5]+h1[1],h[5]+h1[2],h[5]+h1[3]]]
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
sql = "create table `" + '自營商買賣超彙總表 (股)' + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ',PRIMARY KEY (`年月日`,`證券代號`))'
c.execute(sql)

#----update using datetime----
import datetime

tablename='自營商買賣超彙總表 (股)'
startdate = datetime.datetime(2016, 4, 7)
delta = datetime.datetime.now() - startdate
for t in range(delta.days):
    date = startdate + datetime.timedelta(days=t + 1)
    try:
        url = 'http://www.twse.com.tw/ch/trading/fund/TWT43U/TWT43U.php'
        month, day = date.month, date.day
        if len(str(month)) == 1:
            month='0'+str(month)
        if len(str(day)) == 1:
            day='0'+str(day)
        input_date = str(date.year - 1911) + '/'  + str(month) + '/' + str(day)
        print(date.year, date.month, date.day, input_date)
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
        payload = {'download':'','qdate': input_date, 'sorting': 'by_stkno'}
        source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
        source_code.encoding = 'utf8'
        plain_text = source_code.text
        soup = BeautifulSoup(plain_text, 'html.parser')
        title=soup.find_all('thead')[0].find_all('tr')[0].find_all('td')[0].text
        ymd = re.findall(r"\d\d\d?", title)
        h = ['年月日']
        for th in soup.find_all('thead')[0].find_all('tr')[1].find_all('td'):
            h.append(th.text)
        h1 = []
        for th in soup.find_all('thead')[0].find_all('tr')[2].find_all('td'):
            h1.append(th.text)
        l = [[h[0], h[1], h[2], h[3] + h1[1], h[3] + h1[2], h[3] + h1[3], h[4] + h1[1], h[4] + h1[2],
              h[4] + h1[3], h[5] + h1[1], h[5] + h1[2], h[5] + h1[3]]]
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
        if list(df)==['年月日', '證券代號', '證券名稱', '自營商(自行買賣)賣出股數', '自營商(自行買賣)買賣超股數', '自營商(自行買賣)買進股數', '自營商(避險)賣出股數', '自營商(避險)買賣超股數', '自營商(避險)買進股數', '自營商賣出股數', '自營商買賣超股數', '自營商買進股數']:
            c.executemany('INSERT INTO `' + tablename + '` VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', df.values.tolist())
            conn.commit()
            print(title)
        else:
            print(input_date+' not match')
    except Exception as e:
        print(e)
        pass

#--- split and update ---
df1 = read_sql_query('SELECT * from `自營商買賣超彙總表 (股)`', conn)
df = read_sql_query('SELECT * from `自營商買賣超彙總表 (股)-before20141201`', conn)
list(df)
list(df1)
df = df.rename(columns={'買進股數':'自營商買進股數','賣出股數':'自營商賣出股數','買賣超股數':'自營商買賣超股數'})
df2=concat([df1,df],ignore_index=True)
df2=df2.sort_values(['年月日', '證券代號'])
df2=df2[list(df1)]
list(df2)
sql='ALTER TABLE `自營商買賣超彙總表 (股)` RENAME TO `自營商買賣超彙總表 (股)0`'
c.execute(sql)
list(df1)==list(df2)
tablename='自營商買賣超彙總表 (股)'
names=list(df2)
sql = "create table `" + tablename + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ',PRIMARY KEY (`年月日`,`證券代號`))'
c.execute(sql)
len(names)
c.executemany('INSERT INTO `' + tablename + '` VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', df2.values.tolist())
conn.commit()
print('finish')

sql='ALTER TABLE `自營商買賣超彙總表 (股)0` RENAME TO `自營商買賣超彙總表 (股)-after20141201`'
c.execute(sql)





