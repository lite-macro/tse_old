##----- pe is '0.00' when pe < 0 -----

from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\TEJ.sqlite3')
c = conn.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *
import re

###----大盤統計資訊----

# #----create table----
tablename='大盤統計資訊'
table=0
thead=0
url = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'qdate': '105/02/22', 'selectType': 'ALLBUT0999'}
source_code = requests.post(url, headers=headers, data=payload)
source_code.encoding = 'utf-8'
plain_text = source_code.text
print(plain_text)
soup = BeautifulSoup(plain_text, 'html.parser')
date = soup.find_all('thead')[0].find_all('tr')[0].text
print(date)

h = ['年月日']
for th in soup.find_all('table')[table].find_all('thead')[thead].find_all('tr')[1].find_all('td'):
    h.append(th.string)
soup.find_all('tbody')[0].find_all('tr')[0].find_all('td')

l = [h]
for tr in soup.find_all('table')[table].find_all('tbody')[0].find_all('tr'):
    r = [date]
    for td in tr.find_all('td'):
        r.append(td.string)
    l.append(r)
df = DataFrame(l)
df.columns = df.ix[0, :]
df = df.ix[1:len(df), :]
df['指數'] = df['指數'].str.strip()
tablename0 =list(df)
names = list(df)
sql = "create table `" +tablename + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ',PRIMARY KEY (`年月日`,`指數`))'
c.execute(sql)
# names = list(df)
# sql = "create table " + dic[key] + "(" +  names[0]
# for n in names[1:len(names)]:
#     sql = sql + ' varchar,' + n
# sql = sql + ' varchar, PRIMARY KEY (年月日, 指數))'
# cur.execute(sql)
# conn.commit()
###----大盤成交統計----

# #----create table----
tablename='大盤成交統計'
table=0
thead=2
url = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'qdate': '105/02/22', 'selectType': 'ALLBUT0999'}
source_code = requests.post(url, headers=headers, data=payload)
source_code.encoding = 'utf-8'
plain_text = source_code.text
print(plain_text)
soup = BeautifulSoup(plain_text, 'html.parser')
date = soup.find_all('thead')[0].find_all('tr')[0].text
print(date)

h = ['年月日']
for th in soup.find_all('table')[table].find_all('thead')[thead].find_all('tr')[0].find_all('td'):
    h.append(th.string)

l = [h]
for tr in soup.find_all('table')[table].find_all('tbody')[2].find_all('tr'):
    r = [date]
    for td in tr.find_all('td'):
        r.append(td.string)
    l.append(r)
df = DataFrame(l)
df.columns = df.ix[0, :]
df = df.ix[1:len(df), :]
df['成交統計'] = df['成交統計'].str.strip()
tablename0 =list(df)
names = list(df)
sql = "create table `" +tablename + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ',PRIMARY KEY (`年月日`,`成交統計`))'
c.execute(sql)

###----大盤統計資訊 + 大盤成交統計 in one new----

#----update using datetime----
import datetime
tablename0=['年月日', '指數', '收盤指數', '漲跌(+/-)', '漲跌點數', '漲跌百分比(%)']
tablename1=['年月日', '報酬指數', '收盤指數', '漲跌(+/-)', '漲跌點數', '漲跌百分比(%)']
tablename2=['年月日', '成交統計', '成交金額(元)', '成交股數(股)', '成交筆數']
# thead is irregular in tse, in early years, 1 thead contains 1 tr, in later years, 1 thead sometimes contains 2 tr
table=0
startdate = datetime.datetime(2016, 4, 8)
delta = datetime.datetime.now() - startdate
for t in range(delta.days):
    date = startdate + datetime.timedelta(days=t + 1)
    try:
        url = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
        month, day = date.month, date.day
        if len(str(month)) == 1:
            month='0'+str(month)
        if len(str(day)) == 1:
            day='0'+str(day)
        input_date = str(date.year - 1911) + '/' + str(month) + '/' + str(day)
        print(date.year, date.month, date.day, input_date)
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
        payload = {'qdate': input_date, 'selectType': 'ALLBUT0999'}
        source_code = requests.post(url, headers=headers, data=payload)
        source_code.encoding = 'utf-8'
        plain_text = source_code.text
        soup = BeautifulSoup(plain_text, 'html.parser')
        date = soup.find_all('thead')[0].find_all('tr')[0].text
        ymd = re.findall(r"\d\d\d?\d?", date)
        col=[]
        for th in soup.find_all('table')[table].find_all('thead'):
            for tr in th.find_all('tr'):
                h = ['年月日']
                for td in tr.find_all('td'):
                    h.append(td.string)
                col.append(h)
        for i in range(1, len(col)):
            l = [col[i]]
            for tr in soup.find_all('table')[table].find_all('tbody')[i-1].find_all('tr'):
                r = [str(int(ymd[0])+1911) + '/' + ymd[1] + '/' + ymd[2]]
                for td in tr.find_all('td'):
                    r.append(td.string)
                l.append(r)
            df = DataFrame(l)
            df.columns = df.ix[0, :]
            df = df.ix[1:len(df), :]
            df = df.replace(',', '', regex=True)
            print(df)
            if (list(df) == tablename0) or (list(df) == tablename1):  # 不可以這樣寫 if list(df) == tablename0 or tablename1:
                try:
                    df['指數'] = df['指數'].str.strip()
                    c.executemany('INSERT INTO `大盤統計資訊` VALUES (?,?,?,?,?,?)', df.values.tolist())
                    conn.commit()
                    print(date, list(df),0)
                except Exception as e:
                    print(e)
                    pass
            elif list(df) == tablename2:
                df['成交統計'] = df['成交統計'].str.strip()
                c.executemany('INSERT INTO `大盤成交統計` VALUES (?,?,?,?,?)', df.values.tolist())
                conn.commit()
                print(date, list(df),1)
            else:
                print('does not match any table')
    except Exception as e:
        print(e)
        pass

###----每日收盤行情(全部(不含權證、牛熊證))----

# #----create table----
tablename='每日收盤行情(全部(不含權證、牛熊證))'
table=1
thead=0

url = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'qdate': '105/02/22', 'selectType': 'ALLBUT0999'}
source_code = requests.post(url, headers=headers, data=payload)
source_code.encoding = 'utf-8'
plain_text = source_code.text
print(plain_text)
soup = BeautifulSoup(plain_text, 'html.parser')
date = soup.find_all('table')[table].find_all('thead')[thead].find_all('tr')[0].find_all('span')[0].text
print(date)

h = ['年月日']
for th in soup.find_all('table')[table].find_all('thead')[thead].find_all('tr')[1].find_all('td'):
    h.append(th.string)
l = [h]
for tr in soup.find_all('table')[table].find_all('tbody')[thead].find_all('tr'):
    r = [date]
    for td in tr.find_all('td'):
        r.append(td.string)
    l.append(r)
df = DataFrame(l)
df.columns = df.ix[0, :]
df = df.ix[1:len(df), :]
df['證券代號'], df['證券名稱'] = df['證券代號'].str.strip(), df['證券名稱'].str.strip()
tablename0 =list(df)
names = list(df)
sql = "create table `" +tablename + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ',PRIMARY KEY (`年月日`,`證券代號`))'
c.execute(sql)

# ---read from sqlite---
tablename='每日收盤行情(全部(不含權證、牛熊證))0'
df = read_sql_query('SELECT * from `'+tablename+'`', conn)
sql = "rename `" +tablename + "`"+tablename+'0'
c.execute(sql)
df=df.sort_values(['年月日','證券代號'],ascending=[True,True])
df.to_sql(tablename, conn,index=False)
# --- drop table ---
c.execute("drop table `" +tablename + "1`")


#----update using datetime----

import datetime
import re
tablename='每日收盤行情(全部(不含權證、牛熊證))'
table=1
thead=0
tbody=[0]
startdate = datetime.datetime(2016, 4, 8)
delta = datetime.datetime.now() - startdate
for t in range(delta.days):
    date = startdate + datetime.timedelta(days=t + 1)
    for body in tbody:
        try:
            url = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
            month, day = date.month, date.day
            if len(str(month)) == 1:
                month='0'+str(month)
            if len(str(day)) == 1:
                day='0'+str(day)
            input_date = str(date.year - 1911) + '/'  + str(month) + '/' + str(day)
            print(date.year, date.month, date.day, input_date)
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
            payload = {'qdate': input_date, 'selectType': 'ALLBUT0999'}
            source_code = requests.post(url, headers=headers, data=payload)
            source_code.encoding = 'utf-8'
            plain_text = source_code.text
            soup = BeautifulSoup(plain_text, 'html.parser')
            title = soup.find_all('table')[table].find_all('thead')[thead].find_all('tr')[0].text
            ymd = re.findall(r"\d\d\d?", title)
            h = ['年月日']
            for th in soup.find_all('table')[table].find_all('thead')[thead].find_all('tr')[1].find_all('td'):
                h.append(th.string)

            l = [h]
            for tr in soup.find_all('table')[table].find_all('tbody')[body].find_all('tr'):
                r = [str(int(ymd[0]) + 1911) + '/' + ymd[1] + '/' + ymd[2]]
                for td in tr.find_all('td'):
                    r.append(td.string)
                l.append(r)
            df = DataFrame(l)
            df.columns = df.ix[0, :]
            df = df.ix[1:len(df), :]
            df = df.replace(',', '', regex=True)
            df['證券代號'], df['證券名稱'] = df['證券代號'].str.strip(), df['證券名稱'].str.strip()
            print(title)
            if list(df)==['年月日', '證券代號', '證券名稱', '成交股數', '成交筆數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌(+/-)', '漲跌價差', '最後揭示買價', '最後揭示買量', '最後揭示賣價', '最後揭示賣量', '本益比']:
                c.executemany('INSERT INTO `'+tablename+'` VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', df.values.tolist())
                conn.commit()
            else:
                print(False)
        except Exception as e:
            print(e)
            pass

# ---rename table---
report='每日收盤行情(全部(不含權證、牛熊證))'
sql='ALTER TABLE `'+ report +'` RENAME TO`' + report  +'0`'
c.execute(sql)

df = read_sql_query('SELECT * from `'+report +'0`', conn)
df['證券代號'] = df['證券代號'].astype(str).replace('\.0', '', regex=True)
df['證券代號'], df['證券名稱'] = df['證券代號'].str.strip(), df['證券名稱'].str.strip()
df = df.drop_duplicates(['年月日', '證券代號'])
# ----create table----
names = list(df)
c = conn.cursor()
sql = "create table `" + report  + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ', PRIMARY KEY (`年月日`, `證券代號`))'
c.execute(sql)
# ----inserting data----
sql = 'INSERT INTO `' + report  + '` VALUES (?'
n = [',?'] * (len(names) - 1)
for h in n:
    sql = sql + h
sql = sql + ')'
c.executemany(sql, df.values.tolist())
conn.commit()
print('done')

c = conn.cursor()
sql = "drop table `" + report  + "0`"
c.execute(sql)







