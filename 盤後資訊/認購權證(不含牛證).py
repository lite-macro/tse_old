from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\TEJ.sqlite3')
c = conn.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *

###----認購權證(不含牛證)----

# #----create table----
tablename='認購權證(不含牛證)'
table=0
thead=0
url = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'qdate': '105/02/22', 'selectType': '0999'}
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

l = [h]
l[0][18]='標的'+l[0][18]
l[0][19]='標的'+l[0][19]
l[0][20]='標的證券'+l[0][20]
for tr in soup.find_all('table')[table].find_all('tbody')[0].find_all('tr'):
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
sql = sql + ', PRIMARY KEY (`年月日`,`證券代號`))'
c.execute(sql)

#----renew----
tablename='認購權證(不含牛證)'
table=0
thead=0
tbody=[0]

def f(tablename,y,m,d,tbody):
    for year in y:
        for month in m:
            for day in d:
                for body in tbody:
                    try:
                        url = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
                        payload = {'qdate':year+'/'+month+'/'+day, 'selectType': '0999'}
                        source_code = requests.post(url, headers=headers, data=payload)
                        source_code.encoding = 'utf-8'
                        plain_text = source_code.text
                        soup = BeautifulSoup(plain_text, 'html.parser')
                        date = soup.find_all('thead')[0].find_all('tr')[0].text
                        ymd = re.findall(r"\d\d\d?", date)
                        h = ['年月日']
                        for th in soup.find_all('table')[table].find_all('thead')[thead].find_all('tr')[1].find_all('td'):
                            h.append(th.string)

                        l = [h]
                        l[0][18] = '標的' + l[0][18]
                        l[0][19] = '標的' + l[0][19]
                        l[0][20] = '標的證券' + l[0][20]
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
                        print(date)

                        c.executemany('INSERT INTO `'+tablename+'` VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', df.values.tolist())
                        conn.commit()
                    except Exception as e:
                        print(e)
                        pass
y=['105']
m=['02','01']
d=['31','30','29','28','27','26','25','24','23','22','21','20','19','18','17','16','15','14','13','12','11','10','09','08','07','06','05','04','03','02','01']
f(tablename,y,m,d,tbody)
y=['104','103','102','101','100','99','98','97','96','95','94','93']
m=['12','11','10','09','08','07','06','05','04','03','02','01']
d=['31','30','29','28','27','26','25','24','23','22','21','20','19','18','17','16','15','14','13','12','11','10','09','08','07','06','05','04','03','02','01']
f(tablename,y,m,d,tbody)


###----認售權證(不含熊證)----

# #----create table----
tablename='認售權證(不含熊證)'
table=0
thead=0
url = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'qdate': '105/02/22', 'selectType': '0999P'}
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

l = [h]
l[0][18]='標的'+l[0][18]
l[0][19]='標的'+l[0][19]
l[0][20]='標的證券'+l[0][20]
for tr in soup.find_all('table')[table].find_all('tbody')[0].find_all('tr'):
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
sql = sql + ', PRIMARY KEY (`年月日`,`證券代號`))'
c.execute(sql)

#----renew----
tablename='認售權證(不含熊證)'
table=0
thead=0
tbody=[0]

def f1(tablename,y,m,d,tbody):
    for year in y:
        for month in m:
            for day in d:
                for body in tbody:
                    try:
                        url = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
                        payload = {'qdate':year+'/'+month+'/'+day, 'selectType': '0999P'}
                        source_code = requests.post(url, headers=headers, data=payload)
                        source_code.encoding = 'utf-8'
                        plain_text = source_code.text
                        soup = BeautifulSoup(plain_text, 'html.parser')
                        date = soup.find_all('thead')[0].find_all('tr')[0].text
                        ymd = re.findall(r"\d\d\d?", date)
                        h = ['年月日']
                        for th in soup.find_all('table')[table].find_all('thead')[thead].find_all('tr')[1].find_all('td'):
                            h.append(th.string)

                        l = [h]
                        l[0][18] = '標的' + l[0][18]
                        l[0][19] = '標的' + l[0][19]
                        l[0][20] = '標的證券' + l[0][20]
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
                        print(date)

                        c.executemany('INSERT INTO `'+tablename+'` VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', df.values.tolist())
                        conn.commit()
                    except Exception as e:
                        print(e)
                        pass
y=['105']
m=['02','01']
d=['31','30','29','28','27','26','25','24','23','22','21','20','19','18','17','16','15','14','13','12','11','10','09','08','07','06','05','04','03','02','01']
f1(tablename,y,m,d,tbody)
y=['104','103','102','101','100','99','98','97','96','95','94','93']
m=['12','11','10','09','08','07','06','05','04','03','02','01']
d=['31','30','29','28','27','26','25','24','23','22','21','20','19','18','17','16','15','14','13','12','11','10','09','08','07','06','05','04','03','02','01']
f1(tablename,y,m,d,tbody)

###----牛證(不含可展延牛證)----

# #----create table----
tablename='牛證(不含可展延牛證)'
table=0
thead=0
url = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'qdate': '105/02/22', 'selectType': '0999C'}
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

l = [h]
l[0][18]='標的'+l[0][18]
l[0][19]='標的'+l[0][19]
l[0][20]='標的證券'+l[0][20]
for tr in soup.find_all('table')[table].find_all('tbody')[0].find_all('tr'):
    r = [date]
    for td in tr.find_all('td'):
        r.append(td.string)
    l.append(r)
df = DataFrame(l)
df.columns = df.ix[0, :]
df = df.ix[1:len(df), :]
tablename0 =list(df)
names = list(df)
sql = "create table `" +tablename + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ', PRIMARY KEY (`年月日`,`證券代號`))'
c.execute(sql)

#----renew----
tablename='牛證(不含可展延牛證)'
table=0
thead=0
tbody=[0]

def f2(tablename,y,m,d,tbody):
    for year in y:
        for month in m:
            for day in d:
                for body in tbody:
                    try:
                        url = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
                        payload = {'qdate':year+'/'+month+'/'+day, 'selectType': '0999C'}
                        source_code = requests.post(url, headers=headers, data=payload)
                        source_code.encoding = 'utf-8'
                        plain_text = source_code.text
                        soup = BeautifulSoup(plain_text, 'html.parser')
                        date = soup.find_all('thead')[0].find_all('tr')[0].text
                        ymd = re.findall(r"\d\d\d?", date)
                        h = ['年月日']
                        for th in soup.find_all('table')[table].find_all('thead')[thead].find_all('tr')[1].find_all('td'):
                            h.append(th.string)

                        l = [h]
                        l[0][18] = '標的' + l[0][18]
                        l[0][19] = '標的' + l[0][19]
                        l[0][20] = '標的證券' + l[0][20]
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
                        print(date)

                        c.executemany('INSERT INTO `'+tablename+'` VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', df.values.tolist())
                        conn.commit()
                    except Exception as e:
                        print(e)
                        pass
y=['105']
m=['02','01']
d=['31','30','29','28','27','26','25','24','23','22','21','20','19','18','17','16','15','14','13','12','11','10','09','08','07','06','05','04','03','02','01']
f2(tablename,y,m,d,tbody)
y=['104','103','102','101','100','99','98','97','96','95','94','93']
m=['12','11','10','09','08','07','06','05','04','03','02','01']
d=['31','30','29','28','27','26','25','24','23','22','21','20','19','18','17','16','15','14','13','12','11','10','09','08','07','06','05','04','03','02','01']
f2(tablename,y,m,d,tbody)

###----熊證(不含可展延熊證)----

# #----create table----
tablename='熊證(不含可展延熊證)'
table=0
thead=0
url = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'qdate': '105/02/22', 'selectType': '0999B'}
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

l = [h]
l[0][18]='標的'+l[0][18]
l[0][19]='標的'+l[0][19]
l[0][20]='標的證券'+l[0][20]
for tr in soup.find_all('table')[table].find_all('tbody')[0].find_all('tr'):
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
sql = sql + ', PRIMARY KEY (`年月日`,`證券代號`))'
c.execute(sql)

#----renew----
tablename='熊證(不含可展延熊證)'
table=0
thead=0
tbody=[0]

def f3(tablename,y,m,d,tbody):
    for year in y:
        for month in m:
            for day in d:
                for body in tbody:
                    try:
                        url = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
                        payload = {'qdate':year+'/'+month+'/'+day, 'selectType': '0999B'}
                        source_code = requests.post(url, headers=headers, data=payload)
                        source_code.encoding = 'utf-8'
                        plain_text = source_code.text
                        soup = BeautifulSoup(plain_text, 'html.parser')
                        date = soup.find_all('thead')[0].find_all('tr')[0].text
                        ymd = re.findall(r"\d\d\d?", date)
                        h = ['年月日']
                        for th in soup.find_all('table')[table].find_all('thead')[thead].find_all('tr')[1].find_all('td'):
                            h.append(th.string)

                        l = [h]
                        l[0][18] = '標的' + l[0][18]
                        l[0][19] = '標的' + l[0][19]
                        l[0][20] = '標的證券' + l[0][20]
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
                        print(date)

                        c.executemany('INSERT INTO `'+tablename+'` VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', df.values.tolist())
                        conn.commit()
                    except Exception as e:
                        print(e)
                        pass
y=['105']
m=['02','01']
d=['31','30','29','28','27','26','25','24','23','22','21','20','19','18','17','16','15','14','13','12','11','10','09','08','07','06','05','04','03','02','01']
f3(tablename,y,m,d,tbody)
y=['104','103','102','101','100','99','98','97','96','95','94','93']
m=['12','11','10','09','08','07','06','05','04','03','02','01']
d=['31','30','29','28','27','26','25','24','23','22','21','20','19','18','17','16','15','14','13','12','11','10','09','08','07','06','05','04','03','02','01']
f3(tablename,y,m,d,tbody)

###----可展延牛證----

# #----create table----
tablename='可展延牛證'
table=0
thead=0
url = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'qdate': '105/02/22', 'selectType': '0999X'}
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

l = [h]
l[0][18]='標的'+l[0][18]
l[0][19]='標的'+l[0][19]
l[0][20]='標的證券'+l[0][20]
for tr in soup.find_all('table')[table].find_all('tbody')[0].find_all('tr'):
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
sql = sql + ', PRIMARY KEY (`年月日`,`證券代號`))'
c.execute(sql)

#----renew----
tablename='可展延牛證'
table=0
thead=0
tbody=[0]

def f4(tablename,y,m,d,tbody):
    for year in y:
        for month in m:
            for day in d:
                for body in tbody:
                    try:
                        url = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
                        payload = {'qdate':year+'/'+month+'/'+day, 'selectType': '0999X'}
                        source_code = requests.post(url, headers=headers, data=payload)
                        source_code.encoding = 'utf-8'
                        plain_text = source_code.text
                        soup = BeautifulSoup(plain_text, 'html.parser')
                        date = soup.find_all('thead')[0].find_all('tr')[0].text
                        ymd = re.findall(r"\d\d\d?", date)
                        h = ['年月日']
                        for th in soup.find_all('table')[table].find_all('thead')[thead].find_all('tr')[1].find_all('td'):
                            h.append(th.string)

                        l = [h]
                        l[0][18] = '標的' + l[0][18]
                        l[0][19] = '標的' + l[0][19]
                        l[0][20] = '標的證券' + l[0][20]
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
                        print(date)

                        c.executemany('INSERT INTO `'+tablename+'` VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', df.values.tolist())
                        conn.commit()
                    except Exception as e:
                        print(e)
                        pass
y=['105']
m=['02','01']
d=['31','30','29','28','27','26','25','24','23','22','21','20','19','18','17','16','15','14','13','12','11','10','09','08','07','06','05','04','03','02','01']
f4(tablename,y,m,d,tbody)
y=['104','103','102','101','100','99','98','97','96','95','94','93']
m=['12','11','10','09','08','07','06','05','04','03','02','01']
d=['31','30','29','28','27','26','25','24','23','22','21','20','19','18','17','16','15','14','13','12','11','10','09','08','07','06','05','04','03','02','01']
f4(tablename,y,m,d,tbody)

