# sqlite3 can only run in console
from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\TEJ.sqlite3')
c = conn.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *

#----get unique id----
url = 'http://www.twse.com.tw/ch/trading/exchange/BWIBBU/BWIBBU_d.php'
payload = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36',
    'input_date': '105/02/15', 'select2': 'ALL', 'order': 'STKNO'}
source_code = requests.post(url, params=payload)
source_code.encoding = 'big5'
plain_text = source_code.text
print(plain_text)
soup = BeautifulSoup(plain_text, 'html.parser')
date = soup.find_all('thead')[0].find_all('tr')[0].find_all('th')[0].string

h = ['年月日']
for tr in soup.find_all('thead')[0].find_all('tr')[1]:
    h.append(tr.text)
l = [h]
for tr in soup.find_all('tbody')[0].find_all('tr'):
    r = [date.split()[0] + date.split()[0]]
    for td in tr.find_all('td'):
        r.append(td.string)
    l.append(r)
df = DataFrame(l)
df.columns = df.ix[0, :]
df = df.ix[1:len(df), :]

id=df.ix[:, 1].unique().tolist()
for u in id:
    print(u)

# #----create table----
# h = ['證券代號','證券名稱']
# for td in soup.find_all('table')[7].find_all('tr')[1].find_all('td'):
#     h.append(td.text)
# l = [h]
# table=soup.find_all('table')[7].find_all('tr')
# for tr in table[2:len(table)-1]:
#     r = [date.split()[1] , date.split()[2]]
#     for td in tr.find_all('td'):
#         r.append(td.string)
#     l.append(r)
# df = DataFrame(l)
# df.columns = df.ix[0, :]
# df = df.ix[1:len(df), :]
# names = list(df)
# sql = "create table `" + '個股日成交資訊' + "`(" + "'" + names[0] + "'"
# for n in names[1:len(names)]:
#     sql = sql + ',' + "'" + n + "'"
# sql = sql + ')'
# c.execute(sql)
#
# #----test inserting data----
# c.executemany('INSERT INTO `個股日成交資訊` VALUES (?,?,?,?,?,?,?,?,?,?,?)', df.values.tolist())
# conn.commit()

#----renew----
# y=['2015','2014','2013','2012','2011','2010','2009','2008','2007','2006','2005','2004','2003','2002','2001','2000','1999','1998','1997','1996','1995','1994','1993']
# m=['12','11','10','09','08','07','06','05','04','03','02','01']
# print(id)
y=['2016']
m=['02']
for year in y:
    for month in m:
        for u in id:
            try:
                url='http://www.twse.com.tw/ch/trading/exchange/STOCK_DAY/genpage/Report' + year + month + '/' + year + month + '_F3_1_8_' + u + '.php?STK_NO=' + u + '&myear=' + year + '&mmon=' + month
                payload = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'
                    }
                source_code = requests.get(url, params=payload)
                source_code.encoding = 'big5'
                plain_text = source_code.text
                soup = BeautifulSoup(plain_text, 'html.parser')
                date = soup.find_all('table')[7].find_all('tr')[0].find_all('div')[0].string

                h = ['證券代號','證券名稱']
                for td in soup.find_all('table')[7].find_all('tr')[1].find_all('td'):
                    h.append(td.text)
                l = [h]
                table=soup.find_all('table')[7].find_all('tr')
                for tr in table[2:len(table)-1]:
                    r = [date.split()[1] , date.split()[2]]
                    for td in tr.find_all('td'):
                        r.append(td.string)
                    l.append(r)
                df = DataFrame(l)
                df.columns = df.ix[0, :]
                df = df.ix[1:len(df), :]
                df = df.replace(',', '', regex=True)
                print(date)

                c.executemany('INSERT INTO `個股日成交資訊` VALUES (?,?,?,?,?,?,?,?,?,?,?)', df.values.tolist())
                conn.commit()
            except:
                pass