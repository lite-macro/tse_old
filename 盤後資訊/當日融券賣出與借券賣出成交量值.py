# sqlite3 can only run in console
from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\TEJ.sqlite3')
c = conn.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *
url = 'http://www.twse.com.tw/ch/trading/exchange/TWTASU/TWTASU.php'
#----create table----

payload = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36',
    'input_date': '105/02/15'}
source_code = requests.post(url, params=payload)
source_code.encoding = 'big5'
plain_text = source_code.text
print(plain_text)
soup = BeautifulSoup(plain_text, 'html.parser')
date = soup.find_all('thead')[0].find_all('tr')[0].find_all('th')[0].text

th1=soup.find_all('thead')[0].find_all('tr')[1].find_all('th')
th2=soup.find_all('thead')[0].find_all('tr')[2].find_all('th')

l = [['年月日',th1[0].string,th1[1].string+th2[0].string,th1[1].string+th2[1].string ,th1[2].string+th2[0].string,th1[2].string+th2[1].string]]
for tr in soup.find_all('tbody')[0].find_all('tr'):
    r = [date.split()[0]]
    for td in tr.find_all('td'):
        r.append(td.string)
    l.append(r)
df = DataFrame(l)
df.columns = df.ix[0, :]
df = df.ix[1:len(df), :]
df=df.replace(',','',regex=True)

names = list(df)
sql = "create table `" + '當日融券賣出與借券賣出成交量值(元)' + "`(" + "'" + names[0] + "'"
for n in names[1:len(names)]:
    sql = sql + ',' + "'" + n + "'"
sql = sql + ')'
c.execute(sql)

#----renew----

y=['105']
m=['02']
d=['23','22','21','20', '19', '18', '17', '16', '15','14', '13', '12', '11','10','09','08','07', '06','05','04','03', '02','01']

# y=['104','103','102','101','100','99','98','97']
# m=['12','11','10','09','08','07','06','05','04','03','02','01']
# d=['31', '30', '29','28','27','26','25','24', '23','22','21','20', '19', '18', '17', '16', '15','14', '13', '12', '11','10','09','08','07', '06','05','04','03', '02','01']
for year in y:
    for month in m:
        for day in d:
            try:
                input_date=year+'/'+month+'/'+day
                payload = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36',
                           'input_date': input_date}
                source_code = requests.post(url,params=payload)
                source_code.encoding = 'big5'
                plain_text = source_code.text
                soup = BeautifulSoup(plain_text, 'html.parser')
                date=soup.find_all('thead')[0].find_all('tr')[0].find_all('th')[0].text

                th1 = soup.find_all('thead')[0].find_all('tr')[1].find_all('th')
                th2 = soup.find_all('thead')[0].find_all('tr')[2].find_all('th')

                l = [['年月日', th1[0].string, th1[1].string + th2[0].string, th1[1].string + th2[1].string,
                      th1[2].string + th2[0].string, th1[2].string + th2[1].string]]
                for tr in soup.find_all('tbody')[0].find_all('tr'):
                    r = [date.split()[0]]
                    for td in tr.find_all('td'):
                        r.append(td.string)
                    l.append(r)
                df = DataFrame(l)
                df.columns = df.ix[0, :]
                df = df.ix[1:len(df), :]
                df = df.replace(',', '', regex=True)
                print(date)

                c.executemany('INSERT INTO `當日融券賣出與借券賣出成交量值(元)` VALUES (?,?,?,?,?,?)', df.values.tolist())
                conn.commit()
            except:
                pass



