##三大法人買賣超日報 全部(不含權證、牛熊證、可展延牛熊證)

# sqlite3 can only run in console
from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\TEJ.sqlite3')
c = conn.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *
url = 'http://www.twse.com.tw/ch/trading/fund/T86/T86.php'

#----create table----

# payload = {
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36',
#     'input_date': '105/02/03', 'select2': 'ALLBUT0999', 'sorting': 'by_issue'}
# source_code = requests.post(url, params=payload)
# source_code.encoding = 'big5'
# plain_text = source_code.text
# soup = BeautifulSoup(plain_text, 'html.parser')
# date = soup.find_all('thead')[0].find_all('tr')[0].find_all('th')[0].string
#
# h = ['年月日']
# for th in soup.find_all('thead')[0].find_all('tr')[1].find_all('th'):
#     h.append(th.text)
# l = [h]
# df = DataFrame(l)
# df.columns = df.ix[0, :]
# df = df.ix[1:len(df), :]
# names = list(df)
# sql = "create table `" + date.split()[1] + "`(" + "'" + names[0] + "'"
# for n in names[1:len(names)]:
#     sql = sql + ',' + "'" + n + "'"
# sql = sql + ')'
# c.execute(sql)

# y=['104','103','102','101']
# m=['12','11','10','09','08','07','06','05','04','03','02','01']
# d=['31', '30', '29','28','27','26','25','24', '23','22','21','20', '19', '18', '17', '16', '15','14', '13', '12', '11','10','09','08','07', '06','05','04','03', '02','01']

#----renew----

y=['105']
m=['02']
d=['15','14']

for year in y:
    for month in m:
        for day in d:
            try:
                input_date=year+'/'+month+'/'+day
                payload = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36',
                           'input_date': input_date, 'select2': 'ALLBUT0999','sorting': 'by_issue'}
                source_code = requests.post(url,params=payload)
                source_code.encoding = 'big5'
                plain_text = source_code.text
                soup = BeautifulSoup(plain_text, 'html.parser')
                #print(plain_text)
                #print(source_code)
                #print(source_code.headers['content-type'])
                #print(source_code.text)
                #print(source_code.json())
                date=soup.find_all('thead')[0].find_all('tr')[0].find_all('th')[0].string

                h=['年月日']
                for th in soup.find_all('thead')[0].find_all('tr')[1].find_all('th'):
                    h.append(th.text)
                l=[h]
                for tr in soup.find_all('tbody')[0].find_all('tr'):
                    r=[date.split()[0]]
                    for td in tr.find_all('td'):
                        r.append(td.string)
                    l.append(r)
                df = DataFrame(l)
                df.columns=df.ix[0,:]
                df=df.ix[1:len(df),:]
                df = df.replace(',', '', regex=True)
                print(date)

                c.executemany('INSERT INTO `三大法人買賣超日報(股)` VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', df.values.tolist())
                conn.commit()
            except:
                pass




