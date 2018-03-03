#----import----
from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\TEJ.sqlite3')
c = conn.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *
from functools import *
def mymerge(x,y):
    m=merge(x,y,how='outer')
    return m

#----get unique id----
url = 'http://www.twse.com.tw/ch/trading/exchange/BWIBBU/BWIBBU_d.php'
payload = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36',
    'input_date':'105/02/15', 'select2': 'ALL', 'order': 'STKNO'}
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

#----test connection----
CO_ID='5522'
#2015
YEAR='2015'
df1=DataFrame()
SEASON='2'
# url = "http://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID="+CO_ID+'&SYEAR='+YEAR+'&SSEASON='+SEASON+'&REPORT_ID=C'
# source_code = requests.get(url)
url='http://mops.twse.com.tw/server-java/t164sb01'
headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
payload = {'step':'1','CO_ID':CO_ID,'SYEAR':YEAR,'SSEASON':SEASON,'REPORT_ID':'C'}
source_code = requests.post(url,headers=headers,data=payload) #should use data instead of params
source_code.encoding = 'big5'
plain_text = source_code.text
print(plain_text)

#----test getting data----
CO_ID='5522'
stat=1
L=[]
for YEAR in ['2015','2014','2013']:
    for SEASON in ['4','3','2','1']:
        try:
            url = "http://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID="+CO_ID+'&SYEAR='+YEAR+'&SSEASON='+SEASON+'&REPORT_ID=C'
            source_code = requests.get(url)
            source_code.encoding = 'big5'
            plain_text = source_code.text
            soup = BeautifulSoup(plain_text, 'html.parser')
            h=[]
            for th in soup.find_all('table')[stat].find_all('tr')[0].find_all('th'):
                h.append(th.string)
            row=len(soup.find_all('table')[stat].find_all('tr')[2].find_all('td',{'align':""}))
            td=soup.find_all('table')[stat].find_all('tr')[2].find_all('td')
            l=[]
            for j in range(0,len(h)):
                x=[h[j]]
                for i in range(0, row*len(h),len(h)):
                    x.append(td[i+j].string)
                l.append(x)
            df = DataFrame(l)
            df.columns=df.ix[0,:]
            df=df.ix[1:len(df),:]
            L.append(df) # do not use L=L.append(df)!!
        except:
            pass
df1=reduce(mymerge,L)
print(df1)
df1=df1.rename(columns={'會計項目':'年季'})
df2=df1.drop_duplicates(subset='年季')
df2.index=arange(len(df2))
df2['證券代號']=CO_ID
df2=df2.sort(['年季', '證券代號'], ascending=[0,1])
cols = df2.columns.tolist()
cols = cols[-1:] + cols[:-1]
df2=df2[cols]
df2 = df2.replace(',', '', regex=True)
print(df2)

# #----create table----
# names = list(df2)
# c = conn.cursor()
# sql = "create table `" + "income" + "`(" + "'" + names[0] + "'"
# for n in names[1:len(names)]:
#      sql = sql + ',' + "'" + n + "'"
# sql = sql + ')'
# c.execute(sql)

# #----test inserting data----
# sql='INSERT INTO `income` VALUES (?'
# n=[',?'] * (len(cols)-1)
# for h in n:
#     sql=sql+h
# sql=sql+')'
# c.executemany(sql, df3.values.tolist())
# conn.commit()

#----main----
# tse好像會阻擋,而且資料會有少
stat=1
for CO_ID in id[0:len(id)]:
    try:
        L = []
        for YEAR in ['2015','2014','2013']:
            for SEASON in ['4','3','2','1']:
                try:
                    url='http://mops.twse.com.tw/server-java/t164sb01'
                    headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
                    payload = {'step':'1','CO_ID':CO_ID,'SYEAR':YEAR,'SSEASON':SEASON,'REPORT_ID':'C'}
                    source_code = requests.post(url,headers=headers, data=payload)
                    source_code.encoding = 'big5'
                    plain_text = source_code.text
                    soup = BeautifulSoup(plain_text, 'html.parser')
                    h=[]
                    for th in soup.find_all('table')[stat].find_all('tr')[0].find_all('th'):
                        h.append(th.string)
                    row=len(soup.find_all('table')[stat].find_all('tr')[2].find_all('td',{'align':""}))
                    td=soup.find_all('table')[stat].find_all('tr')[2].find_all('td')
                    l=[]
                    for j in range(0,len(h)):
                        x=[h[j]]
                        for i in range(0, row*len(h),len(h)):
                            x.append(td[i+j].string)
                        l.append(x)
                    df = DataFrame(l)
                    df.columns=df.ix[0,:]
                    df=df.ix[1:len(df),:]
                    L.append(df) # do not use L=L.append(df)
                except Exception as e:
                    print(e)
                    print('Wait 8 seconds')
                    time.sleep(8)
                    print('Continue...')
                    pass
        df1=reduce(mymerge,L)
        df1=df1.rename(columns={'會計項目':'年季'})
        df2=df1.drop_duplicates(subset='年季')
        df2.index=arange(len(df2))
        df2['證券代號']=CO_ID
        df2=df2.sort(['年季', '證券代號'], ascending=[0,1])
        cols = df2.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        df2=df2[cols]
        df2 = df2.replace(',', '', regex=True)
        print(df2)
        #df4=concat([df4, df3], ignore_index=True)
        #df4.to_csv('C:/Users/ak66h_000/OneDrive/webscrap/income/df4.csv',index=False)
        path='C:/Users/ak66h_000/OneDrive/webscrap/income/df'+CO_ID+'.csv'
        df2.to_csv(path,index=False)
    except Exception as e:
        print(e)
        print('Wait 10 seconds')
        time.sleep(10)
        print('Continue...')
        pass