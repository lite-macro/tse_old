##----- pe is '0.00' when pe < 0 -----

from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\db\\tse.sqlite3')
c = conn.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *
import re
import datetime

lastdate = datetime.datetime(2017, 5, 19)  #last time 9/03
delta = datetime.datetime.now() - lastdate
###----大盤統計資訊 + 大盤成交統計 in one new----

#----update using datetime----
tablename0 = ['年月日', '指數', '收盤指數', '漲跌(+/-)', '漲跌點數', '漲跌百分比(%)']
tablename1 = ['年月日', '報酬指數', '收盤指數', '漲跌(+/-)', '漲跌點數', '漲跌百分比(%)']
tablename2 = ['年月日', '成交統計', '成交金額(元)', '成交股數(股)', '成交筆數']
# thead is irregular in tse, in early years, 1 thead contains 1 tr, in later years, 1 thead sometimes contains 2 tr
table = 0
for t in range(delta.days):
    date = lastdate + datetime.timedelta(days=t + 1)
    try:
        url = 'http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX.php'
        month, day = date.month, date.day
        if len(str(month)) == 1:
            month = '0'+str(month)
        if len(str(day)) == 1:
            day = '0'+str(day)
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
        col = []
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
            print(df.head(10))
            print(list(df))
            # if (list(df) == tablename0) or (list(df) == tablename1):
            if list(df) == tablename0:
                try:
                    df['指數'] = df['指數'].str.strip()
                    c.executemany('INSERT INTO `大盤統計資訊` VALUES (?,?,?,?,?,?)', df.values.tolist())
                    conn.commit()
                    print(date, list(df),0)
                except Exception as e:
                    print(e)
                    pass
            elif list(df) == tablename1:
                df['報酬指數'] = df['報酬指數'].str.strip()
                c.executemany('INSERT INTO `大盤統計資訊` VALUES (?,?,?,?,?,?)', df.values.tolist())
                conn.commit()
                print(date, list(df), 1)
            elif list(df) == tablename2:
                df['成交統計'] = df['成交統計'].str.strip()
                c.executemany('INSERT INTO `大盤成交統計` VALUES (?,?,?,?,?)', df.values.tolist())
                conn.commit()
                print(date, list(df),2)
            else:
                print('does not match any table')
    except Exception as e:
        print(e)
        pass

###----每日收盤行情(全部(不含權證、牛熊證))----
#----update using datetime----
tablename='每日收盤行情(全部(不含權證、牛熊證))'
table=1
thead=0
tbody=[0]
for t in range(delta.days):
    date = lastdate + datetime.timedelta(days=t + 1)
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

#----update using datetime----
tablename='個股日本益比、殖利率及股價淨值比'
for t in range(delta.days):
    date = lastdate + datetime.timedelta(days=t + 1)
    try:
        url = 'http://www.twse.com.tw/ch/trading/exchange/BWIBBU/BWIBBU_d.php'
        month, day = date.month, date.day
        if len(str(month)) == 1:
            month='0'+str(month)
        if len(str(day)) == 1:
            day='0'+str(day)
        input_date = str(date.year - 1911) + '/'  + str(month) + '/' + str(day)
        print(date.year, date.month, date.day, input_date)
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
        payload = {'input_date': input_date, 'select2': 'ALL', 'order': 'STKNO'}
        source_code = requests.post(url, headers=headers, data=payload)
        source_code.encoding = 'big5'
        plain_text = source_code.text
        soup = BeautifulSoup(plain_text, 'html.parser')
        title=soup.find_all('thead')[0].find_all('tr')[0].find_all('th')[0].string
        ymd = re.findall(r"\d\d\d?", title)
        h=['年月日']
        for tr in soup.find_all('thead')[0].find_all('tr')[1]:
            h.append(tr.text)
        l=[h]
        for tr in soup.find_all('tbody')[0].find_all('tr'):
            r = [str(int(ymd[0]) + 1911) + '/' + ymd[1] + '/' + ymd[2]]
            for td in tr.find_all('td'):
                r.append(td.string)
            l.append(r)
        df = DataFrame(l)
        df.columns=df.ix[0,:]
        df=df.ix[1:len(df),:]
        df['證券代號'], df['證券名稱'] = df['證券代號'].str.strip(), df['證券名稱'].str.strip()
        print(title)

        c.executemany('INSERT INTO `'+tablename+'` VALUES (?,?,?,?,?,?)', df.values.tolist())
        conn.commit()
    except Exception as e:
        print(e)
        pass

#----update using datetime----
tablename='當日融券賣出與借券賣出成交量值(元)'
for t in range(delta.days):
    date = lastdate + datetime.timedelta(days=t + 1)
    try:
        print(date.year, date.month, date.day)
        url = 'http://www.twse.com.tw/ch/trading/exchange/TWTASU/TWTASU.php'
        if len(str(date.month)) == 1:
            input_date= str(date.year-1911)+'/'+'0'+str(date.month)+'/'+str(date.day)
        if len(str(date.month)) == 2:
            input_date= str(date.year-1911)+'/'+str(date.month)+'/'+str(date.day)
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'}
        payload = {'input_date': input_date}
        source_code = requests.post(url, headers=headers, data=payload)
        source_code.encoding = 'big5'
        plain_text = source_code.text
        soup = BeautifulSoup(plain_text, 'html.parser')
        title=soup.find_all('thead')[0].find_all('tr')[0].find_all('th')[0].text
        ymd = re.findall(r"\d\d\d?", title)
        th1 = soup.find_all('thead')[0].find_all('tr')[1].find_all('th')
        th2 = soup.find_all('thead')[0].find_all('tr')[2].find_all('th')

        l = [['年月日', th1[0].string, th1[1].string + th2[0].string, th1[1].string + th2[1].string,
              th1[2].string + th2[0].string, th1[2].string + th2[1].string]]
        for tr in soup.find_all('tbody')[0].find_all('tr'):
            r = [str(int(ymd[0]) + 1911) + '/' + ymd[1] + '/' + ymd[2]]
            for td in tr.find_all('td'):
                r.append(td.string)
            l.append(r)
        df = DataFrame(l)
        df.columns = df.ix[0, :]
        df = df.ix[1:len(df), :]
        df = df.replace(',', '', regex=True)
        df = df.rename(columns={'證券名稱':'證券名稱0'})
        df['證券代號']=df['證券名稱0'].str.split().str[0]
        df['證券名稱']=df['證券名稱0'].str.split().str[1]
        df=df[['年月日', '證券代號', '證券名稱', '融券賣出成交數量', '融券賣出成交金額', '借券賣出成交數量', '借券賣出成交金額']]
        df['證券代號'], df['證券名稱'] = df['證券代號'].str.strip(), df['證券名稱'].str.strip()
        print(title)

        c.executemany('INSERT INTO `'+tablename+'` VALUES (?,?,?,?,?,?,?)', df.values.tolist())
        conn.commit()
    except Exception as e:
        print(e)
        pass


tablename='外資及陸資買賣超彙總表 (股)'
for t in range(delta.days):
    date = lastdate + datetime.timedelta(days=t + 1)
    try:
        url = 'http://www.twse.com.tw/ch/trading/fund/TWT38U/TWT38U.php'
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

#----update using datetime----
tablename='投信買賣超彙總表 (股)'
for t in range(delta.days):
    date = lastdate + datetime.timedelta(days=t + 1)
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

#----update using datetime----
tablename='自營商買賣超彙總表 (股)'

for t in range(delta.days):
    date = lastdate + datetime.timedelta(days=t + 1)
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




