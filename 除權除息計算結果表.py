from sqlite3 import *
conn = connect('C:\\Users\\ak66h_000\\Documents\\db\\tse.sqlite3')
c = conn.cursor()

import requests
from bs4 import BeautifulSoup
from numpy import *
from pandas import *
from functools import *
import re

set_option("display.max_columns", 1000)
set_option('display.expand_frame_repr', False)
set_option('display.unicode.east_asian_width', True)

def mymerge(x, y):
    m = merge(x, y, how='outer')
    return m

#---- update ----
import datetime

lastdate = datetime.datetime(2003, 5, 4)  #last time 9/04
delta = datetime.datetime.now() - lastdate

tablename='除權息計算結果表'
ymd_e = []
for t in range(delta.days):
    date = lastdate + datetime.timedelta(days=t + 1)
    try:
        url = 'http://www.twse.com.tw/ch/trading/exchange/TWT49U/TWT49U.php'
        month, day = date.month, date.day
        if len(str(month)) == 1:
            month='0'+str(month)
        if len(str(day)) == 1:
            day='0'+str(day)
        input_date = str(date.year - 1911) + '/' + str(month) + '/' + str(day)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36'}
        payload = {'qdate': input_date}
        source_code = requests.post(url, headers=headers, data=payload)  # should use data instead of params
        source_code.encoding = 'utf8'
        plain_text = source_code.text
        soup = BeautifulSoup(plain_text, 'html.parser')
        title = soup.find_all('table')[0].find_all('tr')[0].get_text()
        d = re.findall(r'\d+', title)
        d = '/'.join([str(int(d[0]) + 1911), d[1], d[2]])
        h = []
        for th in soup.find_all('table')[0].find_all('tr')[1].find_all('td'):
                h.append(th.get_text())
        tb = [h]
        for tr in soup.find_all('table')[0].find_all('tr')[2:]:
            r = []
            for td in tr.find_all('td'):
                r.append(td.get_text())
            tb.append(r)
        df = DataFrame(tb)
        df.columns = df.ix[0, :]
        df = df.ix[1:len(df), :]
        df.insert(0, '年月日', d)
        df=df[['年月日', '股票代號', '股票名稱', '除權息前收盤價', '除權息參考價', '權值+息值']]
        df['權值+息值'], df['除權息前收盤價'], df['除權息參考價'] = df['除權息參考價'].str.replace(',', ''), df['除權息前收盤價'].str.replace(',', ''), df['除權息參考價'].str.replace(',', '')
        df['權值+息值'], df['除權息前收盤價'], df['除權息參考價'] = df['權值+息值'].astype(float), df['除權息前收盤價'].astype(float), df['除權息參考價'].astype(float)
        df['股票名稱'] = df['股票名稱'].str.strip()
        df['股票代號'] = df['股票代號'].astype(str)
        df = df.ix[df.股票代號 != '查無資料！']
        sql = 'insert into `{}`(`{}`) values({})'.format(tablename, '`,`'.join(list(df)), ','.join('?' * len(list(df))))
        c.executemany(sql, df.values.tolist())
        conn.commit()
        print(input_date, d)
    except Exception as e:
        print(e)
        ymd_e.append(d)
        pass


# tablename='除權息計算結果表'
# sql = 'create table `{}` (`{}`, PRIMARY KEY ({}))'.format(tablename, '`,`'.join(list(df)), '`年月日`, `股票代號`')
# c.execute(sql)
#
# sql = 'insert into `{}`(`{}`) values({})'.format(tablename, '`,`'.join(list(df)), ','.join('?' * len(list(df))))
# c.executemany(sql, df.values.tolist())
# conn.commit()

df=read_sql_query("SELECT * from `%s`"%tablename, conn)

tablename='除權息計算結果表'
df0 = read_sql_query("SELECT * from `%s`"%tablename, conn).rename(columns={'股票代號': '證券代號', '股票名稱': '證券名稱'})
df0['權值+息值'] = df0['權值+息值'].astype(float)
df0['證券名稱']=df0['證券名稱'].str.strip()
df0['證券名稱'].tolist()
tablename='每日收盤行情(全部(不含權證、牛熊證))'
df1 = read_sql_query("SELECT * from `%s`"%tablename, conn).replace('--', NaN)
df1['證券名稱']=df1['證券名稱'].str.strip()
df1['收盤價'] = df1['收盤價'].astype(float)
df = mymerge(df0, df1).sort_values(['證券代號', '年月日'])
df.insert(3, 'a', df['權值+息值'])
df.a=df.a.replace(NaN, 0)
df['b']=df.groupby(['證券代號'])['a'].cumsum()
df=df.reset_index(drop=True)
df['調整收盤價']=df.收盤價+df.b
df[['年月日', '證券代號', 'a', 'b', '權值+息值', '收盤價', '調整收盤價']]
