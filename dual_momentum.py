from bs4 import BeautifulSoup
import urllib3
import time
import requests
import sqlite3
import yfinance as yf


def download_from_YAHOO(stock_name, start_time, end_time):
    tsm = yf.download(stock_name,start=start_time,end=end_time)
    tsm.rename(columns = {'Adj Close':'Adj_Close'}, inplace=True)
    tsm = tsm.round(2)

    return tsm

def find_closing_price_TWSE(stock_number, year, month):
    http = urllib3.PoolManager()
    url = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY_AVG?response=html&date='+year+month+'01&stockNo='+stock_number 
    res = http.request('GET', url)
    html_doc = res.data
    res.close()


    soup = BeautifulSoup(html_doc, 'html.parser')
    b_tag = soup.find_all("table")
    trs = b_tag[0].find_all("tr")
    tds = trs[-1].find_all("td")
    ##print(tds[-1].text)
    closing_price = tds[-1].text
    return closing_price

def find_closing_price_TPEX(stock_number, year, month):
    http = urllib3.PoolManager()
    url = 'https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_print.php?l=zh-tw&d='+year+'/'+month+'&stkno='+stock_number+'&s=0,asc,0'
    res = http.request('GET', url)
    html_doc = res.data
    res.close()

    soup = BeautifulSoup(html_doc, 'html.parser')
    b_tag = soup.find_all("table")
    trs = b_tag[0].find_all("tr")
    day_closing_price = []
    for tr in trs[2:-1]:
        tds = tr.find_all("td")
        day_closing_price.append(float(tds[6].text))
    average_month_price = sum(day_closing_price) / len(day_closing_price)
    average_month_price = round(average_month_price,2)
    # print(average_month_price)
    return average_month_price

def dual_momentum(DB_stock, DB_debt):
    initial = float(1000)
    stock = DB_stock.select_price()
    debt = DB_debt.select_price()
    for i in range(1, len(stock)-1):
        stock_change = (stock[i] - stock[i-1])/stock[i-1]
        debt_change = (debt[i] - debt[i-1])/debt[i-1]
        if stock_change >= debt_change:
            money_change = (stock[i+1] - stock[i])/stock[i]
            initial = initial * (1+money_change)
        else:
            money_change = (debt[i+1] - debt[i])/debt[i]
            initial = initial * (1+money_change)
    return initial

def stock_only_cal(DB_stock):
    initial = float(1000)
    stock = DB_stock.select_price()
    for i in range(2, len(stock)):
        stock_change = (stock[i] - stock[i-1])/stock[i-1]
        initial = initial * (1+stock_change)
    return initial

def debt_only_cal(DB_debt):
    initial = float(1000)
    debt = DB_debt.select_price()
    for i in range(2, len(debt)):
        debt_change = (debt[i] - debt[i-1])/debt[i-1]
        initial = initial * (1+debt_change)
        
    return initial

class DB_Operation_daily:
    def __init__(self, table_name):
        self.conn = sqlite3.connect('finance_daily.db')
        self.table_name = str(table_name) 
        
    def create_table(self):
        c = self.conn.cursor()
        str = '''CREATE TABLE IF NOT EXISTS {table_name}
        (Date              TEXT   ,
        Open           FLOAT    ,
        High           FLOAT    ,
        Low           FLOAT    ,
        Close           FLOAT    ,
        Adj_Close           FLOAT    ,
        Volume           INT    );'''.format(table_name="[" + self.table_name + "]")
        c.execute(str)
        self.conn.commit()

    def pandas_dataframe_to_sqlite(self, finance_name):
        finance_name.to_sql(self.table_name, self.conn, if_exists='replace', index=True) 

    def select_Date(self, Y_M):
        price = []
        c = self.conn.cursor()
        c.execute('''SELECT Close FROM {table_name} WHERE Date LIKE "{strr}%"'''.format(table_name="[" + self.table_name + "]", strr = Y_M))
        for row in c.fetchall():
            price.append(row[0])
        return price

    def connnect_close(self):
        self.conn.close()

class DB_Operation_month:
    def __init__(self, table_name):
        self.conn = sqlite3.connect('finance_month.db')
        self.table_name = str(table_name) 
        
    def create_table(self):
        c = self.conn.cursor()
        str = '''CREATE TABLE IF NOT EXISTS {table_name}
        (Y_M              TEXT   ,
        price           FLOAT);'''.format(table_name="[" + self.table_name + "]")
        c.execute(str)
        self.conn.commit()

    def insert_data(self, Y_M, price):
        c = self.conn.cursor()
        str = '''SELECT * FROM {table_name} WHERE Y_M="{Y_M}";'''.format(table_name="[" + self.table_name + "]", Y_M= Y_M)
        c.execute(str)
        rows = c.fetchall()
        if len(rows) == 0:
            c.execute('''INSERT INTO {table_name} (Y_M,price) \
            VALUES ("{Y_M}",{price})'''.format(table_name="[" + self.table_name + "]", Y_M= Y_M, price = price))
        self.conn.commit()

    def select_price(self):
        price = []
        c = self.conn.cursor()
        c.execute("SELECT price FROM {table_name}".format(table_name="[" + self.table_name + "]"))
        for row in c.fetchall():
            price.append(row[0])
        return price

    def connnect_close(self):
        self.conn.close()

if __name__ == "__main__":
    # stock_number = input("stock_number: ")
    tsm = download_from_YAHOO('0050.TW', '2008-01-01', '2022-01-01')
    print(tsm)
    DB_0050_daily = DB_Operation_daily('0050.TW')
    DB_0050_daily.create_table()
    DB_0050_daily.pandas_dataframe_to_sqlite(tsm)

    for i in range(2008, 2021+1):
         for j in range(1, 13):
            month_str = str(j).zfill(2)
            price = DB_0050_daily.select_Date(str(i)+"-"+month_str)
            average_month_price = sum(price) / len(price)
            average_month_price = round(average_month_price,2)

            DB_0050_month = DB_Operation_month('0050.TW')
            DB_0050_month.create_table()
            DB_0050_month.insert_data(str(i)+"-"+month_str, float(average_month_price))
    
    tsm = download_from_YAHOO('SHY', '2008-01-01', '2022-01-01')
    print(tsm)
    DB_SHY_daily = DB_Operation_daily('SHY')
    DB_SHY_daily.create_table()
    DB_SHY_daily.pandas_dataframe_to_sqlite(tsm)

    for i in range(2008, 2021+1):
         for j in range(1, 13):
            month_str = str(j).zfill(2)
            price = DB_SHY_daily.select_Date(str(i)+"-"+month_str)
            average_month_price = sum(price) / len(price)
            average_month_price = round(average_month_price,2)

            DB_SHY_month = DB_Operation_month('SHY')
            DB_SHY_month.create_table()
            DB_SHY_month.insert_data(str(i)+"-"+month_str, float(average_month_price))
    
    dual_result = dual_momentum(DB_0050_month, DB_SHY_month)
    stock_only = stock_only_cal(DB_0050_month)
    debt_only = debt_only_cal(DB_SHY_month)
    print("2008-2021")
    print("dual_result: ", dual_result)
    print("stock_only: ", stock_only)
    print("debt_only: ", debt_only)

