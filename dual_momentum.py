from bs4 import BeautifulSoup
import urllib3
import time
import requests
import sqlite3

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

class DB_Operation:
    def __init__(self, table_name):
        self.conn = sqlite3.connect('test.db')
        self.table_name = str(table_name) 
        
    def create_table(self):
        c = self.conn.cursor()
        str = '''CREATE TABLE IF NOT EXISTS {table_name}
        (ID INTEGER PRIMARY KEY  AUTOINCREMENT   NOT NULL,
        Y_M              TEXT   NOT NULL,
        price           FLOAT    NOT NULL);'''.format(table_name="[" + self.table_name + "]")
        c.execute(str)
        self.conn.commit()

    def select_price(self):
        price = []
        c = self.conn.cursor()
        c.execute("SELECT price FROM {table_name}".format(table_name="[" + self.table_name + "]"))
        for row in c.fetchall():
            price.append(row[0])
        return price

    def search_data(self, Y_M):
        c = self.conn.cursor()
        str = '''SELECT * FROM {table_name} WHERE Y_M="{Y_M}";'''.format(table_name="[" + self.table_name + "]", Y_M= Y_M)
        c.execute(str)
        rows = c.fetchall()
        if len(rows) == 0:
            return 1
        else:
            return 0
    
    def insert_data(self, Y_M, price):
        c = self.conn.cursor()
        str = '''SELECT * FROM {table_name} WHERE Y_M="{Y_M}";'''.format(table_name="[" + self.table_name + "]", Y_M= Y_M)
        c.execute(str)
        rows = c.fetchall()
        if len(rows) == 0:
            c.execute('''INSERT INTO {table_name} (Y_M,price) \
            VALUES ("{Y_M}",{price})'''.format(table_name="[" + self.table_name + "]", Y_M= Y_M, price = price))
        self.conn.commit()

    def connnect_close(self):
        self.conn.close()


if __name__ == "__main__":
    # stock_number = input("stock_number: ")
    initial_money = 1000
    year = 107
    stock = []
    debt = []
    DB_debt = DB_Operation("00694B")
    DB_debt.create_table()
    DB_stock = DB_Operation("0050")
    DB_stock.create_table()
    

    for i in range(year, 110+1):
        for j in range(1, 13):
            year_str = str(i+1911)
            month_str = str(j).zfill(2)
            print(year_str)
            print(month_str)
            if DB_stock.search_data(str(i)+"_"+month_str) == 1:
                average_month_price = find_closing_price_TWSE("0050", year_str, month_str)
                stock.append(float(average_month_price))
                DB_stock.insert_data(str(i)+"_"+month_str, float(average_month_price))
            
            year_str = str(year)
            month_str = str(j).zfill(2)
            if DB_debt.search_data(str(i)+"_"+month_str) == 1:
                average_month_price = find_closing_price_TPEX("00694B",year_str,month_str)
                debt.append(float(average_month_price))
                DB_debt.insert_data(str(i)+"_"+month_str, float(average_month_price))

    dual_result = dual_momentum(DB_stock, DB_debt)
    stock_only = stock_only_cal(DB_stock)
    debt_only = debt_only_cal(DB_debt)
    print("2018-2021")
    print("dual_result: ", dual_result)
    print("stock_only: ", stock_only)
    print("debt_only: ", debt_only)

