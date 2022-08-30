from bs4 import BeautifulSoup
import urllib3
import time
import requests
import sqlite3

def find_closing_price_TWSE(stock_number, year, month):
    http = urllib3.PoolManager()
    url = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY_AVG?response=html&date='+year+month+'01&stockNo='+stock_number
    try: 
        res = http.request('GET', url)
        html_doc = res.data
        res.close()
    except requests.exceptions.RequestException as e:
        print(e)


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
    try: 
        res = http.request('GET', url)
        html_doc = res.data
        res.close()
    except requests.exceptions.RequestException as e:
        print(e)
    soup = BeautifulSoup(html_doc, 'html.parser')
    b_tag = soup.find_all("table")
    trs = b_tag[0].find_all("tr")
    day_closing_price = []
    for tr in trs[2:-1]:
        tds = tr.find_all("td")
        day_closing_price.append(float(tds[6].text))
    average_month_price = sum(day_closing_price) / len(day_closing_price)
    # print(average_month_price)
    return average_month_price

def dual_momentum(stock, debt):
    initial = float(1000)
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

def stock_only_cal(stock):
    initial = float(1000)
    for i in range(2, len(stock)):
        stock_change = (stock[i] - stock[i-1])/stock[i-1]
        initial = initial * (1+stock_change)
    return initial

def debt_only_cal(debt):
    initial = float(1000)
    for i in range(2, len(debt)):
        debt_change = (debt[i] - debt[i-1])/debt[i-1]
        initial = initial * (1+debt_change)
    return initial

if __name__ == "__main__":
    # stock_number = input("stock_number: ")
    initial_money = 1000
    year = 107
    stock = []
    debt = []

    for i in range(year, 110+1):
        for j in range(1, 13):
            year_str = str(i+1911)
            month_str = str(j).zfill(2)
            print(year_str)
            print(month_str)
            average_month_price = find_closing_price_TWSE("0050", year_str, month_str)
            stock.append(float(average_month_price))
            year_str = str(year)
            month_str = str(j).zfill(2)
            average_month_price = find_closing_price_TPEX("00694B",year_str,month_str)
            debt.append(float(average_month_price))
    dual_result = dual_momentum(stock, debt)
    stock_only = stock_only_cal(stock)
    debt_only = debt_only_cal(debt)
    print("2018-2021")
    print("dual_result: ", dual_result)
    print("stock_only: ", stock_only)
    print("debt_only: ", debt_only)

