import requests
import json
import csv
from multiprocessing.dummy import Pool as ThreadPool
from tqdm import tqdm
import akshare as ak

FUND_URL = "http://www.fundsmart.com.cn/api/fund.list.data.php?d=&t=3&i={}"
stock_df = ak.stock_zh_index_spot()


funds = [x[2:] for x in stock_df['symbol'].to_list()]
def getCoFunds(code):
    return requests.get(FUND_URL.format(code)).json()['list']

with ThreadPool(5) as p:
    r = list(tqdm(p.imap(getCoFunds, funds), total=len(funds)))

with open('全市场指数基金信息.csv', 'w+') as f:
    fieldnames = list(r[0][0].keys())
    writer = csv.DictWriter(f, fieldnames=fieldnames)    
    writer.writeheader()
    for funds in r:
        for fund in funds:
            fund['indexTicker'] = "'" + fund['indexTicker'].zfill(6)
            writer.writerow(fund)
