import requests
import json
import csv
from multiprocessing.dummy import Pool as ThreadPool
from tqdm import tqdm

FUND_URL = "http://www.fundsmart.com.cn/api/fund.list.data.php?d=&t=3&i={}"
INDICES_URL = 'http://www.fundsmart.com.cn/api/market.index.screener.data.php'

PARAMS_KUANJI = {
    'assets': '股票',
    'marketBasis': '宽泛',
    'pageSize': 300,
    'page': 1,
}

PARAMS_HANGYE = {
    'assets': '股票',
    'marketBasis': '狭窄',
    'pageSize': 300,
    'page': 1,
}

PARAMS_OVERSEAS = {
    'assets': '股票',
    'investArea': '发达市场,中国香港,全球配置,混合市场',
    'pageSize': 300,
    'page': 1,
}


def pull_indices():
    indices = []
    for params in PARAMS_HANGYE,PARAMS_KUANJI,PARAMS_OVERSEAS:
        resp = requests.post(INDICES_URL, data=params)
        for zhishu in resp.json()['list']:
            if '--' not in zhishu['ticker']:
                ticker = zhishu['ticker']
                indices.append(ticker)
    return indices

import tenacity

@tenacity.retry(stop=tenacity.stop_after_attempt(7), wait=tenacity.wait_random(min=1, max=10))
def getCoFunds(code):
    return requests.get(FUND_URL.format(code)).json()['list']

if __name__ == '__main__':
    indices = pull_indices()

    with ThreadPool(5) as p:
        r = list(tqdm(p.imap(getCoFunds, indices), total=len(indices)))

    with open('全市场指数基金信息.csv', 'w+') as f:
        fieldnames = list(r[0][0].keys())
        writer = csv.DictWriter(f, fieldnames=fieldnames)    
        writer.writeheader()
        for indices in r:
            for fund in indices:
                fund['indexTicker'] = "'" + fund['indexTicker'].zfill(6)
                writer.writerow(fund)
