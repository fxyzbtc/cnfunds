import requests
import json
import csv
from multiprocessing.dummy import Pool as ThreadPool
from tqdm import tqdm
import tenacity

import pandas as pd
import requests

url = 'http://www.fundsmart.com.cn/api/fund.screener.data.php'

headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh-TW;q=0.7,zh;q=0.6,ms;q=0.5',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Cookie': 'PHPSESSID=ff1482ca1b45e32eea70f438492b52e0; __utmc=1; __utmz=1.1690633807.1.1.utmcsr=sg.search.yahoo.com|utmccn=(referral)|utmcmd=referral|utmcct=/; __utma=1.1310858476.1690633807.1691245931.1691248315.4; __utmt=1; __utmb=1.10.9.1691248568385',
    'DNT': '1',
    'Host': 'www.fundsmart.com.cn',
    'Origin': 'https://www.fundsmart.com.cn',
    'Referer': 'https://www.fundsmart.com.cn/mutualfund/screener',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"'
}

payload = {
    'type': 'all',
    'static[area]': '',
    'static[assetal]': '',
    'static[manage]': '',
    'static[org]': '',
    'static[exchange]': '',
    'static[trade]': '',
    'static[trade2]': '',
    'static[csrcFundCategory]': '',
    'static[navUnit]': '',
    'static[indexMarketBasis]': '',
    'static[indexMarketStyle]': '',
    'static[indexValueStyle]': '',
    'static[assetalKeepType]': '',
    'static[estable]': '',
    'static[size]': '',
    'static[company]': '',
    'static[manager]': '',
    'static[cust]': '',
    'static[sale]': '',
    'static[gzzs]': '',
    'static[qsfc]': '',
    'static[ccgp]': '',
    'static[comzc]': '',
    'static[comzb]': '',
    'static[orgForm]': '',
    'static[controllerProperty]': '',
    'static[comnx]': '',
    'static[manxl]': '',
    'static[manxb]': '',
    'static[mannx]': '',
    'static[manrz]': '',
    'static[number]': '',
    'static[mansm]': '',
    'static[manjm]': '',
    'static[custOrgType]': '',
    'static[custzc]': '',
    'static[custzb]': '',
    'static[custnx]': '',
    'static[saleOrgType]': '',
    'static[salezc]': '',
    'static[salezb]': '',
    'static[salenx]': '',
    'static[assetalKeepLength]': '',
    'static[stockStylePeriodInResult]': '',
    'static[enddate]': '',
    'static[nameLike]': '',
    'static[periods]': '',
    'static[showtype]': 'Assetal',
    'static[endtype]': 'history',
    'static[pageSize]': '1000',
    'static[include]': '',
    'static[splitZyByResult]': 'false',
    'static[saleIntersection]': 'false',
    'static[isDependentFund]': 'true',
    'static[ahFund]': 'false',
    'static[fundShareType]': '',
    'static[companyExclude]': 'false',
    'static[managerExclude]': 'false',
    'static[custodianExclude]': 'false',
    'static[saleExclude]': 'false',
    'static[companyOnly]': 'false',
    'static[managerOnly]': 'false',
    'static[custodianOnly]': 'false',
    'static[saleOnly]': 'false',
    'static[categories]': 'Assetal,OrganizationForm',
    'static[o]': 'establishYears',
    'static[s]': 'ASC',
    'static[c]': '',
    'static[l]': '',
    'static[stockStyle]': '',
    'static[stockStylePeriods]': '',
    'static[splittype]': '',
    'static[categoryDetails]': '',
    'static[companyId]': '',
    'static[swHeavyPctOfNav]': ''
}

response = requests.post(url, headers=headers, data=payload)

dfs = []



@tenacity.retry(stop=tenacity.stop_after_attempt(7), wait=tenacity.wait_random(min=1, max=10))
def pull_funds_perpage(page):
    payload['static[p]'] = str(page)
    response = requests.post(url, headers=headers, data=payload)
    result_bean = response.json()['resultBean']
    df = pd.json_normalize(result_bean)
    return df

def pull_all(pages=response.json()['page'] + 1):
    with ThreadPool(5) as p:
        result = list(tqdm(p.imap(pull_funds_perpage, range(1,pages))))
    
    return pd.concat(result, ignore_index=True)

if __name__ == '__main__':
    funds = pull_all()
    funds.to_csv('全基金信息.csv', index=False)
