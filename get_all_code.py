import json
import time
import requests
from pymongo import MongoClient


def all():
    url = f"http://data.stats.gov.cn/easyquery.htm?id=zb&dbcode={decode}&wdcode=zb&m=getTree"

    all_list = []
    resp = requests.get(url)
    list = json.loads(resp.text)
    for i in list:
        dict = {}
        dict["level_1"] = i["name"]
        dict["id"] = i["id"]
        all_list.append(dict)
    return all_list


def part(all_list):
    id = all_list["id"]
    url = f"http://data.stats.gov.cn/easyquery.htm?id={id}&dbcode={decode}&wdcode=zb&m=getTree"
    all_part_list = []
    resp = requests.get(url)
    list = json.loads(resp.text)
    for i in list:
        dict = {}
        dict["level_1"] = all_list["level_1"]
        dict["level_2"] = i["name"]
        dict["id"] = i["id"]
        all_part_list.append(dict)
    time.sleep(1)
    return all_part_list


def part_2(part_lsit):
    id = part_lsit["id"]
    url = f"http://data.stats.gov.cn/easyquery.htm?id={id}&dbcode={decode}&wdcode=zb&m=getTree"
    all_part_list = []
    resp = requests.get(url)
    list = json.loads(resp.text)
    if list == []:
        all_part_list.append(part_lsit)
    else:
        for i in list:
            dict = {}
            dict["level_1"] = part_lsit["level_1"]
            dict["level_2"] = part_lsit["level_2"]
            try:
                dict["level_3"] = i["name"]
            except:
                dict["level_3"] = None
            dict["id"] = i["id"]
            all_part_list.append(dict)
    time.sleep(1)
    return all_part_list


def connect():
    client = MongoClient(host='localhost', port=27017)
    # client = MongoClient(host='52.83.244.2', port=27017)  # 连接mongodb
    db = client.national
    data = db.code_data  # 用于存放爬取数据的表
    return data



if __name__ == '__main__':
    # 爬取所有的代表id
    decode = "hgyd"   # dbcode=hgyd表示月度数据,hgjd表示爬取季度数据,hjnd表示爬取年度数据
    Cookie = "JSESSIONID=D8CC9F9B7386D045CB83602329004140; u=2"
    data = connect()
    all_list = all()
    for i in all_list:
        all_part_list = part(i)
        print("------------------------------------------------------")
        for i in all_part_list:
            all_part_list2 = part_2(i)
            for i in all_part_list2:
                code = i["id"]
                data.insert_one(i)

