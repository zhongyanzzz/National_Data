import json
import random
import time
from user_agent import *
import requests
from pymongo import MongoClient
from requests.adapters import HTTPAdapter


def connect():
    client = MongoClient(host='localhost', port=27017)
    # client = MongoClient(host='52.83.244.2', port=27017)  # 连接mongodb
    db = client.national
    code_data = db.jd_code_data  # 用于存放爬取数据的表
    data = db.jd_data
    return code_data,data

def detail(part2_list):
    id = part2_list["id"]
    url2 = f"http://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode={dbcode}&rowcode=zb&colcode=sj&wds=%5B%5D&dfwds=%5B%7B%22wdcode%22%3A%22zb%22%2C%22valuecode%22%3A%22{id}%22%7D%5D"
    url = f"http://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode={dbcode}&rowcode=zb&colcode=sj&wds=%5B%5D&dfwds=%5B%7B%22wdcode%22%3A%22sj%22%2C%22valuecode%22%3A%22{choose}%22%7D%5D"
    header = {"Cookie": Cookie,
              "user_angents":random.choice(USER_AGENTS)}
    session = requests.Session()
    session.headers = header
    session.mount('http://', HTTPAdapter(max_retries=5))
    session.mount('https://', HTTPAdapter(max_retries=5))
    resp = session.get(url,  timeout=10)
    resp = session.get(url2,  timeout=10)
    json_str = json.loads(resp.text)
    list = json_str["returndata"]["wdnodes"]
    for i in list[0]["nodes"]:
        list1 = []
        dict = {}
        dict["level_1"] = part2_list["level_1"]
        dict["level_2"] = part2_list["level_2"]
        try:
            if part2_list["level_3"] is not None:
                dict["level_3"] = part2_list["level_3"]
        except:
            dict["level_3"] = None
        if i["unit"] == "":
            dict["level_4"] = i["cname"]
        else:
            dict["level_4"] = i["cname"]  + " " + "(" + i["unit"] + ")"
        dict["code"] = i["code"]
        for j in list[1]["nodes"]:
            dict1 = {}
            dict1["cname"] = j["cname"]
            dict1["code"] = j["code"]
            list1.append(dict1)
        dict["time"] = list1
        for k in json_str["returndata"]["datanodes"]:
            if k["wds"][0]["valuecode"] == dict["code"]:
                for m in dict["time"]:
                    if k["wds"][1]["valuecode"] == m["code"]:
                        TIME = m["cname"]
                        dict.update({TIME: k["data"]["strdata"]})
        del dict["time"]
        dict["Scrap_Time"] = "2019-06-20"
        if data.find({"code": dict["code"], "Scrap_Time": dict["Scrap_Time"]}).count() > 0:
            pass
        else:
            print(dict)
            data.insert_one(dict)
    return dict

def get_start_time():
    start_time = time.time()
    # url = "http://ip.16yun.cn:817/myip/pl/4f6f78ae-c6d5-40ec-a642-c1a72f12959a/?s=zvrluhywdb&u=webos&count=1"
    # resp = requests.get(url)
    # text = "-----"# resp.text
    # proxy = {"http://": str(text), "http": str(text)}
    return start_time

if __name__ == '__main__':
    Cookie = "JSESSIONID=40A7CC299BB20BA273E2EBACC68A20A2; u=1; experience=show"

    choose = "LAST36" # LAST36 表示爬取最近36个月,201605表示爬取2016年5月单独一个月的数据
    dbcode = "hgyd"  # hgyd 表示爬取月度数据,hgjd表示爬取季度数据,hgnd表示爬取年度数据

    code_data,data = connect()
    start_time = get_start_time()
    for i in code_data.find({},{"_id":0}):
        code = i["id"] + "01"
        end_time = time.time()
        past_time = (end_time - start_time) * 1000
        print(past_time)
        if past_time >180000:
            start_time = get_start_time()
        if data.find({"code": code}).count() > 0:
            pass
        else:
            detail(i)
            time.sleep(5)

