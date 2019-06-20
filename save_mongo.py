from pymongo import MongoClient
import pandas as pd


def get_data():
    '''
    连接远程mongodb，返回三张表的对象
    :return:
    '''

    client = MongoClient(host='localhost', port=27017)
    db = client.national  # 选取数据库
    data = []
    for i in db.yd_data.find({},{"_id":0}):
        del i["Tag"]
        data.append(i)
    return data

def other_data():
    '''
    连接远程mongodb，返回三张表的对象
    :return:
    '''

    client = MongoClient(host='localhost', port=27017)
    db = client.national  # 选取数据库
    other_data = []
    for i in db.yd_mouth_data.find({"Scrap_Time": TIME}, {"_id":0}):
        other_data.append(i)
    return other_data

def save_to_mongo(list):
    client = MongoClient(host='localhost', port=27017)
    db = client.national  # 选取数据库
    db.yd_data.remove({})
    db.yd_data.insert_many(list)


if __name__ == '__main__':
    TIME = "2019-06-10"
    list = []
    data = get_data()
    other_data = other_data()
    for i in range(len(data)):
        # data[i]["2016年5月"] = other_data[i]["2016年5月"]
        data[i]["Tag"] = "scraped"
        list.append(data[i])
    save_to_mongo(list)

