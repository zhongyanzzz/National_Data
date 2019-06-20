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
        data.append(i)
    return data


if __name__ == '__main__':
    TIME = "2019-06-20"
    data = get_data()
    df = pd.DataFrame(data)
    df = df[
        ["level_1", "level_2", "level_3", "level_4", "code", "2019年5月","2019年4月", "2019年3月", "2019年2月", "2019年1月",
         "2018年12月", "2018年11月", "2018年10月", "2018年9月", "2018年8月", "2018年7月", "2018年6月", "2018年5月", "2018年4月",
         "2018年3月", "2018年2月", "2018年1月", "2017年12月", "2017年11月", "2017年10月", "2017年9月", "2017年8月", "2017年7月",
         "2017年6月", "2017年5月", "2017年4月", "2017年3月", "2017年2月", "2017年1月", "2016年12月", "2016年11月", "2016年10月",
         "2016年9月", "2016年7月", "2016年8月", "2016年6月","2016年5月"]]
    df.to_csv(f'hgyd_data_{TIME}.csv', index=0, encoding="utf-8")
    csv = pd.read_csv(f'hgyd_data_{TIME}.csv', encoding='utf-8')
    csv.to_excel(f'hgyd_data_{TIME}.xlsx', sheet_name='Raw')
