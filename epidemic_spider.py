import urllib.request
import json
import pandas as pd
import re
import time
import requests
import datetime
import os
import pymysql
from sqlalchemy import create_engine


class Crawler:
    def __init__(self, host, user, passwd,  db, table):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.table = table
        self.crawl_timestamp = int()
        self.today = datetime.date.today().strftime('%Y%m%d')
        self.curPath = os.getcwd()
        self.curPath = self.curPath + '/' + self.today
        if not os.path.exists(self.curPath):
            os.makedirs(self.curPath)
        self.spider_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        today = datetime.date.today()
        self.yesterday = str(today - datetime.timedelta(days=1))
        self.source = 'none'

    def run(self):
        pass

    def crawler_data(self):
        pass

    def structure_data(self, raw_data):
        pass

    def save_db(self, df):
        # 判断表是否存在
        sql = 'show tables;'
        tables = [self.exeSQL(self.host, self.user, self.passwd, self.db, sql)]
        table_list = re.findall('(\'.*?\')', str(tables))
        table_list = [re.sub("'", '', each) for each in table_list if each != 'Tables_in_test']
        if self.table in table_list:
            # 先删除今天的历史数据
            sql = 'delete from {} where EPSDTC="{}" and EPSOUR="{}"'.format(self.table, self.yesterday, self.source)
            self.exeSQL(self.host, self.user, self.passwd, self.db, sql)
        # 再插入新鲜的数据
        engine = create_engine("mysql+pymysql://{}:{}@{}:3306/{}?charset=utf8mb4".format(self.user, self.passwd, self.host, self.db))
        # df.to_sql(name=self.table, con=engine, if_exists='append', index=False, index_label=False)
        # 获取风险地区信息
        sql = 'select RSPROV, RSCITY, RSLEVEL from risk where RSSDTC = "{}"'.format(self.yesterday)
        risk_df = pd.DataFrame(self.exeSQL(self.host, self.user, self.passwd, self.db, sql))
        df = pd.merge(df, risk_df, left_on=['EPPROV', 'EPCITY'], right_on=['RSPROV', 'RSCITY'], how='left')
        df.fillna("", inplace=True)
        df['EPRLEVEL'] = df['RSLEVEL']
        df = df[self.columns]
        df.to_sql(name=self.table, con=engine, if_exists='append', index=False, index_label=False)
        print('数据更新成功！')

    def table_exists(self, con, table_name):        #这个函数用来判断表是否存在
        sql = "show tables;"
        tables = [con.execute(sql)]
        tables = [con.fetchall()]
        table_list = re.findall('(\'.*?\')',str(tables))
        table_list = [re.sub("'",'',each) for each in table_list]
        if table_name in table_list:
            return 1        #存在返回1
        else:
            return 0        #不存在返回0

    def exeSQL(self, host, user, passwd, db, sql):
        # 打开数据库连接
        conn = pymysql.connect(host=host, user=user, password=passwd, database=db, charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)
        try:
            # 使用 cursor() 方法创建一个游标对象 cursor
            cursor = conn.cursor()
            print('数据库连接成功..')
            # 执行SQL语句
            cursor.execute(sql)
            result = cursor.fetchall()
            # 确认修改
            conn.commit()
            # 关闭游标
            cursor.close()
            # 关闭链接
            conn.close()
            print("语句 {} 执行成功！".format(sql))
            return result
        except Exception as e:
            print("语句 {} 执行失败！".format(sql))
            print('error!! ', e)
            return None

class Crawler_baidu(Crawler):
    def __init__(self, host, user, passwd, db, table):
        super().__init__(host, user, passwd, db, table)
        self.source = 'baidu'
        self.columns = ['EPIDDTC', 'EPSAREA', 'EPCNT', 'EPPROV', 'EPCITY', 'EPRLEVEL', 'EPNLC', 'EPNLAC', 'EPNIC',
                        'EPEHC',
                        'EPEOIC', 'EPEDC', 'EPCDC', 'EPCNUM', 'EPDTNUM', 'EPSDTC', 'EPSOUR', 'ETLDTC']

    def run(self):
        # get data from internet
        raw_data = self.crawler_data()
        # structure data
        struct_data = self.structure_data(raw_data)
        # save baidu data into db
        if struct_data.shape[0] > 0:
            self.save_db(df=struct_data)

    def crawler_data(self):
        url='https://voice.baidu.com/act/newpneumonia/newpneumonia/?from=osari_aladin_banner'
        with urllib.request.urlopen(url) as f:
            html_source=f.read().decode()

        h=str(html_source)
        h=h.encode('UTF-8')
        h=h.decode('unicode_escape')
        h=h.replace('\/','/')
        a=h.index('<script>require')
        b=h.index('{index.enter();});</script>')
        h=h[a:b+26]
        a=h.index('<script type="application/json"')
        h=h[a:len(h)]
        h=h[52:h.index('</script>')]
        json_str=json.loads(h)
        chinaList = json_str["component"][0]["caseList"]
        globalList = json_str["component"][0]["globalList"]
        lastUpdateTime = json_str["component"][0]["mapLastUpdatedTime"]
        print('finished crawled from baidu(epidemic info)!!')
        return [chinaList, globalList, lastUpdateTime]

    def structure_data(self, raw_data):
        lastUpdateTime = raw_data[2].replace(".", "-")
        # 国内
        raw_df = pd.DataFrame(raw_data[0])
        new_df = pd.DataFrame(columns=self.columns)
        i = 0
        for index, row in raw_df.iterrows():
            overseas_confirmedRelative, overseas_curConfirm = '', ''
            for item in row['subList']:
                if item['city'] == '境外输入':
                    overseas_confirmedRelative = item['confirmedRelative']
                    overseas_curConfirm = item['curConfirm']
                else:
                    # item['city'] = item['city'].replace("区", "")
                    new_df.loc[i] = [lastUpdateTime, '亚洲', '中国', row['area'], item['city'], '', item['nativeRelative'],
                                     item['asymptomaticRelative'], '', item['curConfirm'], '', item['curConfirm'],
                                     item['confirmed'], item['crued'], item['died'], self.yesterday, self.source, self.spider_time]
                    i += 1
            new_df.loc[i] = [lastUpdateTime, '亚洲', '中国', row['area'], '', '', row['nativeRelative'],
                             row['asymptomaticRelative'], overseas_confirmedRelative, row['curConfirm'],
                             overseas_curConfirm, row['curConfirm'], row['confirmed'], row['crued'], row['died'],
                             self.yesterday, self.source, self.spider_time]
            i += 1

        # 国外疫情
        aboard_df = pd.DataFrame(raw_data[1])
        for index, row in aboard_df.iterrows():
            if row['area'] != '热门':
                for item in row['subList']:
                    new_df.loc[i] = [lastUpdateTime, row['area'], item['country'], '', '', '', item['confirmedRelative'], 0,
                                     0, 0, 0, item['curConfirm'], item['confirmed'], item['crued'], item['died'],
                                     self.yesterday, self.source, self.spider_time]
                    i += 1
                new_df.loc[i] = [lastUpdateTime, row['area'], '', '', '', '', row['confirmedRelative'], 0,
                                 0, 0, 0, row['curConfirm'], row['confirmed'], row['crued'],
                                 row['died'], self.yesterday, self.source, self.spider_time]
                i += 1
        int_col = ['EPNLC', 'EPNLAC', 'EPNIC', 'EPEHC', 'EPEOIC', 'EPEDC', 'EPCDC', 'EPCNUM', 'EPDTNUM']
        for col in int_col:
            new_df[col] = pd.to_numeric(new_df[col], errors='coerce').fillna(0).astype(int)

        return new_df

class  Crawler_tengxun(Crawler):
    def __init__(self, host, user, passwd,  db, table):
        super().__init__(host, user, passwd,  db, table)
        self.source = 'tengxun'
        self.columns = ['EPIDDTC', 'EPSAREA', 'EPCNT', 'EPPROV', 'EPCITY', 'EPRLEVEL', 'EPNLC', 'EPNLAC', 'EPNIC', 'EPEHC',
                     'EPEOIC', 'EPEDC', 'EPCDC', 'EPCNUM', 'EPDTNUM', 'EPSDTC', 'EPSOUR', 'ETLDTC']

    def run(self):
        # get data from internet
        raw_data = self.crawler_data()
        # structure data
        struct_data = self.structure_data(raw_data)
        # save data into db
        if struct_data.shape[0] > 0:
            self.save_db(df=struct_data)

    def crawler_data(self):
        # 国内疫情数据
        url = "https://api.inews.qq.com/newsqa/v1/query/inner/publish/modules/list?modules=statisGradeCityDetail,diseaseh5Shelf"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
            "Referer": "https://news.qq.com/"}
        chinaList = []
        lastUpdateTime = ''
        try:
            response = requests.get(url=url, headers=headers)
            if response.status_code == 200:
                res = response.json()
                chinaList = res['data']["diseaseh5Shelf"]["areaTree"][0]["children"]
                lastUpdateTime = res['data']["diseaseh5Shelf"]["lastUpdateTime"]
        except requests.ConnectionError as e:
            print('catch china List wrong from tengxun', e.args)


        # 国外疫情数据
        url = "https://api.inews.qq.com/newsqa/v1/automation/modules/list?modules=FAutoCountryConfirmAdd,WomWorld,WomAboard"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
            "Referer": "https://news.qq.com/"}
        globalList = []
        try:
            response = requests.get(url=url, headers=headers)
            if response.status_code == 200:
                res = response.json()
                globalList = res['data']['WomAboard']

        except requests.ConnectionError as e:
            print('catch china List wrong from tengxun', e.args)
        return [chinaList, globalList, lastUpdateTime]

    def structure_data(self, raw_data):
        # 国内
        raw_df = pd.DataFrame(raw_data[0])
        lastUpdateTime = raw_data[2][0:-3] #截取到时分，不要秒
        new_df = pd.DataFrame(
            columns=self.columns)
        i = 0
        for index, row in raw_df.iterrows():
            overseas_confirmedRelative, overseas_curConfirm = '', ''
            for item in row['children']:
                if item['name'] == '境外输入':
                    overseas_confirmedRelative = item['today']['confirm']
                    overseas_curConfirm = item['total']['nowConfirm']
                else:

                    new_df.loc[i] = [lastUpdateTime, '亚洲', '中国', row['name'], item['name'], '', item['today']['confirm'], '',
                                     '', item['total']['nowConfirm'], '', item['total']['nowConfirm'],
                                     item['total']['confirm'], item['total']['heal'], item['total']['dead'],
                                     self.yesterday, self.source, self.spider_time]
                    i += 1
            new_df.loc[i] = [lastUpdateTime, '亚洲', '中国', row['name'], '', '', row['today']['confirm'], '',
                             overseas_confirmedRelative, row['total']['nowConfirm'], overseas_curConfirm,
                             row['total']['nowConfirm'], row['total']['confirm'], row['total']['heal'],
                             row['total']['dead'], self.yesterday, self.source, self.spider_time]
            i += 1

        # 国外疫情
        aboard_df = pd.DataFrame(raw_data[1])
        aboard_df = aboard_df.rename(
            columns={"continent": "EPSAREA", "name": "EPCNT", "confirmAdd": "EPNLC", "nowConfirm": "EPEDC",
                     "confirm": "EPCDC", "heal": "EPCNUM", "dead": "EPDTNUM"})
        aboard_df['EPSAREA'] = aboard_df['EPSAREA'].apply(lambda x: '其他' if x == '' else x)
        for col in self.columns:
            if not (col in aboard_df.columns):
                aboard_df[col] = ''
        aboard_df = aboard_df[self.columns]
        aboard_df['EPIDDTC'] = lastUpdateTime
        aboard_df['EPSDTC'] = self.yesterday
        aboard_df['EPSOUR'] = self.source
        aboard_df['ETLDTC'] = self.spider_time
        # 合并
        new_all_df = pd.concat([new_df, aboard_df], ignore_index=True)
        # 列格式调整
        int_col = ['EPNLC', 'EPNLAC', 'EPNIC', 'EPEHC', 'EPEOIC', 'EPEDC', 'EPCDC', 'EPCNUM', 'EPDTNUM']
        for col in int_col:
            new_all_df[col] = pd.to_numeric(new_all_df[col], errors='coerce').fillna(0).astype(int)
        # 对腾讯中的中国城市命名做规范化处理，对标百度数据
        cityMap = pd.read_csv('cityMap.csv')
        new_all_df = pd.merge(new_all_df, cityMap, left_on=['EPPROV', 'EPCITY'], right_on=['area', 'city_raw'], how='left')
        new_all_df.fillna("", inplace=True)
        new_all_df['EPCITY'] = new_all_df.apply(
            lambda row: row["city_clean"] if row["city_clean"] is not None and row["city_clean"] != "" else row[
                "EPCITY"], axis=1)
        new_all_df = new_all_df[self.columns]
        return new_all_df


class  Crawler_tengxun_risk(Crawler):
    def __init__(self, host, user, passwd,  db, table):
        super().__init__(host, user, passwd,  db, table)
        self.source = 'tengxun_risk'

    def run(self):
        # get data from internet
        raw_data = self.crawler_data()
        # structure data
        struct_data = self.structure_data(raw_data)
        # save data into db
        if struct_data.shape[0] > 0:
            self.save_db(df=struct_data)

    def crawler_data(self):
        # 国内风险地区数据
        url = "https://wechat.wecity.qq.com/api/PneumoniaTravelNoAuth/queryAllRiskLevel"

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36",
            "Referer": "https://feiyan.wecity.qq.com/",
            "Content-Type": "application/json"}

        payload = {"args": {"req": {}}, "service": "PneumoniaTravelNoAuth", "func": "queryAllRiskLevel",
                   "context": {"userId": "5f0aa1902a0444ddb630f7db3f73c7ea"}}


        try:
            response = requests.post(url=url, json=payload, headers=headers)
            if response.status_code == 200:
                res = response.json()
                res = res.get('args').get('rsp')
                latestDeadlineDate = res.get('latestDeadlineDate')
                mediumRiskAreaList = res.get('mediumRiskAreaList')
                highRiskAreaList = res.get('highRiskAreaList')
        except requests.ConnectionError as e:
            print('catch china List wrong from tengxun', e.args)

        return [highRiskAreaList, mediumRiskAreaList, latestDeadlineDate]

    def structure_data(self, raw_data):
        # 中风险
        mediumRiskArea = []
        for province in raw_data[1]:
            for city in province['areaRiskDetails']:
                tmpDict = {}
                tmpDict['RSPROV'] = province['provinceName']
                tmpDict['RSCITY'] = city['cityName']
                if city['cityName'].replace('市', '') in ['北京', '上海', '天津', '重庆']:
                    tmpDict['RSPROV'] = province['provinceName']
                    tmpDict['RSCITY'] = city['areaName']
                tmpDict['mid_dander_area'] = '中风险'
                tmpDict['mid_dander_count'] = len(city['communityRiskDetails'])
                mediumRiskArea.append(tmpDict)
        highRiskArea = []
        for province in raw_data[0]:
            for city in province['areaRiskDetails']:
                tmpDict = {}
                tmpDict['RSPROV'] = province['provinceName']
                tmpDict['RSCITY'] = city['cityName']
                if city['cityName'].replace('市', '') in ['北京', '上海', '天津', '重庆']:
                    tmpDict['RSPROV'] = province['provinceName']
                    tmpDict['RSCITY'] = city['areaName']
                tmpDict['RSLEVEL'] = '高风险'
                tmpDict['high_dander_count'] = len(city['communityRiskDetails'])
                highRiskArea.append(tmpDict)
        highRiskArea_df = pd.DataFrame(highRiskArea)
        highRiskArea_df = highRiskArea_df.groupby(['RSPROV', 'RSCITY', 'RSLEVEL']).agg(
            RSHIGHCOUNT=('high_dander_count', 'sum'))
        highRiskArea_df = pd.DataFrame(highRiskArea_df)
        highRiskArea_df.reset_index(inplace=True)
        mediumRiskArea_df = pd.DataFrame(mediumRiskArea)
        mediumRiskArea_df = mediumRiskArea_df.groupby(['RSPROV', 'RSCITY', 'mid_dander_area']).agg(
            RSMIDCOUNT=('mid_dander_count', 'sum'))
        mediumRiskArea_df = pd.DataFrame(mediumRiskArea_df)
        mediumRiskArea_df.reset_index(inplace=True)
        risk_area_df = pd.merge(highRiskArea_df, mediumRiskArea_df, on=['RSPROV', 'RSCITY'],
                                how='outer')
        risk_area_df['RSLEVEL'].fillna(risk_area_df["mid_dander_area"], inplace=True)
        risk_area_df.fillna(0, inplace=True)
        new_all_df = risk_area_df[['RSPROV', 'RSCITY', 'RSLEVEL', 'RSHIGHCOUNT', 'RSMIDCOUNT']]
        new_all_df['RSIDDTC'] = raw_data[2]
        new_all_df['RSSDTC'] = self.yesterday
        new_all_df['ETLDTC'] = self.spider_time
        new_all_df['RSSOUR'] = 'tengxun'

        # 对中国城市命名做规范化处理，对标疫情明细百度数据
        cityMap = pd.read_csv('cityMap.csv')
        new_all_df = pd.merge(new_all_df, cityMap, left_on=['RSPROV', 'RSCITY'], right_on=['area', 'city_raw'], how='left')
        new_all_df.fillna("", inplace=True)
        new_all_df['RSCITY'] = new_all_df.apply(
            lambda row: row["city_clean"] if row["city_clean"] is not None and row["city_clean"] != "" else row["RSCITY"], axis=1)
        new_all_df = new_all_df[
            ['RSIDDTC', 'RSPROV', 'RSCITY', 'RSLEVEL', 'RSHIGHCOUNT', 'RSMIDCOUNT',  'RSSDTC', 'RSSOUR', 'ETLDTC']]

        return new_all_df

    def save_db(self, df):
        # 判断表是否存在
        sql = 'show tables;'
        tables = [self.exeSQL(self.host, self.user, self.passwd, self.db, sql)]
        table_list = re.findall('(\'.*?\')', str(tables))
        table_list = [re.sub("'", '', each) for each in table_list if each != 'Tables_in_test']
        if self.table in table_list:
            # 先删除今天的历史数据
            sql = 'delete from {} where RSSDTC="{}"'.format(self.table, self.yesterday)
            self.exeSQL(self.host, self.user, self.passwd, self.db, sql)
        # 再插入新鲜的数据
        engine = create_engine("mysql+pymysql://{}:{}@{}:3306/{}?charset=utf8mb4".format(self.user, self.passwd, self.host, self.db))
        df.to_sql(name=self.table, con=engine, if_exists='append', index=False, index_label=False)

        print('数据更新成功！')

if __name__ == '__main__':
    # risk
    crawler = Crawler_tengxun_risk('localhost', 'root', '123456', 'test', 'risk')
    crawler.run()
    # tengxun
    crawler = Crawler_tengxun('localhost', 'root', '123456', 'test', 'ep')
    crawler.run()
    # baidu
    crawler = Crawler_baidu('localhost', 'root', '123456', 'test', 'ep')
    crawler.run()
    # os.system('pause')