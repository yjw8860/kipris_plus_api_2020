import os, requests, re
import xml.etree.ElementTree as ET
import urllib.request
from time import sleep
import http
import urllib3
from tqdm import tqdm
import pandas as pd
from sqlalchemy import create_engine

class DataBase:
    def __init__(self, host, user, password, database_name):
        self.host = host
        self.user = user
        self.password = password
        self.database_name = database_name
        self.engine = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.database_name}')
        self.conn = self.engine.connect()
        self.TABLELIST = []
        self.checkTables()

    def checkTables(self):
        self.TABLELIST = []
        sql = 'SHOW TABLES;'
        result = self.conn.execute(sql)
        row = result.fetchall()
        table_list = []
        for i in row:
            table_list.append(i[0])
        self.TABLELIST = table_list

    def executeSQL(self, sql):
        #create new table: "CREATE TABLE book_details(book_id INT(5), title VARCHAR(20), price INT(5))"
        #alter table(ADD COLUMN): "ALTER TABLE book_details ADD column_name datatype"
        #alter table(MODIFY COLUMN): "ALTER TABLE book_details MODIFY column_name datatype"
        self.conn.execute(sql)

    def appendDataFrameToTable(self, df, table_name):
        df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False)

    def appendDataToTable(self, data, table_name):
        column_names = []
        sql = f'SHOW columns From {table_name};'
        result = self.conn.execute(sql)
        row = result.fetchall()
        for i in row:
            column_names.append(i[0])
        result = ', '.join(column_names)
        sql = f'INSERT INTO {table_name} ({result}) VALUES {data};'
        self.conn.execute(sql)

    def updateTable(self, df, table_name):
        df.to_sql(name=table_name, con=self.engine, if_exists='replace', index=False)

    def exportData(self, table_name):
        return pd.read_sql_table(table_name, self.conn)

    def dropTable(self, table_name):
        sql = f'DROP TABLE {table_name};'
        self.conn.execute(sql)


#다운로드(19600101 ~ 20191231)
class MAKEDATE:
    def __init__(self ,DB, last_year, last_month):
        self.DB = DB
        self.LAST_YEAR = last_year
        self.LAST_MONTH = last_month
        self.MONTH_LIST = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
        self.MONTH_LIST = self.MONTH_LIST[self.LAST_MONTH-1:]
        self.start_date_list = []
        self.end_date_list = []
        self.DATE_LIST = ['31', '28', '31', '30', '31', '30', '31', '31', '30', '31', '30', '31']
        if int(self.LAST_YEAR) % 4 == 0:
            self.DATE_LIST[1] = 29
            if int(self.LAST_YEAR) % 100 == 0:
                self.DATE_LIST[1] = 28
                if int(self.LAST_YEAR) % 400 == 0:
                    self.DATE_LIST[1] = 29
            else:
                self.DATE_LIST[1] = 28
        self.DATE_LIST = self.DATE_LIST[self.LAST_MONTH - 1:]
        for month, date in zip(self.MONTH_LIST, self.DATE_LIST):
            start_date = f'{self.LAST_YEAR}{month}01'
            end_date = f'{self.LAST_YEAR}{month}{date}'
            self.start_date_list.append(start_date)
            self.end_date_list.append(end_date)

class DOWNLOAD:
    def __init__(self, api_key, start_date, end_date):
        self.SAVE_ROOT_PATH = 'E:/data/trademark_from_1960/'
        self.BASIC_URL = 'http://plus.kipris.or.kr/kipo-api/kipi/trademarkInfoSearchService/getAdvancedSearch?'
        self.START_DATE = start_date
        self.END_DATE = end_date
        self.APPLICATION = 'application=TRUE'
        self.REGISTRATION = '&registration=TRUE'
        self.REFUSED = '&refused=FALSE'
        self.EXPRIATION = '&expiration=FALSE'
        self.WITHDRAWAL = '&withdrawal=FALSE'
        self.PUBLICATION = '&publication=TRUE'
        self.CANCEL = '&cancel=FALSE'
        self.ABANDONMENT = '&abandonment=FALSE'
        self.TRADEMARK = '&trademark=TRUE'
        self.SERVICEMARK = '&serviceMark=FALSE'
        self.TRADEMARKSERVICEMARK = '&trademarkServiceMark=FALSE'
        self.BUSINESSEMBLEM = '&businessEmblem=FALSE'
        self.COLLECTIVEMARK = '&collectiveMark=FALSE'
        self.INTERNATIONALMARK = '&internationalMark=FALSE'
        self.CHARACTER = '&character=FALSE'
        self.FIGURE = '&figure=TRUE&figureComposition=TRUE'
        self.COMPOSITIONCHARACTER = '&compositionCharacter=FALSE'
        self.NUMOFROWS = '&numOfRows=500'
        self.PAGENUM = '&pageNo=1'
        self.APPLICATIONDATE = f'&applicationDate={self.START_DATE}~{self.END_DATE}'
        self.SERVICEKEY = api_key
        self.URL = f'{self.BASIC_URL}{self.APPLICATION}{self.REGISTRATION}{self.REFUSED}{self.EXPRIATION}{self.WITHDRAWAL}{self.PUBLICATION}{self.CANCEL}{self.ABANDONMENT}{self.TRADEMARK}{self.SERVICEMARK}' \
                   f'{self.TRADEMARKSERVICEMARK}{self.BUSINESSEMBLEM}{self.COLLECTIVEMARK}{self.INTERNATIONALMARK}{self.CHARACTER}{self.FIGURE}{self.COMPOSITIONCHARACTER}{self.NUMOFROWS}' \
                   f'{self.PAGENUM}{self.APPLICATIONDATE}{self.SERVICEKEY}'

    def updateURL(self, idx):
        self.URL = f'{self.BASIC_URL}{self.APPLICATION}{self.REGISTRATION}{self.REFUSED}{self.EXPRIATION}{self.WITHDRAWAL}{self.PUBLICATION}{self.CANCEL}{self.ABANDONMENT}{self.TRADEMARK}{self.SERVICEMARK}' \
                   f'{self.TRADEMARKSERVICEMARK}{self.BUSINESSEMBLEM}{self.COLLECTIVEMARK}{self.INTERNATIONALMARK}{self.CHARACTER}{self.FIGURE}{self.COMPOSITIONCHARACTER}{self.NUMOFROWS}' \
                   f'&pageNo={str(idx)}{self.APPLICATIONDATE}{self.SERVICEKEY}'
        return self.URL

class PARSE_API():
    def __init__(self, url, database, table_name):
        self.URL = url
        self.APP_NUM_LIST = []
        self.VIENNA_CODE_LIST = []
        self.SAVE_FOLDER = 'E:/data/viennacode/'
        self.IMG_SAVE_FOLDER = 'E:/data/viennacode_img_19600101_20191231/'
        os.makedirs(self.SAVE_FOLDER, exist_ok=True)
        os.makedirs(self.IMG_SAVE_FOLDER, exist_ok=True)
        self.DB = database
        self.TABLE_NAME = table_name

    def Parsing(self):
        try:
            k_tree = ET.parse(urllib.request.urlopen(self.URL))
        except urllib.error.URLError as e:
            print(e)
            print('urllib.error.URLError. Plz wait for 1 minutes')
            sleep(10)
            k_tree = ET.parse(urllib.request.urlopen(self.URL))
            pass
        except urllib.error.HTTPError as e:
            print(e)
            print('urllib.error.HTTPError. Plz wait for 1 minutes')
            sleep(10)
            k_tree = ET.parse(urllib.request.urlopen(self.URL))
            pass
        except http.client.HTTPException as e:
            print(e)
            print('http.client.HTTPException. Plz wait for 1 minute')
            sleep(10)
            k_tree = ET.parse(urllib.request.urlopen(self.URL))
            pass
        except requests.exceptions.ConnectionError as e:
            print(e)
            print('requests.exceptions.ConnectionError. Plz wait for 1 minute')
            sleep(10)
            k_tree = ET.parse(urllib.request.urlopen(self.URL))
            pass
        except TimeoutError as e:
            print(e)
            print('TimeoutError. Plz wait for 1 minute')
            sleep(10)
            k_tree = ET.parse(urllib.request.urlopen(self.URL))
            pass
        except urllib3.exceptions.NewConnectionError as e:
            print(e)
            print('urllib3.exceptions.NewConnectionError. Plz wait for 1 minute')
            sleep(10)
            k_tree = ET.parse(urllib.request.urlopen(self.URL))
            pass
        except urllib3.exceptions.MaxRetryError as e:
            print(e)
            print('urllib3.exceptions.MaxRetryError. Plz wait for 1 minute')
            sleep(10)
            k_tree = ET.parse(urllib.request.urlopen(self.URL))
            pass
        k_root = k_tree.getroot()

        return k_root

    def MakeCsvPath(self,start_date, end_date, page_num):
        save_path = f'{self.SAVE_FOLDER}/{start_date}_{end_date}_{page_num}_.figure_composition.csv'
        return save_path

    def MakeImgPath(self, app_num):
        save_path = os.path.join(self.IMG_SAVE_FOLDER, f'{app_num}.jpg')
        return save_path

    def DownloadImg(self, img_url, app_num):
        save_path = self.MakeImgPath(app_num)
        if not os.path.exists(save_path):
            try:
                img = requests.get(img_url).content
                with open(save_path, 'wb') as downloader:
                    downloader.write(img)
            except urllib.error.URLError as e:
                print('urllib.error.URLError')
                sleep(10)
                img = requests.get(img_url).content
                with open(save_path, 'wb') as downloader:
                    downloader.write(img)
                    sleep(0.2)
                pass
            except urllib.error.HTTPError as e:
                print('urllib.error.HTTPError')
                sleep(10)
                img = requests.get(img_url).content
                with open(save_path, 'wb') as downloader:
                    downloader.write(img)
                    sleep(0.2)
                pass
            except http.client.HTTPException as e:
                print('http.client.HTTPException')
                sleep(10)
                img = requests.get(img_url).content
                with open(save_path, 'wb') as downloader:
                    downloader.write(img)
                    sleep(0.2)
                pass
            except requests.exceptions.ConnectionError as e:
                print('requests.exceptions.ConnectionError')
                sleep(10)
                img = requests.get(img_url).content
                with open(save_path, 'wb') as downloader:
                    downloader.write(img)
                    sleep(0.2)
                pass
            except TimeoutError as e:
                print('TimeoutError')
                sleep(10)
                img = requests.get(img_url).content
                with open(save_path, 'wb') as downloader:
                    downloader.write(img)
                    sleep(0.2)
                pass
            except urllib3.exceptions.NewConnectionError as e:
                print('urllib3.exceptions.NewConnectionError')
                sleep(10)
                img = requests.get(img_url).content
                with open(save_path, 'wb') as downloader:
                    downloader.write(img)
                    sleep(0.2)
                pass
            except urllib3.exceptions.MaxRetryError as e:
                print('urllib3.exceptions.MaxRetryError')
                sleep(10)
                img = requests.get(img_url).content
                with open(save_path, 'wb') as downloader:
                    downloader.write(img)
                    sleep(0.2)
                pass
            except requests.exceptions.MissingSchema as e:
                print('URL is invaild!')
                pass
            except http.client.IncompleteRead as e:
                print('http.client.IncompleteRead')
                pass
            except urllib3.exceptions.ProtocolError as e:
                print('urllib3.exceptions.ProtocolError')
                pass
            except requests.exceptions.ChunkedEncodingError as e:
                print('requests.exceptions.ChunkedEncodingError')
                pass

    def saveAppVienna(self):
        k_root = self.Parsing()
        for child_1 in k_root.iter('item'):
            for child_2 in child_1.getchildren():
                if child_2.tag == 'applicationNumber':
                    app_num = child_2.text
                    self.APP_NUM_LIST.append(app_num)
                elif child_2.tag=='bigDrawing':
                    img_url = child_2.text
                    self.DownloadImg(img_url, app_num)
                elif child_2.tag == 'viennaCode':
                    if child_2.text is None:
                        self.VIENNA_CODE_LIST.append('None')
                    else:
                        self.VIENNA_CODE_LIST.append(child_2.text)
        df = pd.DataFrame({'APP_NUM':self.APP_NUM_LIST, 'VIENNA_CODE':self.VIENNA_CODE_LIST}, columns=['APP_NUM', 'VIENNA_CODE'])
        self.DB.appendDataFrameToTable(df=df, table_name=self.TABLE_NAME)

    def GetPageNum(self):
        k_root = self.Parsing()
        for child in k_root.find('count'):
            if child.tag == 'totalCount':
                count_num = child.text
                if int(count_num) > 500:
                    print(f'total number of images:{count_num}')
                    page_num = int((int(count_num) / 500) + 2)
                    return page_num
                else:
                    return 1



