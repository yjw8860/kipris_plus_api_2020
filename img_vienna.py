import os, requests, re
import xml.etree.ElementTree as ET
import urllib.request
from time import sleep
import http
import urllib3
from tqdm import tqdm
import pandas as pd

#다운로드(19600101 ~ 20200624)

class MAKEDATE:
    def __init__(self):
        self.YEAR_LIST = list(range(2019,1959,-1))
        self.YEAR_LIST = [str(year) for year in self.YEAR_LIST]
        self.MONTH_LIST = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
        self.DATE_LIST = ['31', '29', '31', '30', '31', '30', '31', '31', '30', '31', '30', '31']
        self.start_date_list = []
        self.end_date_list = []
        for year in self.YEAR_LIST:
            for month, date in zip(self.MONTH_LIST, self.DATE_LIST):
                start_date = f'{year}{month}01'
                end_date = f'{year}{month}{date}'
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
        self.FIGURE_TRUE = '&figure=TRUE&figureComposition=FASLE'
        self.FIGURE_FALSE = '&figure=FALSE&figureComposition=TRUE'
        self.COMPOSITIONCHARACTER = '&compositionCharacter=FALSE'
        self.NUMOFROWS = '&numOfRows=500'
        self.PAGENUM = '&pageNo=1'
        self.APPLICATIONDATE = f'&applicationDate={self.START_DATE}~{self.END_DATE}'
        self.SERVICEKEY = api_key
        self.FIGURE_URL = f'{self.BASIC_URL}{self.APPLICATION}{self.REGISTRATION}{self.REFUSED}{self.EXPRIATION}{self.WITHDRAWAL}{self.PUBLICATION}{self.CANCEL}{self.ABANDONMENT}{self.TRADEMARK}{self.SERVICEMARK}' \
                     f'{self.TRADEMARKSERVICEMARK}{self.BUSINESSEMBLEM}{self.COLLECTIVEMARK}{self.INTERNATIONALMARK}{self.CHARACTER}{self.FIGURE_TRUE}{self.COMPOSITIONCHARACTER}{self.NUMOFROWS}' \
                     f'{self.PAGENUM}{self.APPLICATIONDATE}{self.SERVICEKEY}'
        self.FIGURE_COMPOSITION_URL = re.sub(self.FIGURE_TRUE, self.FIGURE_FALSE, self.FIGURE_URL)
        self.URL_LIST = [self.FIGURE_URL, self.FIGURE_COMPOSITION_URL]

class PARSE_API():
    def __init__(self, url):
        self.URL = url
        self.APP_NUM_LIST = []
        self.VIENNA_CODE_LIST = []

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

    def MakeSavePath(self,start_date, end_date, page_num):
        SAVE_FOLDER = 'E:/data/viennacode/'
        os.makedirs(SAVE_FOLDER, exist_ok=True)
        if 'figureComposition=FASLE' in self.URL:
            save_path = f'{SAVE_FOLDER}/{start_date}_{end_date}_{page_num}_figure_composition.csv'
        else:
            save_path = f'{SAVE_FOLDER}/{start_date}_{end_date}_{page_num}_.figure.csv'

        return save_path

    def saveAppVienna(self, start_date, end_date, page_num):
        k_root = self.Parsing()
        for child_1 in tqdm(k_root.iter('item')):
            for child_2 in child_1.getchildren():
                if child_2.tag == 'applicationNumber':
                    self.APP_NUM_LIST.append(child_2.text)
                elif child_2.tag == 'viennaCode':
                    if child_2.text is None:
                        self.VIENNA_CODE_LIST.append('None')
                    else:
                        self.VIENNA_CODE_LIST.append(child_2.text)
        df = pd.DataFrame({'APP_NUM':self.APP_NUM_LIST, 'VIENNA_CODE':self.VIENNA_CODE_LIST}, columns=['APP_NUM', 'VIENNA_CODE'])
        save_path = self.MakeSavePath(start_date, end_date, page_num)
        df.to_csv(save_path, index=False, encoding='euc-kr')


    def GetPageNum(self):
        page_num = 1
        k_root = self.Parsing()
        for child in k_root.find('count'):
            if child.tag == 'totalCount':
                count_num = child.text
                if int(count_num) > 500:
                    print(f'total number of images:{count_num}')
                    page_num = int((int(count_num) / 500) + 2)
        return page_num
