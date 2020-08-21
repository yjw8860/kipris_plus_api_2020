from img_vienna import *
from api_key import API_KEY #API_KEY는 문자열입니다. kipris plus(http://plus.kipris.or.kr/)에서 발급받을 수 있습니다.
from db_attribute import user, password, host, database_name

VIENNA_TABLE_NAME = 'vienna_small_category'
VIENNA_TABLE_SQL = f'CREATE TABLE {VIENNA_TABLE_NAME} (APP_NUM VARCHAR(13), VIENNA_CODE VARCHAR(100));'
DATE_PAGE_TABLE_NAME = "date_page_num"
DATA_PAGE_TABLE_SQL = f'CREATE TABLE {DATE_PAGE_TABLE_NAME} (DATE VARCHAR(17), CURRENT_PAGE SMALLINT(1), TOTAL_PAGE SMALLINT(1));'
DB = DataBase(user=user, password=password, host=host, database_name=database_name)

if not VIENNA_TABLE_NAME in DB.TABLELIST:
    DB.executeSQL(VIENNA_TABLE_SQL)

def getLastYearAndMonth():
    sql = 'select * from date_page_num;'
    result = DB.conn.execute(sql)
    row = result.fetchall()
    for i, data in enumerate(row):
        if i + 1 == len(row):
            LAST_YEAR = int(data[0][0:4])
            LAST_MONTH = int(data[0][4:6])
            LAST_PAGE = data[1] + 1
            TOTAL_PAGE = data[2]
            return LAST_YEAR, LAST_MONTH, LAST_PAGE, TOTAL_PAGE

def downloadImgAndVienna(download, idx, page_num):
    print(f'Connecting {idx}/{page_num - 1}')
    url = download.updateURL(idx)
    parsing = PARSE_API(url=url, database=DB, table_name=VIENNA_TABLE_NAME)
    current_state = (f'{download.START_DATE}~{download.END_DATE}', idx, page_num)
    parsing.saveAppVienna()
    DB.appendDataToTable(data=current_state, table_name=DATE_PAGE_TABLE_NAME)
    print(current_state)
    sleep(1)


def saveImgAndVienna(start_date, end_date):
    download = DOWNLOAD(API_KEY, start_date, end_date)
    url = download.URL
    parsing = PARSE_API(url=url, database=DB, table_name=VIENNA_TABLE_NAME)
    page_num = parsing.GetPageNum()
    print(f'The number of page: {page_num - 1}')
    sleep(1)
    if page_num == 1:
        downloadImgAndVienna(download, 1, page_num)
    else:
        for idx in range(1, page_num):
            downloadImgAndVienna(download, idx, page_num)


def saveFromLastMonth(last_year, last_month, last_page, total_page):
    print(f'last year:{last_year}, last_month:{last_month}, last_page:{last_page}, total_page:{total_page}')
    dateclass = MAKEDATE(DB, last_year, last_month)
    start_date_list = dateclass.start_date_list
    end_date_list = dateclass.end_date_list
    i = 0
    for start_date, end_date in tqdm(zip(start_date_list, end_date_list)):
        download = DOWNLOAD(API_KEY, start_date, end_date)
        if i == 0:
            for idx in range(last_page, total_page):
                downloadImgAndVienna(download, idx, total_page)
            i +=1
        else:
            saveImgAndVienna(start_date, end_date)


def saveFromLastYear(last_year, last_month):
    print(f'START FROM NEW YEAR! last year:{last_year}, last_month:{last_month}')
    for year in range(last_year, 1959, -1):
        dateclass = MAKEDATE(DB, year, last_month)
        start_date_list = dateclass.start_date_list
        end_date_list = dateclass.end_date_list
        for start_date, end_date in tqdm(zip(start_date_list, end_date_list)):
            saveImgAndVienna(start_date, end_date)

if __name__ == '__main__':
    last_year = 2019
    last_month = 1
    last_page = 1
    if not DATE_PAGE_TABLE_NAME in DB.TABLELIST:
        DB.executeSQL(DATA_PAGE_TABLE_SQL)
        saveFromLastYear(last_year, last_month)
    else:
        last_year, last_month, last_page, total_page = getLastYearAndMonth()
        if last_page != total_page:
            saveFromLastMonth(last_year, last_month, last_page, total_page)
            last_year = last_year - 1
            last_month = 1
            saveFromLastYear(last_year, last_month)
        else:
            last_year = last_year - 1
            last_month = 1
            saveFromLastYear(last_year, last_month)

