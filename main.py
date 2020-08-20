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
if not DATE_PAGE_TABLE_NAME in DB.TABLELIST:
    DB.executeSQL(DATA_PAGE_TABLE_SQL)

if __name__ == '__main__':
    dateclass = MAKEDATE()
    start_date_list = dateclass.start_date_list
    end_date_list = dateclass.end_date_list

    for start_date, end_date in tqdm(zip(start_date_list, end_date_list)):
        download = DOWNLOAD(API_KEY, start_date, end_date)
        url = download.URL
        parsing = PARSE_API(url=url, database=DB, table_name=VIENNA_TABLE_NAME)
        page_num = parsing.GetPageNum()
        print(f'The number of page: {page_num-1}')
        sleep(1)
        for idx in range(1, page_num):
            print(f'Connecting {idx}/{page_num-1}')
            url = download.updateURL(idx)
            parsing = PARSE_API(url=url, database=DB, table_name=VIENNA_TABLE_NAME)
            current_state = (f'{download.START_DATE}~{download.END_DATE}', idx, page_num)
            parsing.saveAppVienna()
            DB.appendDataToTable(data=current_state, table_name=DATE_PAGE_TABLE_NAME)
            print(current_state)
            sleep(1)