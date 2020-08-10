from img_vienna import *
from api_key import API_KEY #API_KEY는 문자열입니다. kipris plus(http://plus.kipris.or.kr/)에서 발급받을 수 있습니다.

if __name__ == '__main__':
    dateclass = MAKEDATE()
    start_date_list = dateclass.start_date_list
    end_date_list = dateclass.end_date_list

    for start_date, end_date in tqdm(zip(start_date_list, end_date_list)):
        download = DOWNLOAD(API_KEY, start_date, end_date)
        url_list = download.URL_LIST

        for url in url_list:
            parsing = PARSE_API(url)
            page_num = parsing.GetPageNum()
            print(f'Connecting 1/{page_num}')
            """DOWNLOAD WITH FIRST PAGE"""
            parsing.saveAppVienna(start_date, end_date, str(1))
            sleep(1)
            """DOWNLOAD FROM SECOND PAGE"""
            if page_num>0:
                for idx in range(2, page_num, 1):
                    print(f'Connecting {idx}/{page_num}')
                    url = re.sub(f'&pageNo={str(idx-1)}', f'&pageNo={str(idx)}',url)
                    parsing = PARSE_API(url)
                    parsing.saveAppVienna(start_date, end_date, str(idx))
                    sleep(1)