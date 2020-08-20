from db_attribute import user,password,host,database_name
from img_vienna import MAKEDATE, DataBase
DB = DataBase(user=user, password=password, host=host, database_name=database_name)
test = MAKEDATE(DB, 2000, 1)
print(test.start_date_list)
print(test.end_date_list)




