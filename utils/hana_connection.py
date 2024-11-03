import os
# from dotenv import load_dotenv
from hdbcli import dbapi

HANA_ADDRESS = "172.20.3.66"
HANA_PORT = "36615"
HANA_USER = "AP_AIS"
HANA_PASSWORD = "Hanaais123"

def Connect_HANA():
    # # 加载 .env 文件
    # load_dotenv()
    # # Access the Data_Hana section
    # address = os.getenv('HANA_ADDRESS')
    # port = os.getenv('HANA_PORT')
    # user = os.getenv('HANA_USER')
    # password = os.getenv('HANA_PASSWORD')
    extract_connection = dbapi.connect(address=HANA_ADDRESS, 
                                        port=HANA_PORT,
                                        user=HANA_USER,
                                        password=HANA_PASSWORD)
    return extract_connection

def Upload_Test_Environment(Commit_data):

    values = list(Commit_data.head(100).itertuples(index=False, name=None))
    # insert用 select * from "ZADDON"."ZTHA_AIFCST"
    #測試機
    insert_connection = dbapi.connect(address="172.20.3.81", 
                                    port=36315,
                                    user="AP_AIS",
                                    password="Apais123")
    insert_cursor = insert_connection.cursor()

    for value in values:
        insert = "INSERT INTO ZADDON.ZTHA_AIFCST VALUES {}".format(value)
        insert_cursor.execute(insert)
        # print(insert)
    insert_connection.commit()
    insert_cursor.close()
    insert_connection.close()
    print("Finish 測試資料100筆上傳測試機")

def Upload_Deployment_Environment(Commit_data):

    #轉成list of tuple方便放入hanadb
    values = list(Commit_data.itertuples(index=False, name=None))
    # insert用 select * from "ZADDON"."ZTHA_PROJECTORD"
    #正式機
    insert_connection = Connect_HANA()
    insert_cursor = insert_connection.cursor()
    for value in values:
        insert = "INSERT INTO ZADDON.ZTHA_AIFCST VALUES {}".format(value)
        insert_cursor.execute(insert)
    insert_connection.commit()

    insert_cursor.close()
    insert_connection.close()

    print("Finish 上傳正式機")