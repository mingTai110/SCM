from utils.hana_connection import Connect_HANA
from dateutil.relativedelta import relativedelta
import pandas as pd 
import datetime


extract_connection= Connect_HANA()
# 條件篩選邏輯 
extra_parts = ('9892431F00E','989243PF00E','989243PF10E','989243PF20E','9892F10F00E','9892P3PF00E','9892P3PF10E')
implement_pgs = ('ISG','IAG','ICWG','ESG','ECG', 'IPSG','IDSG')

# 今天日期
today_date = datetime.datetime.today()
# 上六個月的日期
previous_month = today_date + relativedelta(months=-6)
previous_month=previous_month.strftime('%Y/%m/01')

def Get_Openso():
    sql_schema_open_so =f"""SELECT  * FROM "ZADDON"."ZTHA_HISTSO"
    WHERE Part_No IN 
    ( 
    SELECT Distinct MATNR FROM "_SYS_BIC"."APP_AIS/ZCV_APP_AIS_MATERIAL_MASTER" AS MARA
    WHERE PRDPG IN {implement_pgs}
    AND (HQABC LIKE 'A%' OR HQABC LIKE 'B%' )
    AND ZZPLM_STAT IN ('A','H','N','V') 
    AND MTART IN ('ZFIN','ZOEM')
    OR AIFLAG = 'Y'
    OR ((PRDPD IN ('Industrial Storage')) AND (HQABC LIKE 'A%' OR HQABC LIKE 'B%' ) AND (ZZPLM_STAT IN ('A','H','N','V')) AND (MTART IN ('ZFIN','ZOEM','ZPER')) )
    OR (MATNR IN {extra_parts})
    )
    AND VERSION >= \'{previous_month}\'
    """

    open_so_ori=pd.read_sql(sql_schema_open_so, extract_connection)
    return open_so_ori

def Get_Openso_Current():
    sql_schema_open_so_c =f"""SELECT * FROM "_SYS_BIC"."APP_AIS/ZCV_APP_AIS_OPEN_SO"
    WHERE Part_No IN
    ( 
    SELECT Distinct MATNR FROM "_SYS_BIC"."APP_AIS/ZCV_APP_AIS_MATERIAL_MASTER" AS MARA
    WHERE PRDPG IN {implement_pgs}
    AND (HQABC LIKE 'A%' OR HQABC LIKE 'B%' )
    AND ZZPLM_STAT IN ('A','H','N','V') 
    AND MTART IN ('ZFIN','ZOEM')
    OR AIFLAG = 'Y'
    OR ((PRDPD IN ('Industrial Storage')) AND (HQABC LIKE 'A%' OR HQABC LIKE 'B%' ) AND (ZZPLM_STAT IN ('A','H','N','V')) AND (MTART IN ('ZFIN','ZOEM','ZPER')) )
    OR (MATNR IN {extra_parts})
    )
    """

    open_so_c_ori=pd.read_sql(sql_schema_open_so_c, extract_connection)
    return open_so_c_ori

def Get_Opendata():
    Openso= Get_Openso()
    Openso_C=Get_Openso_Current()
    Openso= Openso[Openso['VERSION']!=today_date.strftime('%Y/%m/01')]
    Openso_C=Openso_C[['VERSION','COMPANY_ID','PART_NO','SHIP_PLANT','YM','OPEN_QTY']]
    Open_so_final=pd.concat([Openso, Openso_C], ignore_index=True)
    return Open_so_final