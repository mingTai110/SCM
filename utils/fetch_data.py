import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
from utils.hana_connection import Connect_HANA

def select_aiflag(group):
    if 'Y' in group['AIFLAG'].values:
        return group[group['AIFLAG'] == 'Y'].iloc[0]
    else:
        selected_row = group.iloc[0]
        if pd.isna(selected_row['AIFLAG']):
            selected_row['AIFLAG'] = 'N'
        return selected_row
    
class DataExtractor:
    def __init__(self):
        self.extract_connection = Connect_HANA()
        self.extra_parts = ('9892431F00E', '989243PF00E', '989243PF10E', '989243PF20E', '9892F10F00E', '9892P3PF00E', '9892P3PF10E')
        self.implement_pgs = ('ISG', 'IAG', 'ICWG', 'ESG', 'ECG', 'IPSG', 'IDSG')
        self.iiot_pgs=('ISG', 'IAG', 'ICWG', 'ESG')
        self.eiot_pgs=('ECG', 'IPSG', 'IDSG')
        self.today_date = datetime.datetime.today()

    def get_product_info(self):
        sql_schema_product = f"""
        SELECT DISTINCT MATNR AS PART_NO, PRDPG AS PG, PRDPD AS PD, PRPDL AS PDL, ZZPLM_STAT AS PLM_STATUS, HQABC AS ABC_INDICATOR, BISMT AS MODEL, AIFLAG
        FROM _SYS_BIC."APP_AIS/ZCV_APP_AIS_MATERIAL_MASTER"
        WHERE PRDPG IN {self.implement_pgs}
        AND (HQABC LIKE 'A%' OR HQABC LIKE 'B%' OR HQABC LIKE 'C%')
        AND ZZPLM_STAT IN ('A','H','N','V') 
        AND MTART IN ('ZFIN','ZOEM')
        OR AIFLAG = 'Y'
        OR (PRDPD IN ('Industrial Storage') AND (HQABC LIKE 'A%' OR HQABC LIKE 'B%' OR HQABC LIKE 'C%') AND (ZZPLM_STAT IN ('A','H','N','V')) AND (MTART IN ('ZFIN','ZOEM','ZPER')))
        OR (MATNR IN {self.extra_parts})
        """
        product_ori = pd.read_sql(sql_schema_product, self.extract_connection)
        product_info = product_ori.groupby('PART_NO').apply(select_aiflag).reset_index(drop=True)
        return product_info

    def get_openso(self, previous_month):
        sql_schema_open_so = f"""
        SELECT * FROM "ZADDON"."ZTHA_HISTSO"
        WHERE Part_No IN 
        ( 
        SELECT Distinct MATNR FROM "_SYS_BIC"."APP_AIS/ZCV_APP_AIS_MATERIAL_MASTER" AS MARA
        WHERE PRDPG IN {self.iiot_pgs}
        AND (HQABC LIKE 'A%' OR HQABC LIKE 'B%' OR HQABC LIKE 'C%')
        AND ZZPLM_STAT IN ('A','H','N','V') 
        AND MTART IN ('ZFIN','ZOEM')
        OR AIFLAG = 'Y'
        OR ((PRDPD IN ('Industrial Storage')) AND (HQABC LIKE 'A%' OR HQABC LIKE 'B%' OR HQABC LIKE 'C%) AND (ZZPLM_STAT IN ('A','H','N','V')) AND (MTART IN ('ZFIN','ZOEM','ZPER')))
        OR (MATNR IN {self.extra_parts})
        )
        AND VERSION >= '{previous_month}'
        """
        open_so_ori = pd.read_sql(sql_schema_open_so, self.extract_connection)
        return open_so_ori

    def get_openso_current(self):
        sql_schema_open_so_c = f"""
        SELECT * FROM "_SYS_BIC"."APP_AIS/ZCV_APP_AIS_OPEN_SO"
        WHERE Part_No IN
        ( 
        SELECT Distinct MATNR FROM "_SYS_BIC"."APP_AIS/ZCV_APP_AIS_MATERIAL_MASTER" AS MARA
        WHERE PRDPG IN {self.iiot_pgs}
        AND (HQABC LIKE 'A%' OR HQABC LIKE 'B%'OR HQABC LIKE 'C%')
        AND ZZPLM_STAT IN ('A','H','N','V') 
        AND MTART IN ('ZFIN','ZOEM')
        OR AIFLAG = 'Y'
        OR ((PRDPD IN ('Industrial Storage')) AND (HQABC LIKE 'A%' OR HQABC LIKE 'B%' OR HQABC LIKE 'C%) AND (ZZPLM_STAT IN ('A','H','N','V')) AND (MTART IN ('ZFIN','ZOEM','ZPER')))
        OR (MATNR IN {self.extra_parts})
        )
        """
        open_so_c_ori = pd.read_sql(sql_schema_open_so_c, self.extract_connection)
        return open_so_c_ori

    def get_openwo(self, previous_year):
        sql_schema_open_wo = f"""
        SELECT * FROM "ZADDON"."ZTHA_HISTWO"
        WHERE Part_No IN 
        ( 
        SELECT Distinct MATNR FROM "_SYS_BIC"."APP_AIS/ZCV_APP_AIS_MATERIAL_MASTER" AS MARA
        WHERE PRDPG IN {self.iiot_pgs}
        AND (HQABC LIKE 'A%' OR HQABC LIKE 'B%' OR HQABC LIKE 'C%')
        AND ZZPLM_STAT IN ('A','H','N','V') 
        AND MTART IN ('ZFIN','ZOEM')
        OR AIFLAG = 'Y'
        OR ((PRDPD IN ('Industrial Storage')) AND (HQABC LIKE 'A%' OR HQABC LIKE 'B%' OR HQABC LIKE 'C%) AND (ZZPLM_STAT IN ('A','H','N','V')) AND (MTART IN ('ZFIN','ZOEM','ZPER')))
        OR (MATNR IN {self.extra_parts})
        )
        AND VERSION >= '{previous_year}'
        """
        open_wo_ori = pd.read_sql(sql_schema_open_wo, self.extract_connection)
        return open_wo_ori

    def get_openwo_current(self):
        sql_schema_open_wo_c = f"""
        SELECT * FROM "_SYS_BIC"."APP_AIS/ZCV_APP_AIS_OPEN_WO"
        WHERE Part_No IN 
        ( 
        SELECT Distinct MATNR FROM "_SYS_BIC"."APP_AIS/ZCV_APP_AIS_MATERIAL_MASTER" AS MARA
        WHERE PRDPG IN {self.iiot_pgs}
        AND (HQABC LIKE 'A%' OR HQABC LIKE 'B%' OR HQABC LIKE 'C%')
        AND ZZPLM_STAT IN ('A','H','N','V') 
        AND MTART IN ('ZFIN','ZOEM')
        OR AIFLAG = 'Y'
        OR ((PRDPD IN ('Industrial Storage')) AND (HQABC LIKE 'A%' OR HQABC LIKE 'B%' OR HQABC LIKE 'C%') AND (ZZPLM_STAT IN ('A','H','N','V')) AND (MTART IN ('ZFIN','ZOEM','ZPER')))
        OR (MATNR IN {self.extra_parts})
        )
        """
        open_wo_c_ori = pd.read_sql(sql_schema_open_wo_c, self.extract_connection)
        return open_wo_c_ori

    def get_opendata_so_hana(self, fetch_month=-6):
        previous_month = self.today_date + relativedelta(months=fetch_month)
        previous_month = previous_month.strftime('%Y/%m/01')

        openso = self.get_openso(previous_month)
        openso_c = self.get_openso_current()
        openso = openso[openso['VERSION'] != self.today_date.strftime('%Y/%m/01')]
        openso_c = openso_c[['VERSION', 'COMPANY_ID', 'PART_NO', 'SHIP_PLANT', 'YM', 'OPEN_QTY']]
        open_so_final = pd.concat([openso, openso_c], ignore_index=True)
        return open_so_final

    def get_opendata_wo_hana(self, fetch_month=-6):
        previous_month = self.today_date + relativedelta(months=fetch_month)
        previous_month = previous_month.strftime('%Y/%m/01')

        openwo = self.get_openwo(previous_month)
        openwo_c = self.get_openwo_current()
        openwo = openwo[openwo['VERSION'] != self.today_date.strftime('%Y/%m/01')]
        openwo_c = openwo_c[['VERSION', 'PART_NO', 'PLANT', 'YM', 'OPEN_QTY']]
        df = pd.concat([openwo, openwo_c], ignore_index=True)
        return df

    def get_prediction_data(self):
        sql_schema_fcst = "SELECT * FROM ZADDON.ZTHA_AIFCST"
        fcst_ori = pd.read_sql(sql_schema_fcst, self.extract_connection)
        return fcst_ori

    def get_so_data(self, previous_year):
        sql_schema_so = f"""
        SELECT * FROM _SYS_BIC."APP_AIS/ZCV_APP_AIS_SO_INFO_BY_REQUIREDT"
        WHERE Part_No IN 
        ( 
        SELECT Distinct MATNR FROM "_SYS_BIC"."APP_AIS/ZCV_APP_AIS_MATERIAL_MASTER" AS MARA
        WHERE PRDPG IN {self.iiot_pgs}
        AND (HQABC LIKE 'A%' OR HQABC LIKE 'B%' OR HQABC LIKE 'C%')
        AND ZZPLM_STAT IN ('A','H','N','V') 
        AND MTART IN ('ZFIN','ZOEM')
        OR AIFLAG = 'Y'
        OR (PRDPD IN ('Industrial Storage') AND (HQABC LIKE 'A%' OR HQABC LIKE 'B%' OR HQABC LIKE 'C%') AND (ZZPLM_STAT IN ('A','H','N','V')) AND (MTART IN ('ZFIN','ZOEM','ZPER')))
        OR (MATNR IN {self.extra_parts})
        )
        AND REQUIRED_DATE >= '{previous_year}'
        AND AUART <> 'ZOR9'
        """
        so_ori = pd.read_sql(sql_schema_so, self.extract_connection)
        return so_ori
    def get_so_all_columns(self):
        sql_schema_so = """
        SELECT TOP 5 * FROM _SYS_BIC."APP_AIS/ZCV_APP_AIS_SO_INFO_BY_REQUIREDT" 
        """
        so_ori = pd.read_sql(sql_schema_so, self.extract_connection)
        return so_ori
    

    def get_wo_data(self, previous_year):
        sql_schema_wo = f"""
        SELECT * FROM "_SYS_BIC"."APP_AIS/ZCV_APP_AIS_WO_CONSUMPTION"
        WHERE PART_NO IN 
        ( 
        SELECT Distinct MATNR FROM "_SYS_BIC"."APP_AIS/ZCV_APP_AIS_MATERIAL_MASTER" AS MARA
        WHERE PRDPG IN {self.iiot_pgs}
        AND (HQABC LIKE 'A%' OR HQABC LIKE 'B%' OR HQABC LIKE 'C%')
        AND ZZPLM_STAT IN ('A','H','N','V') 
        AND MTART IN ('ZFIN','ZOEM')
        OR AIFLAG = 'Y'
        OR ((PRDPD IN ('Industrial Storage')) AND (HQABC LIKE 'A%' OR HQABC LIKE 'B%' OR HQABC LIKE 'C%') AND (ZZPLM_STAT IN ('A','H','N','V')) AND (MTART IN ('ZFIN','ZOEM','ZPER')))
        OR (MATNR IN {self.extra_parts})
        )
        AND (AUART='PP01' OR AUART='') 
        AND SUBSTRING(BERID1,5,5) <> '-CUST' AND SUBSTRING(BERID,5,5) <> '-CUST'
        AND START_DATE >= '{previous_year}'
        """
        wo_ori = pd.read_sql(sql_schema_wo, self.extract_connection)
        return wo_ori

    def get_shipment(self, data_interval_yyyymm):
        sql_schema_shipment = f"""
        SELECT shipment.*
        FROM (
            SELECT MATNR, WERKS
            FROM "_SYS_BIC"."APP_SCM/ZCV_APP_SCM_S_OP_P"
            WHERE MSU='MSU'
            AND MATNR IN 
            ( 
            SELECT Distinct MATNR FROM "_SYS_BIC"."APP_AIS/ZCV_APP_AIS_MATERIAL_MASTER" AS MARA
            WHERE PRDPG IN {self.eiot_pgs}
            AND 
            (
                (HQABC LIKE 'A%' OR HQABC LIKE 'B%' OR HQABC LIKE 'C%')
                AND ZZPLM_STAT IN ('A','H','N','V') 
                AND MTART IN ('ZFIN','ZOEM')
            )
            OR 
            (
                AIFLAG = 'Y'
                OR ( PRDPD IN ('Industrial Storage') AND (HQABC LIKE 'A%' OR HQABC LIKE 'B%' OR HQABC LIKE 'C%') AND (ZZPLM_STAT IN ('A','H','N','V')) AND (MTART IN ('ZFIN','ZOEM','ZPER')))
                OR (MATNR IN {self.extra_parts})
            )
            )
        ) AS sop
        INNER JOIN (
            SELECT *
            FROM "_SYS_BIC"."APP_SCM/ZCV_APP_SCM_SHIPMENT"
            WHERE ORDERTYPE IN ('SO', 'STO', 'WO', 'INTERPLANT','EXPENSE')
            AND SHIP_MONTH >= '{data_interval_yyyymm}'
            AND WERKS IN ('TWH1','CKH1','CKH2')
        ) AS shipment ON sop.MATNR = shipment.MATNR AND sop.WERKS = shipment.WERKS
        """
        shipment_ori = pd.read_sql(sql_schema_shipment, self.extract_connection)
        return shipment_ori

    def get_backlog_c(self):
        sql_schema_backlog = f"""
        SELECT MATNR, WERKS,
        SO_STO_WO_DEMAND_ONE, SO_STO_WO_DEMAND_TWO, SO_STO_WO_DEMAND_THREE, SO_STO_WO_DEMAND_FOUR, SO_STO_WO_DEMAND_FIVE, SO_STO_WO_DEMAND_SIX, SO_STO_WO_DEMAND_SEVEN, SO_STO_WO_DEMAND_EIGHT, SO_STO_WO_DEMAND_NINE, SO_STO_WO_DEMAND_TEN, SO_STO_WO_DEMAND_ELEVEN, SO_STO_WO_DEMAND_TWELVE,
        RESERVATION_QTY_ONE, RESERVATION_QTY_TWO, RESERVATION_QTY_THREE, RESERVATION_QTY_FOUR, RESERVATION_QTY_FIVE, RESERVATION_QTY_SIX, RESERVATION_QTY_SEVEN, RESERVATION_QTY_EIGHT, RESERVATION_QTY_NINE, RESERVATION_QTY_TEN, RESERVATION_QTY_ELEVEN, RESERVATION_QTY_TWELVE, SO_STO_WO_DEMAND_PASTDUE ,SP_SO_STO_WO_QTY_THIRTEEN 
        FROM "_SYS_BIC"."APP_SCM/ZCV_APP_SCM_S_OP_P"
        WHERE (MTART IN ('ZFIN','ZOEM','ZPER') AND MSU ='MSU' AND WERKS in ('CKH1','CKH2','TWH1') OR MATNR in {self.extra_parts})
        AND (SO_STO_WO_DEMAND_ONE+ SO_STO_WO_DEMAND_TWO+ SO_STO_WO_DEMAND_THREE+ SO_STO_WO_DEMAND_FOUR+ SO_STO_WO_DEMAND_FIVE+ SO_STO_WO_DEMAND_SIX+ SO_STO_WO_DEMAND_SEVEN+ SO_STO_WO_DEMAND_EIGHT+ SO_STO_WO_DEMAND_NINE+ SO_STO_WO_DEMAND_TEN+ SO_STO_WO_DEMAND_ELEVEN+ SO_STO_WO_DEMAND_TWELVE+ RESERVATION_QTY_ONE+ RESERVATION_QTY_TWO+ RESERVATION_QTY_THREE+ RESERVATION_QTY_FOUR+ RESERVATION_QTY_FIVE+ RESERVATION_QTY_SIX+ RESERVATION_QTY_SEVEN+ RESERVATION_QTY_EIGHT+ RESERVATION_QTY_NINE+ RESERVATION_QTY_TEN+ RESERVATION_QTY_ELEVEN+ RESERVATION_QTY_TWELVE+ SO_STO_WO_DEMAND_PASTDUE +SP_SO_STO_WO_QTY_THIRTEEN ) > 0
        """
        backlog_ori = pd.read_sql(sql_schema_backlog, self.extract_connection)
        return backlog_ori

    def get_backlog_history(self):
        sql_schema_fcst = "SELECT * FROM ZADDON.ZTHA_HIST_BACKLOG"
        backlog_his = pd.read_sql(sql_schema_fcst, self.extract_connection)
        return backlog_his
    
    def get_gai_data(self): # S9 資料源
        sql_schema_GAI = f"""
        SELECT MATNR, ZSTRATEGY FROM "_SYS_BIC"."APP_SCM/ZCV_APP_SCM_S_OP_P"
        WHERE ((HQABC LIKE 'A%' OR HQABC LIKE 'B%' OR HQABC LIKE 'C%') 
        AND MTART in ('ZFIN','ZOEM','ZPER') 
        AND MSU in ('MSU') 
        OR (MATNR in {self.extra_parts})) 
        AND AIFLAG in ('Y')
        """
        gai_data = pd.read_sql(sql_schema_GAI, self.extract_connection)
        return gai_data

    def save_to_csv(self, dataframe, df_name):
        current_date = self.today_date.strftime('%Y-%m-%d')
        file_path = f'./Dataset/Fcst(proposed)/{df_name}_{current_date}.csv'
        dataframe.to_csv(file_path, mode='w', header=True, index=False)
        print(f"Data saved to {file_path}")