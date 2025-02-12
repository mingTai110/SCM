import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
from utils.hana_connection import Connect_HANA

class EOLHandler:
    def __init__(self):
        self.extract_connection = Connect_HANA()
        self.extra_parts = ('9892431F00E', '989243PF00E', '989243PF10E', '989243PF20E', '9892F10F00E', '9892P3PF00E', '9892P3PF10E')
        self.implement_pgs = ('ISG', 'IAG', 'ICWG', 'ESG', 'ECG', 'IPSG', 'IDSG')
        self.iiot_pgs=('ISG', 'IAG', 'ICWG', 'ESG')
        self.eiot_pgs=('ECG', 'IPSG', 'IDSG')
        self.today_date = datetime.datetime.today()

    def get_target_product(self, part_no_list):
        sql_schema_product = f"""
        SELECT DISTINCT MATNR AS PART_NO, PRDPG AS PG, PRDPD AS PD, ZZPLM_STAT AS PLM_STATUS, HQABC AS ABC_INDICATOR, ZZMP_DATE
        FROM _SYS_BIC."APP_AIS/ZCV_APP_AIS_MATERIAL_MASTER"
        WHERE (
            PRDPG IN {self.implement_pgs}
            AND MTART IN ('ZFIN', 'ZOEM')
            OR (PRDPD IN ('Industrial Storage') AND (MTART IN ('ZFIN', 'ZOEM', 'ZPER')))
            OR (MATNR IN {self.extra_parts})
        )
        AND MATNR IN {tuple(part_no_list)}
        """
        product_ori = pd.read_sql(sql_schema_product, self.extract_connection)
        return product_ori

    def get_eol_pair(self):
        sql_schema_eol = f"""
        SELECT MATNR, ZZREPLACEBY, ZZLASTBUY_DATE
        FROM "_SYS_BIC"."AA_MASTER/ZAT_AA_MASTER_GENERAL_MATERIAL_DATA"
        WHERE ZZREPLACEBY IN (
            SELECT DISTINCT MATNR
            FROM ZADDON.ZTHA_AIFCST
            WHERE FCST_TYPE = 'M'
            AND CREATE_DATE = (
                SELECT MAX(CREATE_DATE)
                FROM ZADDON.ZTHA_AIFCST
                WHERE FCST_TYPE = 'M'
            )
        )
        """
        eol_ori = pd.read_sql(sql_schema_eol, self.extract_connection)
        eol_ori = eol_ori[eol_ori['MATNR'] != eol_ori['ZZREPLACEBY']]
        eol = eol_ori[~eol_ori['MATNR'].str.contains('-T|-ES') & ~eol_ori['ZZREPLACEBY'].str.contains('-T|-ES')]
        eol['MATNR'] = eol['MATNR'].str.replace('\xa0', '', regex=False)
        eol['ZZREPLACEBY'] = eol['ZZREPLACEBY'].str.replace('\xa0', '', regex=False)
        eol.rename(columns={'MATNR': 'PART_NO_OLD', 'ZZREPLACEBY': 'PART_NO_NEW', 'ZZLASTBUY_DATE': 'LASTBUY_DATE'}, inplace=True)
        return eol

    def get_target_EOL_parts(self, parts_changed):
        today = pd.Timestamp.today().normalize()
        parts_changed = parts_changed[parts_changed['PART_NO_NEW'] != 'AIMB-508HF-EAA1']
        parts_changed = parts_changed[parts_changed['ABC_INDICATOR_NEW'].str.startswith(('A', 'B'))] # add C indicator 
        parts_changed['ZZMP_DATE_NEW'] = parts_changed['ZZMP_DATE_NEW'].replace('00000000', '99991231')
        parts_changed['ZZMP_DATE_NEW'] = pd.to_datetime(parts_changed['ZZMP_DATE_NEW'], format='%Y%m%d')
        parts_changed = parts_changed[parts_changed['ZZMP_DATE_NEW'] > today - relativedelta(years=2)]
        parts_changed = parts_changed[parts_changed['PD_NEW'] != 'Industrial Storage']
        return parts_changed

    def determine_relationship(self, part_no, replace_by, part_to_replace_counts, replace_to_part_counts):
        part_replace_count = part_to_replace_counts[part_no]
        replace_part_count = replace_to_part_counts[replace_by]
        if part_replace_count == 1 and replace_part_count == 1:
            return 'one-to-one'
        elif part_replace_count > 1 and replace_part_count == 1:
            return 'one-to-many'
        elif part_replace_count == 1 and replace_part_count > 1:
            return 'many-to-one'
        else:
            return 'many-to-many'
        
    # 產出資料
    def get_eol_data(self):
        eol = self.get_eol_pair()
        product_ori = self.get_target_product(eol['PART_NO_OLD'].unique().tolist() + eol['PART_NO_NEW'].unique().tolist())
        df = eol.merge(product_ori, how="left", left_on="PART_NO_OLD", right_on="PART_NO")
        df1 = df.merge(product_ori, how="left", left_on="PART_NO_NEW", right_on="PART_NO")
        df1.columns = ['PART_NO_OLD', 'PART_NO_NEW', 'LASTBUY_DATE', 'PART_NO_x', 'PG_OLD', 'PD_OLD', 'PLM_STATUS_OLD', 'ABC_INDICATOR_OLD', 'ZZMP_DATE_OLD', 'PART_NO_y', 'PG_NEW', 'PD_NEW', 'PLM_STATUS_NEW', 'ABC_INDICATOR_NEW', 'ZZMP_DATE_NEW']
        df1 = df1[['PART_NO_OLD', 'PART_NO_NEW', 'LASTBUY_DATE', 'PG_OLD', 'PD_OLD', 'PLM_STATUS_OLD', 'ABC_INDICATOR_OLD', 'ZZMP_DATE_OLD', 'PG_NEW', 'PD_NEW', 'PLM_STATUS_NEW', 'ABC_INDICATOR_NEW', 'ZZMP_DATE_NEW']]
        df1 = df1.dropna()

        part_to_replace_counts = df1.groupby('PART_NO_OLD')['PART_NO_NEW'].nunique()
        replace_to_part_counts = df1.groupby('PART_NO_NEW')['PART_NO_OLD'].nunique()

        df1['Relationship'] = df1.apply(lambda row: self.determine_relationship(row['PART_NO_OLD'], row['PART_NO_NEW'], part_to_replace_counts, replace_to_part_counts), axis=1)
        df1 = self.get_target_EOL_parts(df1)
        df1 = df1[['PART_NO_OLD', 'PART_NO_NEW']].reset_index(drop=True)
        self.eol_map_data=df1
        # return self.eol_map_data
    
    def merge_and_replace_part_no(self, data):
        merged_df = data.merge(self.eol_map_data, left_on='PART_NO', right_on='PART_NO_OLD', how='left')
        merged_df['PART_NO'] = merged_df['PART_NO_NEW'].fillna(merged_df['PART_NO'])
        merged_df = merged_df.drop(columns=['PART_NO_OLD', 'PART_NO_NEW'])
        return merged_df
    
    def get_target_so(self, previous_year, parts_list):
        parts_str = ', '.join([f"'{part}'" for part in parts_list])
        sql_schema_so = f"""
        SELECT * FROM _SYS_BIC."APP_AIS/ZCV_APP_AIS_SO_INFO_BY_REQUIREDT"
        WHERE REQUIRED_DATE > '{previous_year}'
        AND Part_No IN ({parts_str})
        """
        so_ori = pd.read_sql(sql_schema_so, self.extract_connection)
        merged_so_ori=self.merge_and_replace_part_no(so_ori)
        return merged_so_ori

    def get_target_wo(self, previous_year, parts_list):
        parts_str = ', '.join([f"'{part}'" for part in parts_list])
        sql_schema_wo = f"""
        SELECT * FROM "_SYS_BIC"."APP_AIS/ZCV_APP_AIS_WO_CONSUMPTION"
        WHERE START_DATE >= '{previous_year}'
        AND PART_NO IN ({parts_str})
        """
        wo_ori = pd.read_sql(sql_schema_wo, self.extract_connection)
        merged_wo_ori=self.merge_and_replace_part_no(wo_ori)
        return merged_wo_ori

    def get_target_shipment(self, parts_list):
        parts_str = ', '.join([f"'{part}'" for part in parts_list])
        sql_schema_shipment = f"""
        SELECT * 
        FROM "_SYS_BIC"."APP_SCM/ZCV_APP_SCM_SHIPMENT"
        WHERE SHIP_MONTH > '201812' 
        AND MATNR IN ({parts_str})
        """
        shipment_ori = pd.read_sql(sql_schema_shipment, self.extract_connection)
        return shipment_ori

    def get_target_open_so(self, previous_month, PART_NO_list):
        parts_str = ', '.join([f"'{part}'" for part in PART_NO_list])
        sql_schema_open_so = f"""
        SELECT * FROM "ZADDON"."ZTHA_HISTSO"
        WHERE Part_No IN ({parts_str})
        AND VERSION >= '{previous_month}'
        """
        open_so_ori = pd.read_sql(sql_schema_open_so, self.extract_connection)
        merged_open_so_ori=self.merge_and_replace_part_no(open_so_ori)
        return merged_open_so_ori

    def get_target_open_wo(self, previous_month, PART_NO_list):
        parts_str = ', '.join([f"'{part}'" for part in PART_NO_list])
        sql_schema_open_wo = f"""
        SELECT * FROM "ZADDON"."ZTHA_HISTWO"
        WHERE Part_No IN ({parts_str})
        AND VERSION >= '{previous_month}'
        """
        open_wo_ori = pd.read_sql(sql_schema_open_wo, self.extract_connection)
        merged_open_wo_ori=self.merge_and_replace_part_no(open_wo_ori)
        return merged_open_wo_ori

    def get_target_current_open_so(self, PART_NO_list):
        if len(PART_NO_list) == 1:
            sql_schema_open_so = f"""SELECT * FROM "_SYS_BIC"."APP_AIS/ZCV_APP_AIS_OPEN_SO" where PART_NO = '{PART_NO_list[0]}'"""
        else:
            sql_schema_open_so = f"""SELECT * FROM "_SYS_BIC"."APP_AIS/ZCV_APP_AIS_OPEN_SO" where PART_NO in {tuple(PART_NO_list)}"""
        open_so_ori = pd.read_sql(sql_schema_open_so, self.extract_connection)
        return open_so_ori

    def get_target_current_open_wo(self, PART_NO_list):
        if len(PART_NO_list) == 1:
            sql_schema_open_wo = f"""SELECT * FROM "_SYS_BIC"."APP_AIS/ZCV_APP_AIS_OPEN_WO" where PART_NO = '{PART_NO_list[0]}'"""
        else:
            sql_schema_open_wo = f"""SELECT * FROM "_SYS_BIC"."APP_AIS/ZCV_APP_AIS_OPEN_WO" where PART_NO in {tuple(PART_NO_list)}"""
        open_wo_ori = pd.read_sql(sql_schema_open_wo, self.extract_connection)
        return open_wo_ori
    

    
    # def get_target_current_open_so_wo_ori(self, PART_NO_list):
    #     open_so_ori = self.get_target_current_open_so(PART_NO_list=PART_NO_list)
    #     open_wo_ori = self.get_target_current_open_wo(PART_NO_list=PART_NO_list)

    #     opengroupby_so = open_so_ori.groupby(['VERSION', 'PART_NO', 'YM']).agg({'OPEN_QTY': 'sum'}).reset_index()
    #     opengroupby_wo = open_wo_ori.groupby(['VERSION', 'PART_NO', 'YM']).agg({'OPEN_QTY': 'sum'}).reset_index()

    #     open_so_wo_ori = opengroupby_so.merge(opengroupby_wo, how='outer', left_on=['PART_NO', 'YM'], right_on=['PART_NO', 'YM'], suffixes=('', '_wo'))
    #     open_so_wo_ori['VERSION'] = open_so_wo_ori['VERSION'].fillna(open_so_wo_ori['VERSION_wo'])

    #     open_so_wo_ori.drop('VERSION_wo', axis=1, inplace=True)
    #     open_so_wo_ori['OPEN_QTY_so_wo'] = open_so_wo_ori['OPEN_QTY'].fillna(0) + open_so_wo_ori['OPEN_QTY_wo'].fillna(0)

    #     return open_so_wo_ori[['VERSION', 'YM', 'PART_NO', 'OPEN_QTY', 'OPEN_QTY_wo', 'OPEN_QTY_so_wo']]

    # def get_eol_current_open_so_wo_ori(self, part_changed_df):
    #     part_no_list = part_changed_df['PART_NO_OLD'].unique().tolist()
    #     open_so_wo_ori = self.get_target_current_open_so_wo_ori(part_no_list)

    #     open_so_wo_ori = open_so_wo_ori.merge(part_changed_df[['PART_NO_OLD', 'PART_NO_NEW']], how='left', left_on='PART_NO', right_on='PART_NO_OLD')
    #     open_so_wo_ori = open_so_wo_ori.groupby(['PART_NO_NEW', 'YM']).agg({'OPEN_QTY': 'sum', 'OPEN_QTY_wo': 'sum', 'OPEN_QTY_so_wo': 'sum'}).reset_index()
    #     open_so_wo_ori.rename(columns={'OPEN_QTY': 'OPEN_QTY_so'}, inplace=True)

    #     return open_so_wo_ori
    