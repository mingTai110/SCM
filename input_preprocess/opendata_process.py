import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime

class OpenDataProcessor:
    def __init__(self, open_so_histo, open_wo_histo):
        """
        初始化 OpenDataProcessor，存入歷史開放銷售訂單（SO）和工作訂單（WO）數據。

        :param open_so_histo: 歷史開放銷售訂單數據 DataFrame
        :param open_wo_histo: 歷史開放工作訂單數據 DataFrame
        """
        self.open_so_histo = open_so_histo
        self.open_wo_histo = open_wo_histo

    def _process_open_current_data(self, open_so_curr):
        """
        處理當前開放訂單數據（SO），將 `VERSION` 減去 1 個月。

        :param open_so_curr: 當前開放訂單數據 DataFrame
        :return: 處理後的 DataFrame
        """
        open_so_curr_copy = open_so_curr.copy()
        open_so_curr_copy['VERSION'] = pd.to_datetime(open_so_curr_copy['VERSION'], format='%Y/%m/%d')
        open_so_curr_copy['VERSION'] = open_so_curr_copy['VERSION'].apply(lambda x: x - relativedelta(months=1))
        open_so_curr_copy['VERSION'] = open_so_curr_copy['VERSION'].dt.strftime('%Y/%m/%d')
        return open_so_curr_copy

    def process_open_data(self, curr_data, version_col, columns_to_keep, exclude_version=None):
        """
        處理開放數據（過濾歷史數據、處理當前數據並合併）。

        :param curr_data: 當前開放數據 DataFrame
        :param version_col: 版本欄位名稱
        :param columns_to_keep: 需要保留的欄位列表
        :param exclude_version: 需要排除的 `VERSION`，默認為前一個月
        :return: 最終合併的 DataFrame
        """
        if exclude_version is None:
            exclude_version = (datetime.now() - relativedelta(months=1)).strftime('%Y/%m/%d')

        print(f'exclude_version: {exclude_version}')

        # 過濾歷史數據
        histo_data = self.open_so_histo if version_col == 'VERSION' else self.open_wo_histo
        histo_filtered = histo_data[histo_data[version_col] != exclude_version].reset_index(drop=True)

        # 處理當前數據
        curr_filtered = self._process_open_current_data(curr_data)
        curr_filtered = curr_filtered[columns_to_keep]

        # 合併數據
        final_data = pd.concat([histo_filtered, curr_filtered], ignore_index=True)

        return final_data

    def get_realtime_data(self, open_so_curr, open_wo_curr, exclude_month=None):
        """
        獲取即時開放訂單數據（SO 與 WO）。

        :param open_so_curr: 當前未來銷售訂單數據 DataFrame
        :param open_wo_curr: 當前未來工作訂單數據 DataFrame
        :param exclude_month: 需要排除的 `VERSION`，默認為前一個月
        :return: (處理後的未來 SO 數據, 處理後的未來 WO 數據)
        """
        so_columns_to_keep = ['VERSION', 'COMPANY_ID', 'PART_NO', 'SHIP_PLANT', 'YM', 'OPEN_QTY']
        wo_columns_to_keep = ['VERSION', 'PART_NO', 'PLANT', 'YM', 'OPEN_QTY']

        # 確保 OPEN_QTY 欄位正確
        open_so_curr['OPEN_QTY'] = open_so_curr['ORDER_QTY']

        # 處理 SO 和 WO 數據
        open_so_final = self.process_open_data(open_so_curr, 'VERSION', so_columns_to_keep, exclude_version=exclude_month)
        open_wo_final = self.process_open_data(open_wo_curr, 'VERSION', wo_columns_to_keep, exclude_version=exclude_month)

        return open_so_final, open_wo_final
