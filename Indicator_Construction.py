import pandas as pd  
import numpy as np
def Change_rate(open_data, shift_month):

    change_rate= ((open_data['OPEN_QTY']-open_data['OPEN_QTY'].shift(shift_month))/open_data['OPEN_QTY'].shift(shift_month)*100).fillna(0)
    
    return  change_rate



def Foward_Indicator(PART_NO_Openso_1, PART_NO_Openso_2, PART_NO_Openso_3 ):
    '''
    指標設計條件  
    '''
    indicator_1= Change_rate(PART_NO_Openso_1, 1)
    indicator_2= Change_rate(PART_NO_Openso_2, 2)
    indicator_3= Change_rate(PART_NO_Openso_3, 3)


    Indicator = [pd.DataFrame() for _ in range(3)]
    Indicator[0]['Date']=PART_NO_Openso_1.year_month- pd.DateOffset(months=1)
    Indicator[0]['Indicator']=indicator_1
    Indicator[1]['Date']=PART_NO_Openso_2.year_month- pd.DateOffset(months=2)
    Indicator[1]['Indicator']=indicator_2
    Indicator[2]['Date']=PART_NO_Openso_3.year_month- pd.DateOffset(months=3)
    Indicator[2]['Indicator']=indicator_3

    return Indicator 


def Trend_Table(Indicator):
    '''
    方向指標設定趨勢指標條件  
    '''
    # 完成兩種backlog變化率的計算
    Indicator_selection = pd.merge(Indicator[0], Indicator[1], on='Date', how='inner', suffixes=('_0', '_1'))
    # 條件設定
    conditions = [
        (Indicator_selection['Indicator_0'] > 0) & (Indicator_selection['Indicator_1'] > 30) & (Indicator_selection['Indicator_1'] > Indicator_selection['Indicator_0']),# 同時大於0
        (Indicator_selection['Indicator_0'] < 0) & (Indicator_selection['Indicator_1'] < -50 ) &(Indicator_selection['Indicator_1'] < Indicator_selection['Indicator_0']) # 同時小於0
    ]
    # 給予對應的標籤值
    choices = [1, -1]

    # 使用 np.select 根據條件給予標籤，其他情況標籤為 0
    labels = np.select(conditions, choices, default=0)

    # 指標dataframe 
    trend_labels=pd.DataFrame()
    trend_labels['Date']=Indicator_selection['Date']
    trend_labels['labels']=labels

    return trend_labels

def Historcial_Table(PART_NO_SO):
       # 歷史資料 dataframe 
    historical_info=pd.DataFrame()
    historical_info['Date']=PART_NO_SO['year_month']- pd.DateOffset(months=2)
    historical_info['labels'] = np.where(PART_NO_SO['ORDER_QTY'].shift(2) <PART_NO_SO['2_month_avg'], 1, -1)
    return historical_info

'''
驗證趨勢指標& 實際情形是否一致 
'''
def Validation_Indicator_Performance(PART_NO_name, Trend_Indicator, Historical_info):

    trend_compare=pd.merge(Trend_Indicator, Historical_info, on='Date', how='inner', suffixes=('_indicator', '_historical'))
    # 忽略=0 盤整情況
    trend_compare_sleected=trend_compare[trend_compare['labels_indicator']!=0].copy()
    # 判斷兩個欄位是否相同
    trend_compare_sleected.loc[:, 'same_labels'] = trend_compare_sleected['labels_indicator'] == trend_compare_sleected['labels_historical']
    # 指標為正的數量
    pos_indicator=(trend_compare_sleected['labels_indicator'] == 1).sum()
    # 指標為正真為正的數量
    tp_num=((trend_compare_sleected['labels_indicator'] == 1) & (trend_compare_sleected['same_labels'] == True)).sum()
    tp_rate=(tp_num/pos_indicator)*100
    # 指標為負的數量
    neg_indicator=(trend_compare_sleected['labels_indicator'] == -1).sum()
    # 指標為負真為負的數量
    tn_num=((trend_compare_sleected['labels_indicator'] == -1) & (trend_compare_sleected['same_labels'] == True)).sum()
    tn_rate=(tn_num/neg_indicator)*100
    total_indicator_num=len(trend_compare_sleected)

    data_to_save = pd.DataFrame({
    'PART_NO': [PART_NO_name],  # 料號
    "Total_num":[total_indicator_num],
    'Upward_num': [pos_indicator],  # 向上指標數
    'Downward_num': [neg_indicator],  # 向下指標數
    'TP_ratio_up': [round(tp_rate, 2)],  # 向上TP ratio
    'TN_ratio_down': [round(tn_rate, 2)],  # 向下TP ratio
    'TP_num':   [tp_num],
    'TN_num':   [tn_num]
    })


    return data_to_save

# def 

    # 將 DataFrame 寫入 CSV 檔案，指定檔名
    
    # file_path = 'Indicator_Confusion_matrix.csv'

    # # 如果文件不存在，寫入數據並添加標題; 如果文件存在，則追加數據並不寫入標題
    # if not os.path.exists(file_path):
    #     data_to_save.to_csv(file_path, mode='w', header=True, index=False)
    # else:
    #     data_to_save.to_csv(file_path, mode='a', header=False, index=False)