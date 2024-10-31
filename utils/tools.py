import numpy as np 
import pandas as pd
import os 
import matplotlib.pyplot as plt


def Caculate_Gap_year(version, YM):
    year= YM.apply(lambda x: x.replace('/','')[2:4]).astype(int)- version.apply(lambda x: x.replace('/','')[2:4]).astype(int)
    month=YM.apply(lambda x: x.replace('/','')[4:6]).astype(int)- version.apply(lambda x: x.replace('/','')[4:6]).astype(int)
    diff_month=12*year+month
    return diff_month


def fiilna(data_name, full_date_range, date_name='year_month', QTY_name= 'ORDER_QTY' ):
    ''' 
    填補歷史so日期
    '''
    # 使用 reindex 根據完整的日期範圍來補全缺失的日期
    data_name= data_name.set_index(date_name).reindex(full_date_range).reset_index()

    # 將列名 'index' 重新命名為 'year_month'
    data_name.rename(columns={'index': date_name}, inplace=True)

    # 使用 fillna 填充缺失的 ORDER_QTY 為 0
    min=data_name[QTY_name].min()
    
    data_name[QTY_name] = data_name[QTY_name].fillna(0) # min

    return data_name

def Save_Part_Data(file_path, PART_NO_SO,Trend_Indicator, PG, Region_class, PD_name, PART_NO_name ,save_file=None):
    '''
    存取資料info  
    '''
    # file_path = r'C:\Users\mt.yang\Desktop\SCM_Indicator\EIOT_AB_AIMB_EBC(B).csv' 
    PART_NO_SO=PART_NO_SO.rename(columns={'year_month': 'Date'})
    data_output=pd.merge(PART_NO_SO, Trend_Indicator, on='Date', how='outer').dropna(subset=['ORDER_QTY']).fillna(0)
    # 加入基本資訊
    data_output.insert(0, 'PG_Class', PG)
    data_output.insert(1, 'Ship_Plant', Region_class)
    data_output.insert(2, 'PD', PD_name)
    data_output.insert(3, 'PART_NO', PART_NO_name)
    data_output.drop(columns=['2_month_avg'], inplace=True)

    if save_file==True:
        

        # 如果文件不存在，寫入數據並添加標題; 如果文件存在，則追加數據並不寫入標題
        if not os.path.exists(file_path):
            data_output.to_csv(file_path, mode='w', header=True, index=False)
        else:
            data_output.to_csv(file_path, mode='a', header=False, index=False)



def Plot_History_Trend(PART_NO_name, Model_openso, Model_openso_2, so_info, trend_labels):   
    '''
    這張圖是在說所有客戶同料號的情況
    '''
    plt.figure(figsize=(10, 6))
    # plt.plot(open_so_info['YM'], open_so_info['OPEN_QTY'], label=f'{model_name} and {PART_NO_name} in open_So by 2 month (shift)', marker='o',color='orange')
    '''
    open_data time series
    '''
    plt.plot(Model_openso['year_month'], Model_openso['OPEN_QTY'], label=f' {PART_NO_name} in open_So by 1 month (shift)', marker='o',color='orange')
    plt.plot(Model_openso_2['year_month'], Model_openso_2['OPEN_QTY'], label=f'{PART_NO_name} in open_So by 2 month (shift)', marker='o',color='pink')
    # plt.plot(Model_openso_3['YM'], Model_openso_3['OPEN_QTY'], label=f'{model_name} and {PART_NO_name} in open_So by 3 month (shift)', marker='o',color='brown')
    # plt.plot(opendata_info['YM'], opendata_info['Trend'], label=f'{model_name} and {PART_NO_name} in open_So action at thrat month ', marker='o',color='green')
    '''
    so_data time series
    '''
    plt.plot(so_info['year_month'], so_info['ORDER_QTY'], label=f' {PART_NO_name} in So', marker='x',color='blue')
    # plt.plot(so_info['Date'], so_info['2_month_avg'], label=f' {PART_NO_name} in So', marker='x',color='green')
    # plt.plot(so_partno_time_series['year_month'], so_partno_time_series['3_month_avg'], label=f'{model_name} and {PART_NO_name} in So (MA3)', marker='x',color='purple')
    ''' 
    時序誤差量
    '''
    openso_so_diff=Model_openso['OPEN_QTY']-so_info['ORDER_QTY']
    # # 標準化
    openso_so_diff_normal=(openso_so_diff-openso_so_diff.mean())/openso_so_diff.std()
    # plt.bar(so_info['year_month'], openso_so_diff_normal, label=f'{model_name} Normalize Diff openso-so ', alpha=0.3, width=20 ,color='black')
    # plt.bar(so_info['year_month'], openso_so_diff, label=f'{model_name} Diff openso-so ', alpha=0.3, width=20 ,color='green')
    # plt.bar(diff_so_openso["Date"], diff_so_openso["Diff"], label=f'{model_name} Diff openso-so ', alpha=0.3, width=20 ,color='green')
    ''' 
    open_so 指標
    '''
    plt.bar(trend_labels['Date'], trend_labels['labels']*10, label='trend_labels' , alpha=0.3, width=20 ,color='black')
    # plt.bar(Indicator[0]['Date'], Indicator[0]['Indicator']*0.05, label='Indicator one ', alpha=0.3, width=20 ,color='red')
    # plt.bar(Indicator[1]['Date'], Indicator[1]['Indicator']*0.05, label=' Indicator two ', alpha=0.3, width=20 ,color='green')
    # 添加标签和标题
    plt.xlabel('Date')
    plt.ylabel('Value')
    # plt.title(f'MODEL_{model} Comparison between so  and open_so')
    plt.legend()
