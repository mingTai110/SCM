import pandas as pd 
# location E:\SCM_FCST_MSU\20241002_09.00_monthly_abc model_selector_within_predict_result_P_fulfillment.csv
def get_orgin_fcst(month):
      temp=pd.read_csv(f'./Dataset/compare_data/2024{month}版.csv')
      temp=temp[['YM','PART_NO', 'F-TOTAL_QTY']].copy()
      temp['month']=temp['YM'].str[-5:-3].astype(int)
      temp=temp[temp['month']== (int(month)+2)]
      temp['year_month'] = pd.to_datetime(temp['YM'])
      temp=temp.rename(columns={'F-TOTAL_QTY':'MODEL_QTY'})
      temp=temp[['year_month','PART_NO','MODEL_QTY']].reset_index(drop=True)

      return temp

def orgin_fcst_result(start_month_version=2, evaluation_months=5):    
      month_strings = [f"{i:02d}" for i in range(start_month_version, start_month_version+evaluation_months)]
      data=pd.DataFrame()
      for month in month_strings:
            data=pd.concat([data,get_orgin_fcst(month)])
      return data 