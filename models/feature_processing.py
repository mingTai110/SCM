import pandas as pd

def feature_data(PART_NO_Openso_1,PART_NO_Openso_2, PART_NO_SO, Trend_Indicator,Indicator):
    data= pd.DataFrame()
    data['Date']=PART_NO_Openso_1.year_month
    data['ORDER_QTY']=PART_NO_SO.ORDER_QTY
    data['Openso_1']=PART_NO_Openso_1.OPEN_QTY
    data['Openso_2']=PART_NO_Openso_2.OPEN_QTY
     # 2 to 1 
    Openso1and2_diff=PART_NO_Openso_1.OPEN_QTY-PART_NO_Openso_2.OPEN_QTY
    data ['Feature_1']= Openso1and2_diff.shift(-1).fillna(0)
    data ['Feature_2']= Openso1and2_diff.shift(0).fillna(0)
    data ['Feature_3']= Openso1and2_diff.shift(1).fillna(0)
    data ['Feature_4']= Openso1and2_diff.shift(2).fillna(0)
    data ['Feature_5']= Openso1and2_diff.shift(3).fillna(0)
    data ['Feature_6']= Openso1and2_diff.shift(4).fillna(0)
    # 1 to so
    Openso1andso_diff= PART_NO_SO.ORDER_QTY-PART_NO_Openso_1.OPEN_QTY
    data ['Feature_7']= (Openso1andso_diff.shift(1)).fillna(0)
    data ['Feature_8']= (Openso1andso_diff.shift(2)).fillna(0)
    data ['Feature_9']= (Openso1andso_diff.shift(3)).fillna(0)
    data ['Feature_10']= (Openso1andso_diff.shift(4)).fillna(0)
    data ['Feature_11']= (data['Openso_2'].shift(-2).fillna(0)-data['Openso_2'].shift(-1).fillna(0))
    data ['Feature_12']= data['Openso_2']
    data ['Feature_13']= data['Openso_2'].shift(-2).fillna(0)
    data['Diff_two_month']= data['ORDER_QTY']-data['Openso_2']
    # data ['Feature_12']= (data['Openso_1'].shift(-1).fillna(0)-data['Openso_2'])
    # data ['Feature_12']= data['Openso_2'].shift(-1).fillna(0)
    Backlog=Indicator[0].merge(Indicator[1],on='Date', how='inner',suffixes=('_openso1', '_openso2'))
    data=data.merge(Trend_Indicator, on='Date', how='left')
    data=data.merge(Backlog, on='Date', how='left')
    data= data.fillna(0)
    return data

def input_output_data(data, columns_t, columns_t2, use_all=True):
    inputs=[]
    outputs = []
    # 找出 labels 為 1 -1 的列的索引位置
    if use_all==True:
        index = data.index
    else:
        index = data[(data['labels'] == 1) | (data['labels'] == -1) ].index
        
    for idx in index :
        if idx + 2 < len(data):
            inputs.append(data.loc[idx , columns_t])
            outputs.append(data.loc[idx + 2, columns_t2])

    return inputs, outputs

def feature_process(PART_NO_Openso_1,PART_NO_Openso_2, PART_NO_SO, Trend_Indicator ,Indicator ,input_columns, output_columns):

    caculate_data=feature_data(PART_NO_Openso_1,PART_NO_Openso_2, PART_NO_SO, Trend_Indicator ,Indicator)
    input_data, output_data=input_output_data(caculate_data, input_columns, output_columns,use_all=True)

    return input_data, output_data


def train_valid_split(input_data, output_data, end_date, fcst_days=1,train_month= 13):
    
    train_input=pd.DataFrame(input_data[end_date-train_month:end_date ]).iloc[:,1:].reset_index(drop=True)
    train_output=pd.DataFrame(output_data[end_date-train_month:end_date ]).reset_index(drop=True)
    train_output['labels']=pd.DataFrame(input_data[end_date-train_month:end_date]).iloc[:,0].reset_index(drop=True)

    test_input=pd.DataFrame(input_data[end_date: end_date+fcst_days]).iloc[:,1:].reset_index(drop=True)
    test_output=pd.DataFrame(output_data[end_date: end_date+fcst_days]).reset_index(drop=True)
    test_output['labels']=pd.DataFrame(input_data[end_date: end_date+fcst_days]).iloc[:,0].reset_index(drop=True)
    
    return train_input, test_input, train_output, test_output

def normalrize_data(train_input, test_input, train_output, test_output):
    #normalrize feature
    train_data=(train_input-train_input.mean())/train_input.std()
    test_data=(test_input-train_input.mean())/train_input.std()
    # normalrize output diff
    diff_mean=(train_output.loc[:,'Diff_two_month']).mean()
    diff_std=(train_output.loc[:,'Diff_two_month']).std()
    train_output['mean']=diff_mean
    train_output['std']=diff_std
    train_output['Normalrized_Diff_two_month']=(train_output.loc[:,'Diff_two_month']-diff_mean)/diff_std
    test_output['mean']=diff_mean
    test_output['std']=diff_std
    test_output['Normalrized_Diff_two_month']=(test_output.loc[:,'Diff_two_month']-diff_mean)/diff_std
    
    return  train_data, test_data, train_output, test_output
