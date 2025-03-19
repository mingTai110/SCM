from utils.tools import WACC
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def plt_fcst(y_train,y_pred):
    plt.figure(figsize=(12, 6))
    plt.plot(range(len(y_train)), y_train, label='Actual Values', color='blue')
    plt.plot(range(len(y_train)), y_pred, label='Predicted Values', color='red', alpha=0.7)
    plt.xlabel('Sample Index')
    plt.ylabel('Values')
    plt.title('Actual vs Predicted Values')
    plt.legend()
    plt.show()

def test_data_performance(Valid_Input_n, Valid_Output_n, y_test_pred):
    test_data=pd.concat([Valid_Input_n, Valid_Output_n], axis=1)
    test_data['y_pred']=y_test_pred
    test_data['pred_demand']=round(test_data['y_pred']*test_data['std']+test_data['mean'])#+test_data['Openso_2'])
    test_data['Error_rate']=np.abs((test_data['pred_demand']-test_data['ORDER_QTY'])/test_data['ORDER_QTY']).clip(0,1).fillna(0)
    test_data['Pred_Acc']=round((1-test_data['Error_rate'])*100, 2)
    print('模型(含0)預測準確度',round(test_data['Pred_Acc'].mean(),2))
    print('此PD(含0)模型WACC準確度',round(WACC(test_data['Pred_Acc'], test_data['ORDER_QTY']),2))
    # print('模型(含0)Rule_base準確度',test_data['ACC'].mean())
    return test_data

def print_func(data,result_type,acc_columns):
    temp=pd.DataFrame()
    temp['type']=[result_type]
    temp['number']=[len(data)]
    temp['ACC']=[round(data[acc_columns].mean(),2)]
    temp['WACC']=[round(WACC(data[acc_columns], data['ORDER_QTY']),2)]
    return temp

# def fcst_performance( fcst_data):
#     #僅有open so 一個月之間能見度變化+上上個月openso+這個月openso 2
#     all_result=fcst_data[((fcst_data['labels']==1 )|(fcst_data['labels']==-1 ))]
#     downward_result=fcst_data[((fcst_data['labels']==-1))]
#     upward_result=fcst_data[((fcst_data['labels']==1 ))]

#     print_func(all_result,'所有' ,'Pred_Acc')
#     print_func(downward_result, '下降', 'Pred_Acc')
#     print_func(upward_result, '上升', 'Pred_Acc')

#     return all_result[['labels','ORDER_QTY','pred_demand','Pred_Acc']]


def fcst_performance( fcst_data):
    #僅有open so 一個月之間能見度變化+上上個月openso+這個月openso 2
    all_result=fcst_data[((fcst_data['labels']==1 )|(fcst_data['labels']==-1 ))]
    downward_result=fcst_data[((fcst_data['labels']==-1))]
    upward_result=fcst_data[((fcst_data['labels']==1 ))]

    temp=pd.DataFrame()
    temp=pd.concat([temp, print_func(fcst_data,'所有預測(Model)' ,'Model_Acc')])
    temp=pd.concat([temp, print_func(fcst_data,'所有預測(MSU)' ,'MSU_Acc')])
    temp=pd.concat([temp, print_func(fcst_data,'所有預測(DS)' ,'DS_Acc')])
    temp=pd.concat([temp, print_func(fcst_data,'所有預測(AWS)' ,'AWS_Acc')])
    temp=pd.concat([temp, print_func(fcst_data,'所有預測' ,'Pred_Acc')])
    temp=pd.concat([temp, print_func(all_result,'所有指標(Model)' ,'Model_Acc')])
    temp=pd.concat([temp, print_func(all_result,'所有指標(MSU)' ,'MSU_Acc')])
    temp=pd.concat([temp, print_func(all_result,'所有指標(DS)' ,'DS_Acc')])
    temp=pd.concat([temp, print_func(all_result,'所有指標(AWS)' ,'AWS_Acc')])
    temp=pd.concat([temp, print_func(all_result,'所有指標' ,'Pred_Acc')])
    temp=pd.concat([temp, print_func(upward_result,'上升指標(Model)' ,'Model_Acc')])
    temp=pd.concat([temp, print_func(upward_result,'上升指標(MSU)' ,'MSU_Acc')])
    temp=pd.concat([temp, print_func(upward_result,'上升指標(DS)' ,'DS_Acc')])
    temp=pd.concat([temp, print_func(upward_result,'上升指標(AWS)' ,'AWS_Acc')])     
    temp=pd.concat([temp, print_func(upward_result,'上升指標' ,'Pred_Acc')])
    temp=pd.concat([temp, print_func(downward_result,'下降指標(Model)' ,'Model_Acc')])
    temp=pd.concat([temp, print_func(downward_result,'下降指標(MSU)' ,'MSU_Acc')])
    temp=pd.concat([temp, print_func(downward_result,'下降指標(DS)' ,'DS_Acc')])
    temp=pd.concat([temp, print_func(downward_result,'下降指標(AWS)' ,'AWS_Acc')])
    temp=pd.concat([temp, print_func(downward_result,'下降指標' ,'Pred_Acc')])
    return temp