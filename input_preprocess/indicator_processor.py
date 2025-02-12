import pandas as pd
import numpy as np

class IndicatorProcessor:
    def __init__(self, full_date_range):
        self.full_date_range = full_date_range

    def fillna(self, data, date_name='year_month', QTY_name='ORDER_QTY'):
        data = data.set_index(date_name).reindex(self.full_date_range).reset_index()
        data.rename(columns={'index': date_name}, inplace=True)
        data[QTY_name] = data[QTY_name].fillna(0) 
        return data
    
    def change_rate(self, open_data, shift_month):
        change_rate = ((open_data['OPEN_QTY'] - open_data['OPEN_QTY'].shift(shift_month)) / open_data['OPEN_QTY'].shift(shift_month).replace(0, 1) * 100).fillna(0)
        return change_rate

    def foward_indicator(self, PART_NO_Openso_1, PART_NO_Openso_2, PART_NO_Openso_3):
        indicator_1 = self.change_rate(PART_NO_Openso_1, 1)
        indicator_2 = self.change_rate(PART_NO_Openso_2, 1)
        indicator_3 = self.change_rate(PART_NO_Openso_3, 2)#

        Indicator = [pd.DataFrame() for _ in range(3)]
        Indicator[0]['Date'] = PART_NO_Openso_1.year_month - pd.DateOffset(months=1)
        Indicator[0]['Indicator'] = indicator_1
        Indicator[1]['Date'] = PART_NO_Openso_2.year_month - pd.DateOffset(months=1)
        Indicator[1]['Indicator'] = indicator_2
        Indicator[2]['Date'] = PART_NO_Openso_3.year_month - pd.DateOffset(months=2)
        Indicator[2]['Indicator'] = indicator_3

        return Indicator

    def trend_table(self, Indicator):
        Indicator_selection = pd.merge(Indicator[1], Indicator[2], on='Date', how='inner', suffixes=('_0', '_1'))
        conditions = [
            (Indicator_selection['Indicator_0'] > 0) & (Indicator_selection['Indicator_1'] > 30) & (Indicator_selection['Indicator_1'] > Indicator_selection['Indicator_0']),
            (Indicator_selection['Indicator_0'] < 0) & (Indicator_selection['Indicator_1'] < -50) & (Indicator_selection['Indicator_1'] < Indicator_selection['Indicator_0'])
        ]
        choices = [1, -1]
        labels = np.select(conditions, choices, default=0)

        trend_labels = pd.DataFrame()
        trend_labels['Date'] = Indicator_selection['Date']
        trend_labels['labels'] = labels

        return trend_labels

    def historical_table(self, PART_NO_SO):
        historical_info = pd.DataFrame()
        historical_info['Date'] = PART_NO_SO['year_month'] - pd.DateOffset(months=2)
        historical_info['labels'] = np.where(PART_NO_SO['ORDER_QTY'].shift(2) < PART_NO_SO['2_month_avg'], 1, -1)
        return historical_info

    def validation_indicator_performance(self, PART_NO_name, Trend_Indicator, Historical_info):
        trend_compare = pd.merge(Trend_Indicator, Historical_info, on='Date', how='inner', suffixes=('_indicator', '_historical'))
        trend_compare_selected = trend_compare[trend_compare['labels_indicator'] != 0].copy()
        trend_compare_selected.loc[:, 'same_labels'] = trend_compare_selected['labels_indicator'] == trend_compare_selected['labels_historical']
        pos_indicator = (trend_compare_selected['labels_indicator'] == 1).sum()
        tp_num = ((trend_compare_selected['labels_indicator'] == 1) & (trend_compare_selected['same_labels'] == True)).sum()
        tp_rate = (tp_num / pos_indicator) * 100
        neg_indicator = (trend_compare_selected['labels_indicator'] == -1).sum()
        tn_num = ((trend_compare_selected['labels_indicator'] == -1) & (trend_compare_selected['same_labels'] == True)).sum()
        tn_rate = (tn_num / neg_indicator) * 100
        total_indicator_num = len(trend_compare_selected)

        data_to_save = pd.DataFrame({
            'PART_NO': [PART_NO_name],
            "Total_num": [total_indicator_num],
            'Upward_num': [pos_indicator],
            'Downward_num': [neg_indicator],
            'TP_ratio_up': [round(tp_rate, 2)],
            'TN_ratio_down': [round(tn_rate, 2)],
            'TP_num': [tp_num],
            'TN_num': [tn_num]
        })

        return data_to_save

    def process_and_fillna(self, data, part_no_name, date_column, qty_name):
        material_sql = 'PART_NO==@part_no_name'
        part_no_data = data.query(material_sql).groupby([date_column])[qty_name].sum().reset_index()
        part_no_data = self.fillna(part_no_data, QTY_name=qty_name)
        return part_no_data
    
    def generate_indicators(self, PART_NO_name, Used_Data, filtered_open_data_1, filtered_open_data_2, filtered_open_data_3):

        date_column = 'year_month'
        PART_NO_SO = self.process_and_fillna(Used_Data, PART_NO_name, date_column, 'ORDER_QTY')
        PART_NO_SO['2_month_avg'] = PART_NO_SO['ORDER_QTY'].rolling(window=2).mean()

        PART_NO_Openso_1 = self.process_and_fillna(filtered_open_data_1, PART_NO_name, date_column, 'OPEN_QTY')
        PART_NO_Openso_2 = self.process_and_fillna(filtered_open_data_2, PART_NO_name, date_column, 'OPEN_QTY')
        PART_NO_Openso_3 = self.process_and_fillna(filtered_open_data_3, PART_NO_name, date_column, 'OPEN_QTY')
    
        Indicator = self.foward_indicator(PART_NO_Openso_1, PART_NO_Openso_2, PART_NO_Openso_3)
        Trend_Indicator = self.trend_table(Indicator)
        Historical_info = self.historical_table(PART_NO_SO)
        Evaluate_Table = self.validation_indicator_performance(PART_NO_name, Trend_Indicator, Historical_info)
        
        return PART_NO_SO, PART_NO_Openso_1, PART_NO_Openso_2, PART_NO_Openso_3, Trend_Indicator, Evaluate_Table