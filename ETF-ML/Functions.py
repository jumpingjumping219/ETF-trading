import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime
import rqfactor
import talib
from rqalpha.apis import *
from rqfactor.notebook.exposure import factor_analysis
from rqoptimizer import *
from rqoptimizer.utils import *
from rqdatac import *
from rqalpha_plus import run_func
import warnings
from sklearn.model_selection import RandomizedSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
warnings.filterwarnings("ignore")
rqdatac.init()

def get_code():
    df_etf = pd.read_excel('etf标的.xlsx')
    etf_code = df_etf.code.values.tolist()
    etf_code = [str(code) for code in etf_code]
    df_etf = all_instruments(type='ETF' , market = 'cn')
    df_etf = df_etf[df_etf.trading_code.isin(etf_code)]
    etf_code2 = df_etf.order_book_id.values.tolist()
    return etf_code2

def get_code_2():
    df_etf_code = pd.read_excel('ETF_industry.xlsx')
    etf_code = df_etf_code.code.values.tolist()
    etf_code = [id_convert(code) for code in etf_code]
    return etf_code

def import_stock_data():
    etf_code2 = get_code()
    df_price = get_price(etf_code2, start_date='20160101' ,end_date='20231231' , fields=['open','high','low','close'])
    df_price = df_price.dropna().reset_index()
    df_price = df_price.sort_index().fillna(method='ffill').dropna()
    df_price['date'] = df_price['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    # 按收盘价计算每日涨幅
    df_price['pct'] = df_price['close'] / df_price['close'].shift(1) - 1.0
    df_price = df_price.dropna().reset_index(drop=True)
    return df_price

# 定义一个函数用于计算移动平均线
def calculate_sma(group):
    close_prices = group['close'].values
    group['sma5'] = talib.SMA(close_prices, timeperiod=5)
    group['sma10'] = talib.SMA(close_prices, timeperiod=10)
    group['sma20'] = talib.SMA(close_prices, timeperiod=20)
    group['sma30'] = talib.SMA(close_prices, timeperiod=30)
    group['sma40'] = talib.SMA(close_prices, timeperiod=40)
    group['sma50'] = talib.SMA(close_prices, timeperiod=50)
    group['sma60'] = talib.SMA(close_prices, timeperiod=60)
    group['sma75'] = talib.SMA(close_prices, timeperiod=75)
    return group
# 最近十天累计收益率
def calculate_roc(group):
    group['roc'] = group.close.pct_change(periods = 10)
    return group
# 最近十天收盘价累计增长量
def calculate_mom(group):
    group['mom'] = group.close.diff(periods = 10)
    return group
# label
def calculate_label(group):
    group['label'] = 0
    group['avg_returns'] = group['pct'].rolling(window=5).mean()
    group.loc[(group['avg_returns'] > 0.003) & (group['pct'] > 0), 'label'] = 1
    return group

def pre_processing_data(df_price):
    df_price = df_price.groupby('order_book_id').apply(calculate_sma).dropna().reset_index(drop=True)
    df_price = df_price.groupby('order_book_id').apply(calculate_roc).dropna().reset_index(drop=True)
    df_price = df_price.groupby('order_book_id').apply(calculate_mom).dropna().reset_index(drop=True)
    df_price = df_price.groupby('order_book_id').apply(calculate_label).dropna().reset_index(drop=True)
    return df_price

def train_test_split(df_price):
    train_start = pd.to_datetime('2016-01-01')
    train_end = pd.to_datetime('2019-12-31')
    test_start = pd.to_datetime('2020-01-01')
    test_end = pd.to_datetime('2023-12-31')
    # 按照股票代码进行分组，并划分训练集和测试集
    train_data = pd.DataFrame()
    test_data = pd.DataFrame()
    df_price['date'] = pd.to_datetime(df_price['date'])

    for order_book_id, group in df_price.groupby('order_book_id'):
        train_group = group[(group['date'] >= train_start) & (group['date'] <= train_end)]
        test_group = group[(group['date'] >= test_start) & (group['date'] <= test_end)]
        train_data = pd.concat([train_data, train_group])
        test_data = pd.concat([test_data, test_group])

    X_train = train_data[
        ['open', 'high', 'low', 'close', 'sma5', 'sma10', 'sma20', 'sma30', 'sma40', 'sma50', 'sma60', 'sma75', 'roc',
         'mom']]
    y_train = train_data.label.values
    X_test = test_data[
        ['open', 'high', 'low', 'close', 'sma5', 'sma10', 'sma20', 'sma30', 'sma40', 'sma50', 'sma60', 'sma75', 'roc',
         'mom']]
    y_test = test_data.label.values
    return (X_train,y_train,X_test,y_test)

def ML_train(df_price):
    param_grid = {
        'n_estimators': [50, 100, 200],
        'max_depth': [3, 5, 7],
        'min_samples_split': [2, 5, 10]
    }
    X_train, y_train, X_test, y_test = train_test_split(df_price)
    model = RandomForestClassifier()
    random_search = RandomizedSearchCV(model, param_grid, n_iter=10, cv=5, scoring='accuracy')
    random_search.fit(X_train, y_train)
    best_model = random_search.best_estimator_
    y_test_pred = best_model.predict(X_test)

    accuracy = accuracy_score(y_test, y_test_pred)
    precision = precision_score(y_test, y_test_pred)
    recall = recall_score(y_test   , y_test_pred)
    f1 = f1_score(y_test, y_test_pred)
    confusion_mat = confusion_matrix(y_test, y_test_pred)
    # 7. 输出测试集 ETF 上涨概率作为 PMI 的替代性变量
    y_test_prob = best_model.predict_proba(X_test)[:, 1]
    X_test['label_prob'] = y_test_prob
    return X_test


