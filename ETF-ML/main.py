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
from Functions import *
from sklearn.model_selection import RandomizedSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
warnings.filterwarnings("ignore")
rqdatac.init()

def init(context):
    context.ETFList = get_code()
    context.Max_StockNum = 8  # 设置最大持有ETF的数量
    context.ETF_PMI = {}  # 字典，存储每一个ETF的PMI指标
    context.ETF_Buy = []  # 列表，存储当前可以买入的ETF名单
    context.price = pd.read_csv('test.csv')
    scheduler.run_daily(before_market_open)
    # 按照计算的轮动指标，执行轮动交易操作
    scheduler.run_daily(before_trading)
    # 收盘后运行：显示当天交易记录，总资产，总收益
    scheduler.run_daily(after_trading)


def get_label(security,context):
    # df_price = import_stock_data()
    # df_price = pre_processing_data(df_price)
    # result = ML_train(df_price = df_price)
    result = context.price
    date = context.now
    date = date.strftime('%Y-%m-%d')
    label = result[(result.order_book_id == security) & (result.date == date)].label_prob.values
    return label

def handle_bar(context, bar_dict):
    pass

def before_market_open(context,bar_dict):
    # 输出运行时间
    logger.info('函数运行时间(before_market_open)：' + str(context.now))

    for security in context.ETFList:
        PMI_Value = get_label(security,context)
        context.ETF_PMI[security] = PMI_Value
        print(PMI_Value)
    # 按照PMI指标值，从大到小排序，我们取前面几个：Max_StockNum
    sorted_list = list(sorted(context.ETF_PMI.items(), key=lambda x: x[1], reverse=True))
    context.ETF_PMI_Buy = sorted_list[0:context.Max_StockNum]  # 只取前面g.Max_StockNum个
    print('可买的ETF的个数：' + str(len(context.ETF_PMI_Buy)))


## 开盘时运行函数（每周三，上午11：15，运行一次）
def before_trading(context,bar_dict):
    logger.info('函数运行时间(market_open):' + str(context.now))
    # 获取可以买入的ETF列表:ETF_Buy
    ETF_Buy = []
    for ETF_PMI in context.ETF_PMI_Buy:
        ETF_Buy.append(ETF_PMI[0])
    print(ETF_Buy)

    # 获取当前持仓的ETF列表
    ETF_Chicang = []
    long_positions_dict = context.portfolio.positions
    for position in list(long_positions_dict.values()):
        ETF_Chicang.append(position.order_book_id)

    # 对比ETF_Buy名单，得到可以建仓的ETF
    ETF_Jiancang = []
    for ETF in ETF_Buy:
        if ETF not in ETF_Chicang:  # 持仓中没有，则可以建仓的ETF
            ETF_Jiancang.append(ETF)
    print(ETF_Jiancang)

    ETF_Qingcang = []
    for ETF in ETF_Chicang:
        if ETF not in ETF_Buy:  # 持仓中ETF，没有在ETF_Buy名单，则可以清仓的ETF
            ETF_Qingcang.append(ETF)
    print(ETF_Qingcang)

    # 进行清仓和建仓
    if ETF_Qingcang != []:  # 有ETF需要清仓，卖出
        for security in ETF_Qingcang:
            orders = order_target_percent(security, 0)  # 满足要求，清仓卖出
            if orders is None:
                logger.info("创建订单失败...")
            else:
                logger.info(security + "：清仓卖出...")

    weight_total = 0  # 计算权重
    if ETF_Jiancang != []:  # 有ETF需要建仓，买入
        for security in ETF_Jiancang:
            weight = 1 / 0.01
            weight_total += weight
        print(weight_total)
        cash = context.stock_account.cash
        for security in ETF_Jiancang:
            weight = 1 / 0.01
            cash_buy = cash * 0.99 * weight / weight_total
            if cash_buy >= 60000:  # 根据权重，如果建仓金额低于6W，太小，放弃，不建仓
                orders = order_value(security, cash_buy)  # 下一个金额单，当前价格
                if orders is None:
                    logger.info("创建订单失败...")
                else:
                    logger.info(security + "：建仓买入...")


def after_trading(context,bar_dict):
    logger.info(str('函数运行时间(after_market_close):' + str(context.now)))

    # 打印账户信息
    # for position in list(context.portfolio.positions.values()):
    #     securities = position.order_book_id
    #     # cost = position.avg_cost
    #     price = position.price
    #     # ret = 100 * (price / cost - 1)
    #     value = position.value
    #     amount = position.total_amount
    #     print('代  码:{}'.format(securities))
    #     # print('成本价:{}'.format(format(cost, '.2f')))
    #     print('现  价:{}'.format(price))
    #     print('收益率:{}%'.format(format(ret, '.2f')))
    #     print('持  股:{}'.format(amount))
    #     print('市  值:{}'.format(format(value, '.2f')))
    #     print('———————————————————————————————————')
    Cangwei_Total = 100 * context.stock_account.total_value / context.portfolio.total_value
    logger.info('总体市值：{}'.format(format(context.portfolio.total_value, '.0f')))
    logger.info('总体仓位：{}%'.format(format(Cangwei_Total, '.2f')))
    logger.info('可用资金：{}'.format(format(context.stock_account.cash, '.0f')))
    print('———————————————————————————————————————分割线————————————————————————————————————————')



config = {
    "base": {
        # 本地下载数据路径，请替换成您自有数据路径
        "start_date": '2019-01-02',
        "end_date": '2019-12-29',
        "frequency": '1d',
        "accounts": {
            "stock": 1000000000,
            "future": 1000000000
        }
    },
    "extra": {
        "log_level": "info",
    },
    "mod": {
        "sys_analyser": {
            "enabled": True,
            "plot": True,
            "benchmark": '510300.XSHG',
        },
        "sys_simulation": {
            # 是否开启信号模式
            "signal": False,
            # 启用的回测引擎，目前支持 `current_bar` (当前Bar收盘价撮合) 和 `next_bar` (下一个Bar开盘价撮合)
            "matching_type": "current_bar",
            # price_limit: 在处于涨跌停时，无法买进/卖出，默认开启【在 Signal 模式下，不再禁止买进/卖出，如果开启，则给出警告提示。】
            "price_limit": False,
            # liquidity_limit: 当对手盘没有流动性的时候，无法买进/卖出，默认关闭
            "liquidity_limit": False,
            # 是否有成交量限制
            "volume_limit": False,
            # 按照当前成交量的百分比进行撮合
            "volume_percent": 0.25,
        },
        "sys_transaction_cost": {
            # 设置每笔成交最小手续费为5元
            'cn_stock_min_commission': 5,
            # 设置佣金乘数 0.375，乘以默认佣金费率万8等于万3
            "commission_multiplier": 0.375
        }
    }
}

if __name__ == "__main__":
    from rqalpha_plus import run_func

    run_func(config=config, init=init, handle_bar=handle_bar)
