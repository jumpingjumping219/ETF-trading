import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime
import rqfactor
from rqalpha.apis import *
from rqfactor.notebook.exposure import factor_analysis
from rqoptimizer import *
from rqoptimizer.utils import *
from rqdatac import *
import rqdatac
import rqalpha_plus
from rqalpha_plus import run_func
import warnings
warnings.filterwarnings("ignore")
rqdatac.init()

# 参考：中国银河证券研究院，结合价格动量和拥挤度的ETF交易策略
# 价格动量+拥挤度，两个维度，进行ETF的每周三轮动
# 轮动的周期为：一周，每周三，上午11：15

# 价格动量，是一种基于历史价格走势预测未来股价的投资策略。该策略认为，股票价格的走势具有惯性，即在较短时间内涨势或跌势会持续一段时间。
# 根据这个思想，价格动量策略会挑选出在过去一段时间内表现良好的股票，并买入其股票，同时卖出表现不佳的股票，适合震荡行情。
# 价格动量PMI指标，根据5、10、20、30、40、50、60、75日移动平均价格的计算，来构建价格势头强弱的指标。最大值100，最小值-100，间隔增加值为25。

# 拥挤度，拥挤度越大，大量投资者涌向同一个ETF，非理性情绪驱动，投资风险越大
# 拥挤度指标，采用ETF份额在过去60个交易日的历史分位数，分位数越大，拥挤度越大，价格下跌的可能性和程度越大，ETF的权重越小

# ETF候选池：规模大于20亿，日成交量大于5000万，指数型股票ETF，精简到34个（数量多，避免挑选几个，实际导致未来函数问题）

# 导入函数库
import pandas as pd
from datetime import datetime
from statistics import mean

code = pd.read_excel('etf标的.xlsx')
etf_code = code.code.values.tolist()
etf_code = [str(code) for code in etf_code]
df_etf = all_instruments(type='ETF' , market = 'cn')
df_etf = df_etf[df_etf.trading_code.isin(etf_code)]
etf_code2 = df_etf.order_book_id.values.tolist()

# 初始化函数，设定基准等等
def init(context):
    context.Max_StockNum = 8  # 设置最大持有ETF的数量
    context.ETF_PMI = {}  # 字典，存储每一个ETF的PMI指标
    context.ETF_CRI = {}  # 字典，存储每一个ETF的CRI指标
    context.ETF_Buy = []  # 列表，存储当前可以买入的ETF名单
    # 精简后的ETF候选池1010：加一减一，总数还是34个，避免行业重复
    context.ETFList = etf_code2
    context.previous_date = get_previous_trading_date(context.now)
    scheduler.run_weekly(before_market_open, weekday=2)
    # 每周三，中午11：15，按照计算的轮动指标，执行轮动交易操作
    scheduler.run_weekly(market_open, weekday=3)
    # 收盘后运行2：显示当天交易记录，总资产，总收益
    scheduler.run_daily(after_market_close)

config = {
    "base": {
        # 本地下载数据路径，请替换成您自有数据路径
        "start_date": '2018-01-02',
        "end_date": '2023-12-29',
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
            "benchmark": '000300.XSHG',
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


def get_PMI(security,context):
    # 获取一个ETF的历史前75日的收盘价
    close_data = history_bars(security,bar_count=75,frequency='1d',fields='close')['close']
    # close_data = get_price(security , start_date=get_previous_trading_date(context.now,n=75),end_date=context.now,frequency='1d',fields='close')['close']
    # 取前一日的收盘价
    price = close_data[-1]
    # 计算过去5、10、20、30、40、50、60、75日的移动平均价格
    MA75 = close_data[0:75].mean()
    MA60 = close_data[15:75].mean()
    MA50 = close_data[25:75].mean()
    MA40 = close_data[35:75].mean()
    MA30 = close_data[45:75].mean()
    MA20 = close_data[55:75].mean()
    MA10 = close_data[65:75].mean()
    MA5 = close_data[70:75].mean()
    MA_List = [MA5, MA10, MA20, MA30, MA40, MA50, MA60, MA75]

    # 根据规则，计算价格动量指标PMI
    PMI = -100
    Num_Da = 0
    if MA5 > MA10:
        if price > MA5:
            Num_Da += 1
    elif MA5 > MA20:
        if price > MA5:
            Num_Da += 1
    elif MA5 > MA30:
        if price > MA5:
            Num_Da += 1
    elif MA5 > MA40:
        if price > MA5:
            Num_Da += 1
    elif MA5 > MA50:
        if price > MA5:
            Num_Da += 1
    elif MA5 > MA60:
        if price > MA5:
            Num_Da += 1
    elif MA5 > MA75:
        if price > MA5:
            Num_Da += 1

    if MA10 > MA20:
        if price > MA10:
            Num_Da += 1
    elif MA10 > MA30:
        if price > MA10:
            Num_Da += 1
    elif MA10 > MA40:
        if price > MA10:
            Num_Da += 1
    elif MA10 > MA50:
        if price > MA10:
            Num_Da += 1
    elif MA10 > MA60:
        if price > MA10:
            Num_Da += 1
    elif MA10 > MA75:
        if price > MA10:
            Num_Da += 1

    if MA20 > MA30:
        if price > MA20:
            Num_Da += 1
    elif MA20 > MA40:
        if price > MA20:
            Num_Da += 1
    elif MA20 > MA50:
        if price > MA20:
            Num_Da += 1
    elif MA20 > MA60:
        if price > MA20:
            Num_Da += 1
    elif MA20 > MA75:
        if price > MA20:
            Num_Da += 1

    if MA30 > MA40:
        if price > MA30:
            Num_Da += 1
    elif MA30 > MA50:
        if price > MA30:
            Num_Da += 1
    elif MA30 > MA60:
        if price > MA30:
            Num_Da += 1
    elif MA30 > MA75:
        if price > MA30:
            Num_Da += 1

    if MA40 > MA50:
        if price > MA40:
            Num_Da += 1
    elif MA40 > MA60:
        if price > MA40:
            Num_Da += 1
    elif MA40 > MA75:
        if price > MA40:
            Num_Da += 1

    if MA50 > MA60:
        if price > MA50:
            Num_Da += 1
    elif MA50 > MA75:
        if price > MA50:
            Num_Da += 1

    if MA60 > MA75:
        if price > MA60:
            Num_Da += 1

    PMI += 25 * Num_Da
    return PMI

## 计算一个ETF的拥挤度指标CRI，根据一个ETF在过去60个交易日的ETF份额的历史百分位
def get_CRI(security, context):
    ETF_shares = pd.DataFrame()
    today = context.previous_date
    for day_num in range(90):  # 取90个自然日的数据，实际交易日60+
        df = finance.run_query(query(finance.FUND_SHARE_DAILY).filter(finance.FUND_SHARE_DAILY.date == today,
                                                                      finance.FUND_SHARE_DAILY.code == security))
        data = df[['code', 'date', 'shares']]
        ETF_shares = pd.concat([ETF_shares, data], axis=0)
        yesterday = today - datetime.timedelta(days=1)
        today = yesterday

    if ETF_shares.empty == False:
        ETF_shares = ETF_shares[['shares']]
        ETF_shares = ETF_shares.iloc[:, 0]
        Share_yesterday = ETF_shares.iloc[0]
        # print(Share_yesterday)
        for baifenwei in range(100):
            baifenwei = baifenwei / 100
            Share_Value_Low = ETF_shares.quantile(q=baifenwei, interpolation='linear')
            Share_Value_High = ETF_shares.quantile(q=baifenwei + 0.01, interpolation='linear')
            if Share_yesterday >= Share_Value_Low and Share_yesterday <= Share_Value_High:
                Baifenwei_yesterday = baifenwei
                break
        return Baifenwei_yesterday
    else:
        Baifenwei_yesterday = -1  # 当ETF_shares为空时，返回-1
        return Baifenwei_yesterday


## 开盘前运行函数：计算2个轮动指标：价格动量PMI、拥挤度
def before_market_open(context,bar_dict):
    # 输出运行时间
    logger.info('函数运行时间(before_market_open)：' + str(context.now))

    for security in context.ETFList:
        PMI_Value = get_PMI(security,context)
        # print(security+'的PMI指标 = '+str(PMI_Value))
        context.ETF_PMI[security] = PMI_Value
        # CRI_Value = get_CRI(security, context)
        # # print(security+'的CRI指标 = '+str(CRI_Value))
        # g.ETF_CRI[security] = CRI_Value
        # if CRI_Value == -1:  # 判断非正常情况，删除一个ETF的成对数据
        #     del g.ETF_PMI[security]
        #     del g.ETF_CRI[security]
        # print('———————————————————————————————————')
    # 按照PMI指标值，从大到小排序，我们取前面几个：g.Max_StockNum
    sorted_list = list(sorted(context.ETF_PMI.items(), key=lambda x: x[1], reverse=True))
    # print(sorted_list)
    context.ETF_PMI_Buy = sorted_list[0:context.Max_StockNum]  # 只取前面g.Max_StockNum个
    # print(g.ETF_PMI_Buy)
    print('可买的ETF的个数：' + str(len(context.ETF_PMI_Buy)))


## 开盘时运行函数（每周三，上午11：15，运行一次）
def market_open(context,bar_dict):
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

    # 对比ETF_Chicang名单，得到可以清仓的ETF
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
        cash = context.portfolio.available_cash
        for security in ETF_Jiancang:
            weight = 1 / 0.01
            cash_buy = cash * 0.99 * weight / weight_total
            if cash_buy >= 60000:  # 根据权重，如果建仓金额低于6W，太小，放弃，不建仓
                orders = order_value(security, cash_buy)  # 下一个金额单，当前价格
                if orders is None:
                    logger.info("创建订单失败...")
                else:
                    logger.info(security + "：建仓买入...")

def handle_bar(context, bar_dict):
    pass


def after_market_close(context,bar_dict):
    logger.info(str('函数运行时间(after_market_close):' + str(context.now)))

    # 打印账户信息
    # for position in list(context.portfolio.positions.values()):
    #     securities = position.security
    #     cost = position.avg_cost
    #     price = position.price
    #     ret = 100 * (price / cost - 1)
    #     value = position.value
    #     amount = position.total_amount
    #     print('代  码:{}'.format(securities))
    #     print('成本价:{}'.format(format(cost, '.2f')))
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

if __name__ == "__main__":
    from rqalpha_plus import run_func

    run_func(config=config, init=init, handle_bar=handle_bar)

