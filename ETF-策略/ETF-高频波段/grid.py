# 导入聚宽数据函数库
from jqdata import *

# 初始化函数，设定基准等等
def initialize(context):
    # 设定沪深300指数作为基准
    set_benchmark('000300.XSHG')
    # 使用真实价格交易
    set_option('use_real_price', True)
    # 避免未来函数（防止使用未来数据）
    set_option("avoid_future_data", True)
    
    # 设置交易成本，包括买入时佣金、卖出时的佣金和印花税，以及最低佣金
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
    # 设置操作的ETF代码
    g.ETF = ['512880.XSHG']
    # 设置网格交易的网格大小，即价格变动的百分比
    g.net_range = 0.05
    # 设置每个网格的投资金额
    g.per_share = 20000
    
    # 设置用于平均价格计算的天数
    g.days = 60
    # 设置价格下限的百分比
    g.down = 0.2
    # 设置价格上限的百分比
    g.up = 0.2

    # 记录当前处于第几个网格
    g.net = {}
    
    # 记录第一个网格的买入价格
    g.base_price = {}
    
    # 记录每一层网格的买入份数
    g.buy_amount = {}
    
    # 根据可用现金计算最大网格层数
    g.max_net = int(context.portfolio.available_cash/g.per_share)
        
    # 每日收盘后执行的函数，用来计算持仓成本
    run_daily(cal_avg, '15:30')    
        
    # 每天开盘前检查是否有分红或者拆分的情况，并设置调整比例
    run_daily(run_check, '9:30')
    
    # 根据分红或者拆分的情况调整数据
    run_daily(run_adj, '9:30')
    

# 在每日收盘后，获取现有持仓的成本
def cal_avg(context):
    # 记录每个ETF的收盘后成本
    g.pre_date_avg = {}
    for _, fund in context.portfolio.positions.items():
        # 记录今日收盘的成本，用于明日开盘比较
        g.pre_date_avg[fund.security] =  fund.avg_cost
    log.info(g.pre_date_avg)      
    log.info(g.net)   

# 每天开盘前检查是否有分红或者拆分的情况，并设置调整比例
def run_check(context):
    # 设置调整比例（当出现分红或者拆分的时候调整网格的基准）
    g.adj_rate = {}    
    for _, fund in context.portfolio.positions.items():
        if g.pre_date_avg[fund.security] == fund.avg_cost:
            g.adj_rate[fund.security] = 1
        else:
            g.adj_rate[fund.security] = fund.avg_cost/g.pre_date_avg[fund.security]
    log.info(g.adj_rate)       
    
# 根据分红或者拆分的情况，根据比例调整网格基准价格
def run_adj(context):
    for _, fund in context.portfolio.positions.items():
        g.base_price[fund.security] = g.base_price[fund.security] * g.adj_rate[fund.security]
    
# 每分钟运行的函数
def handle_data(context, data):
    # 检查是否所有ETF都已经买入，如果没有则获取它们的平均价格
    if len(g.net) != len(g.ETF):
        etf_pri = {}
        etf_list = g.ETF[:]
        for etf in g.ETF:
            if etf in g.net:
                etf_list.remove(etf)
        
        # 获取过去60天的价格平均值
        for etf in etf_list:
            avg_pri = get_bars(etf, count=g.days, unit='1d', fields=['close'])['close'].mean()
            etf_pri[etf] = round(avg_pri, 2)
        log.info("当前备选基金的过去%s日平均价格为：" % (g.days))
        log.info(etf_pri)
        
    # 遍历ETF列表，执行交易策略
    for etf in g.ETF: 
        # 如果ETF还没有买入，执行买入策略
        if etf not in g.net:
            log.info("etf:%s" % (etf))
            base_pri = etf_pri[etf]
            # 获取当前价格
            cur_pri = get_price(etf, start_date=context.current_dt, end_date=context.current_dt, 
                                    frequency='1m', fields='close', skip_paused=False, fq='pre')
            if cur_pri.empty:
                continue
            cur_pri = cur_pri.close[0]
            log.info("%s的当前价格为:%s, 上下限为:(%s, %s)" % (etf, cur_pri, str(round((1-g.down) * base_pri, 2)), str(round((1+g.up) * base_pri, 2))))
            
            # 如果当前价格在设定的上下限之内，则买入第一份
            if cur_pri > (1-g.down) * base_pri and cur_pri < (1+g.up) * base_pri:
                # 下单
                order_res = order_value(etf, g.per_share)
                log.info("%s的当前价格为:%s, 符合第一次买入的条件，买入%s股" % (etf, cur_pri, order_res.filled))
                # 如果下单成功，记录网格状态
                if order_res and str(order_res.status) in ['held', 'filled']:
                    g.net[etf] = 1
                    g.base_price[etf] = order_res.price
                    g.buy_amount[etf] = []
                    g.buy_amount[etf].append(order_res.filled * order_res.price)
        else:
            # 如果ETF已经买入，执行卖出策略
            cur_pri = get_price(etf, start_date=context.current_dt, end_date=context.current_dt, 
                                    frequency='1m', fields='close', skip_paused=False, fq='pre')
            if cur_pri.empty:
                continue
            cur_pri = cur_pri.close[0]
            
            # 获取当前网格层数和买入基准价格
            net_layer = g.net[etf]
            buy_base_pri = g.base_price[etf]
            # 根据当前价格和网格层数决定卖出策略
            if cur_pri > buy_base_pri * ( 1 - net_layer * g.net_range + 0.1):
                # 在第一层全部卖出
                if g.net[etf] == 1:
                    order_res = order_target(etf, 0)
                    if order_res and str(order_res.status) in ['held', 'filled']:
                        g.buy_amount[etf].pop()
                        g.net[etf] = g.net[etf] - 1
                        if g.net[etf] == 0:
                            del g.net[etf]
                            del g.buy_amount[etf]
                else:
                    # 计算卖出份数
                    buy_in_rate = ( 1 - (net_layer - 1) * g.net_range)
                    sell_out_rate = ( 1 - (net_layer - 2) * g.net_range)
                    tmp = (g.buy_amount[etf][0]*( 1 + (sell_out_rate - buy_in_rate)/buy_in_rate)/cur_pri) % 100
                    if tmp <= 1:
                        sell_num = (g.buy_amount[etf][0]*( 1 + (sell_out_rate - buy_in_rate)/buy_in_rate)/cur_pri)
                    else:
                        sell_num = int(g.buy_amount[etf][0]*( 1 + (sell_out_rate - buy_in_rate)/buy_in_rate)/cur_pri) + 100
                    # 下单卖出
                    order_res = order(etf, -sell_num)
                    if order_res and str(order_res.status) in ['held', 'filled']:
                        g.buy_amount[etf].pop()
                        g.net[etf] = g.net[etf] - 1
                        if g.net[etf] == 0:
                            del g.net[etf]
                            del g.buy_amount[etf]
            
            # 根据当前价格和网格层数决定买入策略
            if cur_pri < buy_base_pri * ( 1 - net_layer * g.net_range):
                # 如果网格层数没达到最大值，继续买入
                if g.net[etf] < g.max_net:
                    order_res = order_value(etf, g.per_share)
                    if order_res and str(order_res.status) in ['held', 'filled']:
                        g.net[etf] = g.net[etf] + 1
                        g.buy_amount[etf].append(order_res.filled * order_res.price)
            
    # 收盘时输出持仓情况
    if  context.current_dt.hour == 14 and context.current_dt.minute == 59:
        log.info("当前持有的基金的买入层数为：")
        log.info(g.net)    
        log.info("每一层的买入份数为")
        log.info(g.buy_amount)       
        # 输出每个基金的买入成本
        for _, fund in context.portfolio.positions.items():
            log.info('%s当前价格为%s：累计的持仓成本:%s, 当前持仓成本:%s, 当日持仓成本:%s' % (fund.security, fund.price, fund.acc_avg_cost, fund.avg_cost, fund.hold_cost))
            log.info('总仓位：%s' % (fund.total_amount))     