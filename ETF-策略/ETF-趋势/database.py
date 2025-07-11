import psycopg2, psycopg2.extras, traceback
import pandas as pd
from functools import wraps

# 异常处理 & 自动重连
def psql_try_except(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except psycopg2.OperationalError:
            if args[0]._conn_times < args[0].max_retries:
                args[0]._connect()
                wrapper(*args, **kwargs)
            else:
                args[0]._close()
                return None
        except:
            print(traceback.format_exc())
            args[0]._conn.rollback()
            return None
    return wrapper


POSTGRES_IP  = "192.168.80.58"
POSTGRES_PORT  = "5432"
POSTGRES_USERNAME  = "postgres"
POSTGRES_PASSWORD  = "07290819"


class PostgresUtils:
    def __init__(
            self, 
            user=POSTGRES_USERNAME, 
            password=POSTGRES_PASSWORD, 
            host=POSTGRES_IP, 
            port=POSTGRES_PORT, 
            dbname='postgres', 
            keepalives=1, 
            keepalives_idle=120, 
            keepalives_interval=10, 
            keepalives_count=10, 
            max_retries=5
            ):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.dbname = dbname
        self.keepalives = keepalives                        # 保持连接
        self.keepalives_idle = keepalives_idle              # 空闲时，每120秒保持连接连通
        self.keepalives_interval = keepalives_interval      # 没得到回应时，等待10秒重新尝试保持连通
        self.keepalives_count = keepalives_count            # 尝试最多10次重新保持连通
        self._conn_times = 0                                # 连接次数
        self.max_retries = max_retries                      # 最大重连次数
        self._cursor = None
        self._conn = None
        self._connect()

    # 连接数据库
    def _connect(self):
        self._conn_times += 1
        self._conn = psycopg2.connect(
            user=self.user, 
            password=self.password, 
            host=self.host, 
            port=self.port,
            dbname=self.dbname,
            keepalives=self.keepalives,
            keepalives_idle=self.keepalives_idle,
            keepalives_interval=self.keepalives_interval,
            keepalives_count=self.keepalives_count
        )
        self._cursor = self._conn.cursor()
        print(f"[POSTGRES] 创建连接池, HOST: {self.host}, PORT: {self.port}, NAME: {self.user}, DBNAME: {self.dbname}")
    
    # 关闭数据库连接
    def _close(self):
        # 事务提交
        self._conn.commit()
        # 关闭数据库连接
        self._cursor.close()
        self._conn.close()

    # 参数暂时仅用于条件查询，且默认表示等于
    # TOTHINK1: 基于实际需求可扩展，params = [("xx", "=", "xxx"), ("xx", ">=", 1)]
    # TOTHINK2: 复杂需求，另外封装接口
    @psql_try_except
    def query_return_dataframe(self, table_name: str, params: dict={}):
        """
        params: {id: xx, ...}
        """
        sql = f"SELECT * FROM {table_name}"
        if params:
            psql_list = [f"{key} = '{value}'" if isinstance(value, str) else f"{key} = {value}" for key, value in params.items()]
            psql_str = " AND ".join(psql_list)
            sql += f" WHERE {psql_str}"
        
        # print("query_return_dataframe:", sql)
        df = pd.read_sql(sql, self._conn)
        return df
    
    @psql_try_except
    def query_return_dataframe_with_sql(self, table_name: str, where: str):
        sql = f"SELECT * FROM {table_name} {where}"
        # print("query_return_dataframe_with_sql:", sql)
        df = pd.read_sql(sql, self._conn)
        return df
    
    @psql_try_except
    def run_with_sql_return_dataframe(self, sql: str):
        df = pd.read_sql(sql, self._conn)
        return df

    # 单条执行：并且可以返回新插入的id
    @psql_try_except
    def run_with_sql_return_id(self, sql):
        self._cursor.execute(sql)
        self._conn.commit()
        return self._cursor.fetchone()
    
    # 批量插入
    @psql_try_except
    def batch_insert_with_dataframe(self, table_name: str, data: pd.DataFrame, page_size: int=5000):
        columns = ",".join(data.columns)
        data = data.to_numpy()
        sql = f"INSERT INTO {table_name} ({columns}) VALUES %s"
        psycopg2.extras.execute_values(self._cursor, sql, data, page_size=page_size)
        self._conn.commit()
        return True

    # TODO: 待验证
    # 批量插入：不存在则插入，存在则更新
    @psql_try_except
    def batch_upsert_with_dataframe(self, table_name: str, data: pd.DataFrame, primary_key: list=[], page_size: int=5000):
        columns = ",".join(data.columns)
        sql = f"INSERT INTO {table_name} ({columns}) VALUES %s "
        if len(primary_key):
            key = ",".join(primary_key)
            set_list = [f"{col}=EXCLUDED.{col}" for col in data.columns if col not in primary_key]
            if set_list:
                updates = ",".join(set_list)
                sql += f"ON CONFLICT ({key}) DO UPDATE SET {updates}"
        data = data.to_numpy()
        psycopg2.extras.execute_values(self._cursor, sql, data, page_size=page_size)
        self._conn.commit()
        return True

# if __name__ == "__main__":
#     psql = PostgresUtils()
#     psql.query_return_dataframe("strategy", {"id": "0", "name": "triavg"})
vnpy_db = PostgresUtils(dbname="postgres")
import datetime as dt
import pandas as pd
import traceback

def get_price(rq_symbol, start: dt.datetime, end: dt.datetime, frequency: str):
    conn = vnpy_db._conn

    if frequency == "60m":
        freq = "1h"
    elif frequency == "1d":
        freq = "d"
    else:
        freq = "1m"

    symbol = rq_symbol

    select = f"select * from dbbardata where symbol='{symbol}' and interval='{freq}' and \
              datetime >= '{start}' and datetime <= '{end}'"

    try:
        data = pd.read_sql(select, conn).set_index("datetime")
        data = data.rename(columns={
            "open_price": "open",
            "close_price": "close",
            "high_price": "high",
            "low_price": "low"
        })
    except Exception as e:
        data = None
        print(traceback.format_exc())

    return data
