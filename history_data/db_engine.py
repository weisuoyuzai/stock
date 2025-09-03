from sqlalchemy import create_engine
import os
import pandas as pd

DB_CONFIG = {
    'host': 'mariadb',
    'user': 'root',
    'password': os.environ.get('MYSQL_PASSWORD','123456'),
    'database': 'instockdb',
    'port': 3306,
    'charset': 'utf8mb4'
}

CLICKHOUSE_CONFIG = {
    'host': 'clickhouse',
    'port': 8123,
    'username': 'root',
    'password': os.environ.get('CLICKHOUSE_PASSWORD','123456'),
    'database': 'instockdb'
}

current_dir = os.path.dirname(os.path.abspath(__file__))
CODE_MAP_CSV = os.path.join(current_dir, 'code_map.csv')
HISTORY_DIR = os.path.join(current_dir, 'history_stock_data')
if not os.path.exists(HISTORY_DIR):
    os.makedirs(HISTORY_DIR)
AGG_DATA_DIR = os.path.join(current_dir, 'agg_data')
if not os.path.exists(AGG_DATA_DIR):
    os.makedirs(AGG_DATA_DIR)


def create_mysql_engine():
    """创建MySQL数据库连接引擎"""
    try:
        # 构建连接字符串
        connection_string = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}?charset={DB_CONFIG['charset']}"
        
        engine = create_engine(
            connection_string,
            echo=False,  # 设置为True可以看到SQL语句
            pool_pre_ping=True,  # 连接池预检
            pool_recycle=3600,   # 连接回收时间
        )
        
        print(f"成功连接到MySQL数据库: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        return engine
    
    except Exception as e:
        print(f"连接数据库失败: {str(e)}")
        return None


def create_clickhouse_client():
    """创建ClickHouse数据库连接客户端"""
    try:
        import clickhouse_connect
        
        # 首先连接到默认数据库来创建目标数据库
        temp_client = clickhouse_connect.get_client(
            host=CLICKHOUSE_CONFIG['host'],
            port=CLICKHOUSE_CONFIG['port'],
            username=CLICKHOUSE_CONFIG['username'],
            password=CLICKHOUSE_CONFIG['password']
        )
        
        # 创建数据库（如果不存在）
        create_db_sql = f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_CONFIG['database']}"
        temp_client.command(create_db_sql)
        print(f"数据库 {CLICKHOUSE_CONFIG['database']} 已确保存在")
        temp_client.close()
        
        # 连接到目标数据库
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_CONFIG['host'],
            port=CLICKHOUSE_CONFIG['port'],
            username=CLICKHOUSE_CONFIG['username'],
            password=CLICKHOUSE_CONFIG['password'],
            database=CLICKHOUSE_CONFIG['database']
        )
        
        # 测试连接
        result = client.query("SELECT 1")
        print(f"成功连接到ClickHouse数据库: {CLICKHOUSE_CONFIG['host']}:{CLICKHOUSE_CONFIG['port']}/{CLICKHOUSE_CONFIG['database']}")
        return client
    
    except ImportError:
        print("需要安装clickhouse-connect: pip install clickhouse-connect")
        return None
    except Exception as e:
        print(f"连接ClickHouse数据库失败: {str(e)}")
        return None


def create_stock_history_table_clickhouse(client):
    """在ClickHouse中创建股票历史数据表（单表设计 + 最佳实践）"""
    try:
        # 删除表如果存在（用于重建）
        drop_sql = "DROP TABLE IF EXISTS cn_stock_history"
        client.command(drop_sql)
        
        # 创建优化的表结构
        create_table_sql = """
        CREATE TABLE cn_stock_history (
            date Date,
            code LowCardinality(String),
            market LowCardinality(String),
            open Decimal(12, 4),
            high Decimal(12, 4),
            low Decimal(12, 4),
            close Decimal(12, 4),
            preclose Decimal(12, 4),
            volume UInt64,
            amount Decimal(20, 2),
            adjustflag UInt8,
            turn Decimal(8, 4),
            tradestatus UInt8,
            p_change Decimal(8, 4),
            isST UInt8
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(date)
        ORDER BY (date, code)
        SETTINGS index_granularity = 8192
        """
        
        client.command(create_table_sql)
        print("✅ 成功创建ClickHouse主表: cn_stock_history")
        
        # 创建日度市场统计物化视图
        mv_daily_stats_sql = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS cn_market_daily_stats
        ENGINE = SummingMergeTree()
        PARTITION BY toYYYYMM(date)
        ORDER BY (date, market)
        AS SELECT
            date,
            market,
            count() as stock_count,
            sum(volume) as total_volume,
            sum(amount) as total_amount,
            countIf(p_change > 0) as up_count,
            countIf(p_change < 0) as down_count,
            avg(p_change) as avg_change
        FROM cn_stock_history
        GROUP BY date, market
        """
        
        client.command(mv_daily_stats_sql)
        print("✅ 成功创建市场日度统计视图: cn_market_daily_stats")
        
        # 创建股票基础信息物化视图
        mv_stock_info_sql = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS cn_stock_basic_info
        ENGINE = ReplacingMergeTree()
        ORDER BY code
        AS SELECT
            code,
            market,
            max(date) as last_trading_date,
            min(date) as first_trading_date,
            count() as total_trading_days
        FROM cn_stock_history
        GROUP BY code, market
        """
        
        client.command(mv_stock_info_sql)
        print("✅ 成功创建股票基础信息视图: cn_stock_basic_info")
        
        print(f"""
🎉 ClickHouse表结构创建完成！

📊 设计特点：
• 单表设计，按月自动分区 (PARTITION BY toYYYYMM(date))
• 主键排序 (ORDER BY date, code) 优化时间序列查询
• LowCardinality优化字符串存储
• 物化视图加速常用统计查询

🔍 预期性能提升：
• 存储空间节省 80%+ (列式压缩)
• 查询性能提升 10-100倍
• 无需手动分表管理
• 支持复杂分析查询
        """)
        
        return True
        
    except Exception as e:
        print(f"❌ 创建ClickHouse表时发生错误: {str(e)}")
        return False
