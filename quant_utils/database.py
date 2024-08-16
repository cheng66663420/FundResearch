# -*- coding: utf-8 -*-


import logging
from urllib.parse import quote

import numpy as np
import pandas as pd
import pangres
from sqlalchemy import create_engine, text

from quant_utils import constant

logging.basicConfig(
    filename=constant.LOG_FILE_PATH,
    level=logging.INFO,
    format=constant.LOG_FORMAT,
)


class MySQL:
    """
    MySQL数据库操作封装
    """

    def __init__(
        self,
        host: str,
        user: str,
        pwd: str,
        database: str,
        port: int,
        charset: str = "utf8",
    ):
        self.host = quote(host)
        self.user = quote(user)
        self.pwd = quote(pwd)
        self.database = quote(database)
        self.charset = charset
        self.port = int(port)
        self.engine = self._create_engine()

    def _create_engine(self):
        # MySQL 连接字符串
        connection_string = f"mysql+pymysql://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.database}?charset={self.charset}"
        # 创建引擎并配置连接池
        return create_engine(
            connection_string, pool_size=5, max_overflow=10, pool_timeout=30
        )

    def exec_query(self, query: str) -> pd.DataFrame:
        """
        执行查询语句

        Parameters
        ----------
        query : str
            查询语句

        Returns
        -------
        pd.DataFrame
            查询结果
        """
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
        return pd.DataFrame(result.fetchall(), columns=result.keys())

    def exec_non_query(self, query: str) -> None:
        """
        执行非查询语句

        Parameters
        ----------
        query : str
            非查询语句
        """
        with self.engine.connect() as connection:
            result = connection.execute(text(query))
            affected_rows = result.rowcount
            logging.info("受影响行数为%d行", affected_rows)
            connection.commit()

    def upsert(
        self, df_to_upsert: pd.DataFrame, table: str, batch_size: int = 10000
    ) -> None:
        """
        批量插入数据到MySQL数据库

        Parameters
        ----------
        df_to_upsert : pd.DataFrame
            需要插入的数据
        table : str
            表名
        batch_size : int, optional
            每次插入的行数, by default 10000
        """
        if df_to_upsert.empty:
            logging.info("数据为空,不需要写入")
            return

        df = df_to_upsert.copy()
        df = df.replace(
            {np.nan: None, np.inf: None, -np.inf: None, "inf": None, "-inf": None}
        )

        def _split_dataframe(df: pd.DataFrame, batch_size: int):
            total_rows = df.shape[0]
            num_chunks = (total_rows + batch_size - 1) // batch_size
            for i in range(num_chunks):
                start = i * batch_size
                end = min(start + batch_size, total_rows)
                yield df.iloc[start:end]

        def _create_upsert_sql(table: str, df: pd.DataFrame) -> str:
            columns = ", ".join(df.columns)
            placeholders = ", ".join([f":{col}" for col in df.columns])
            updates = ", ".join([f"{col} = values({col})" for col in df.columns])
            return f"""
            INSERT INTO {table} ({columns}) 
            VALUES ({placeholders}) 
            ON DUPLICATE KEY UPDATE {updates}
            """

        affected_rows_total = 0
        with self.engine.connect() as connection:
            for chunk in _split_dataframe(df, batch_size):
                sql = _create_upsert_sql(table, chunk)
                try:
                    result = connection.execute(
                        text(sql), chunk.to_dict(orient="records")
                    )
                    affected_rows_total += result.rowcount
                    connection.commit()
                except Exception as e:
                    logging.error("Error during upsert: %s", e)
                    connection.rollback()
        logging.info("%s: 更新插入%d 行数据", table, affected_rows_total)


class Postgres:
    """
    PostgreSQL 数据库操作类
    """

    def __init__(self, host, user, pwd, database, port):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.database = database
        self.port = port
        self.engine = self._create_engine()

    def _create_engine(self):
        """
        创建数据库引擎
        """
        return create_engine(
            f"postgresql://{self.user}:{self.pwd}@{self.host}:{self.port}/{self.database}"
        )

    def exec_query(self, query, params=None):
        """
        执行查询并返回结果
        """
        with self.engine.connect() as connection:
            result = connection.execute(text(query), params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return df  # 返回所有结果

    def exec_nonquery(self, query, params=None):
        """
        执行非查询操作（如 INSERT, UPDATE, DELETE)
        """
        with self.engine.connect() as connection:
            connection.execute(text(query), params)
            connection.commit()  # 提交事务

    def upsert(self, df, table_name):
        """
        将 DataFrame 插入到 PostgreSQL 表中，支持 upsert 操作,
        df的index必须为插入表的唯一索引
        """
        self._create_engine()
        try:
            pangres.upsert(df, self.engine, table_name, if_row_exists="update")
            print(f"成功将数据插入到 {self.database} 的 {table_name} 表中!")
        except Exception as e:
            print(e)
            logging.info(e)
