# -*- coding: utf-8 -*-

import logging

import numpy as np
import pandas as pd
import pangres
from sqlalchemy import create_engine, text, inspect, MetaData, Table
from quant_utils import constant
from sqlalchemy.dialects.mysql import insert
import polars as pl
from quant_utils.utils import display_time

logging.basicConfig(
    filename=constant.LOG_FILE_PATH,
    level=logging.INFO,
    format=constant.LOG_FORMAT,
)
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
        uri: str,
    ):
        self.uri = uri
        self.engine = self._create_engine()

    def _create_engine(self):
        # MySQL 连接字符串
        # 创建引擎并配置连接池
        return create_engine(self.uri, pool_size=5, max_overflow=10, pool_timeout=30)

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
        return pl.read_database_uri(query, self.uri).to_pandas()

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

    def __upsert_sql(
        self, table: Table, df: pd.DataFrame, unique_constraint: list[str]
    ):
        insert_stmt = insert(table).values(df.to_dict(orient="records"))
        upsert_dict = {
            col: insert_stmt.inserted[col]
            for col in df.columns
            if col not in unique_constraint
        } or {col: insert_stmt.inserted[col] for col in df.columns}
        return insert_stmt.on_duplicate_key_update(upsert_dict)

    def get_db_table_unque_index(self, table_name: str) -> list[list[str]]:
        # 创建数据库引擎
        # 创建元数据对象
        inspector = inspect(self.engine)
        # 获取指定表的索引列表
        indexes = inspector.get_indexes(table_name)
        return [index["column_names"] for index in indexes if index["unique"] is True]

    def get_unique_constraint(self, table: str) -> list[str]:
        """
        获取表的主键或唯一索引

        Parameters
        ----------
        table : str
            表名

        Returns
        -------
        list[str]
            主键或唯一索引列名列表
        """
        unique_constraint = []
        unique_indexes = self.get_db_table_unque_index(table)
        if not unique_indexes:
            return unique_constraint

        for index in unique_indexes:
            if "ID" in index:
                continue
            if "JSID" in index:
                unique_constraint = ["JSID"]
                break
            unique_constraint = index
            break
        return unique_constraint

    @display_time()
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

        def _split_dataframe(df: pd.DataFrame, batch_size: int):
            total_rows = df.shape[0]
            num_chunks = (total_rows + batch_size - 1) // batch_size
            for i in range(num_chunks):
                start = i * batch_size
                end = min(start + batch_size, total_rows)
                yield df.iloc[start:end]

        if df_to_upsert.empty:
            logging.info("数据为空,不需要写入")
            return

        df = df_to_upsert.copy()
        df = df.replace(
            {np.nan: None, np.inf: None, -np.inf: None, "inf": None, "-inf": None}
        )

        affected_rows_total = 0
        sql_table = Table(table, MetaData(), autoload_with=self.engine)
        unique_constraint = self.get_unique_constraint(table)
        for df_chunk in _split_dataframe(df, batch_size):
            on_duplicate_key_stmt = self.__upsert_sql(
                sql_table, df_chunk, unique_constraint
            )
            with self.engine.connect() as connection:
                try:
                    result = connection.execute(on_duplicate_key_stmt)
                    affected_rows_total += result.rowcount
                    connection.commit()
                except Exception as e:
                    print(f"{table}插入数据失败,错误信息为{e}")
                    connection.rollback()
        print(f"{table}更新插入{affected_rows_total}行数据")

    # def upsert(
    #     self, df_to_upsert: pd.DataFrame, table: str, batch_size: int = 50000
    # ) -> None:
    #     """
    #     批量插入数据到MySQL数据库

    #     Parameters
    #     ----------
    #     df_to_upsert : pd.DataFrame
    #         需要插入的数据
    #     table : str
    #         表名
    #     batch_size : int, optional
    #         每次插入的行数, by default 50000
    #     """
    #     if df_to_upsert.empty:
    #         logging.info("数据为空,不需要写入")
    #         return

    #     df = df_to_upsert.copy()
    #     df = df.replace(
    #         {np.nan: None, np.inf: None, -np.inf: None, "inf": None, "-inf": None}
    #     )

    #     def _create_upsert_sql(table: str, df: pd.DataFrame) -> str:
    #         columns = ", ".join([f"`{col}`" for col in df.columns])
    #         placeholders = ", ".join([f":{col}" for col in df.columns])
    #         updates = ", ".join([f"`{col}` = values(`{col}`)" for col in df.columns])
    #         return f"""
    #         INSERT INTO {table} ({columns})
    #         VALUES ({placeholders})
    #         ON DUPLICATE KEY UPDATE {updates}
    #         """

    #     affected_rows_total = 0
    #     with self.engine.connect() as connection:
    #         for chunk in _split_dataframe(df, batch_size):
    #             sql = _create_upsert_sql(table, chunk)
    #             try:
    #                 result = connection.execute(
    #                     text(sql), chunk.to_dict(orient="records")
    #                 )
    #                 affected_rows_total += result.rowcount
    #                 connection.commit()
    #             except Exception as e:
    #                 logging.error("Error during upsert: %s", e)
    #                 print(e)
    #                 connection.rollback()
    #     logging.info("%s: 更新插入%d 行数据", table, affected_rows_total)


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
            return pd.DataFrame(result.fetchall(), columns=result.keys())

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
