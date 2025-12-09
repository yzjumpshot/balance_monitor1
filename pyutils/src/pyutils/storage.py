from abc import ABC, abstractmethod
from typing import Sequence, Any, Union, Iterable, Optional

import pandas as pd
import clickhouse_connect
from clickhouse_connect.driver import httputil
from clickhouse_connect.driver.summary import QuerySummary
from clickhouse_connect.driver.query import QueryResult
from clickhouse_connect.driver.asyncclient import AsyncClient


class BaseClient:
    """BaseClient是一个基础Client类,用于与ClickHouse数据库进行交互,提供一个共享的连接池."""

    _pool = httputil.get_pool_manager(maxsize=16, num_pools=10)

    def __init__(self, **kwargs):
        self._raw_client = clickhouse_connect.get_client(**kwargs, pool_mgr=self._pool, autogenerate_session_id=False)
        self._client = AsyncClient(client=self._raw_client)


class Querier(BaseClient):
    """Querier是一个通用的查询器,用于执行ClickHouse数据库的查询和数据操作."""

    async def query(self, sql: str, **kwargs) -> QueryResult:
        """执行查询并返回结果.

        Args:
            sql (str): 要执行的SQL查询.

        Returns:
            QueryResult: 查询结果
        """

        return await self._client.query(query=sql, **kwargs)

    async def query_df(self, sql: str, **kwargs) -> pd.DataFrame:
        """执行查询并返回DataFrame.

        Args:
            sql (str): 要执行的SQL查询.

        Returns:
            pd.DataFrame: 查询结果
        """

        return await self._client.query_df(query=sql, **kwargs)


class BaseStorage(ABC, Querier):
    """BaseStorage是一个抽象基类,用于定义ClickHouse存储的基本操作."""

    @property
    @abstractmethod
    def db(self) -> str:
        raise NotImplementedError("Subclasses must implement the 'db' property")

    @property
    @abstractmethod
    def table(self) -> str:
        raise NotImplementedError("Subclasses must implement the 'table' property")

    @property
    def full_table_name(self):
        return f"{self.db}.{self.table}"

    @abstractmethod
    async def create_table(self) -> QuerySummary:
        """创建表格.

        Returns:
            QuerySummary: 创建表格的结果摘要.
        """
        raise NotImplementedError("Subclasses must implement the 'create_table' method")

    async def get_ddl_sql(self) -> str:
        """获取建表语句.

        Returns:
            str: 建表语句
        """
        sql = f"SHOW CREATE TABLE {self.full_table_name}"
        result = await self.query(sql)
        ddl_sql = result.first_row[0]
        return ddl_sql

    async def get_table_info(self) -> pd.DataFrame:
        """获取表的元数据信息."""

        sql = f"""
        SELECT
            database,
            name,
            uuid,
            engine,
            data_paths,
            metadata_path,
            create_table_query,
            partition_key,
            sorting_key,
            primary_key,
            total_rows,
            formatReadableSize(total_bytes) AS total_bytes,
            formatReadableSize(total_bytes_uncompressed) AS total_bytes_uncompressed,
            parts,
            active_parts,
            total_marks,
            comment
        FROM
            system.tables
        WHERE database = %(database)s AND name = %(table)s
        """
        df = await self.query_df(sql, parameters={"database": self.db, "table": self.table})
        return df

    async def get_part_info(self) -> pd.DataFrame:
        """获取表的part信息."""

        sql = f"""
        SELECT
            name as part_name,
            partition,
            part_type,
            path,
            formatReadableQuantity(rows) AS rows,
            formatReadableSize(data_uncompressed_bytes) AS data_uncompressed_bytes,
            formatReadableSize(data_compressed_bytes) AS data_compressed_bytes,
            formatReadableSize(primary_key_bytes_in_memory) AS primary_key_bytes_in_memory,
            marks,
            formatReadableSize(bytes_on_disk) AS bytes_on_disk,
            modification_time,
            disk_name,
            path
        FROM system.parts
        WHERE database = %(database)s AND table = %(table)s AND active = 1
        """

        df = await self.query_df(sql, parameters={"database": self.db, "table": self.table})
        return df

    async def get_column_info(self) -> pd.DataFrame:
        """获取表的列信息."""

        sql = f"""
        SELECT name as column, database, table,
            formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
            formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
            round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 2) AS ratio
        FROM system.columns
        WHERE database = %(database)s AND table = %(table)s
        GROUP BY database, table, name
        """

        df = await self.query_df(sql, parameters={"database": self.db, "table": self.table})
        return df

    async def insert(
        self, data: Sequence[Sequence[Any]], column_names: Union[str, Iterable[str]] = "*", **kwargs
    ) -> QuerySummary | None:
        """插入数据.

        Args:
            data (Sequence[Sequence[Any]]): 要插入的数据.
            column_names (Union[str, Iterable[str]], optional): 要插入的列名. Defaults to "*".

        Returns:
            QuerySummary | None: 如果没有任何数据插入, 则返回None, 否则返回QuerySummary对象.
        """

        if not data:
            return

        return await self._client.insert(
            table=self.full_table_name, data=data, column_names=column_names, database=self.db, **kwargs
        )

    async def insert_df(
        self, df: pd.DataFrame, column_names: Optional[Sequence[str]] = None, **kwargs
    ) -> QuerySummary | None:
        """插入DataFrame数据.

        Args:
            df (pd.DataFrame): 要插入的数据.
            column_names (Optional[Sequence[str]], optional): 要插入的列名. Defaults to None.

        Returns:
            QuerySummary | None: 如果没有任何数据插入, 则返回None, 否则返回QuerySummary对象.
        """

        if df.empty:
            return

        return await self._client.insert_df(
            table=self.full_table_name, df=df, database=self.db, column_names=column_names, **kwargs
        )
