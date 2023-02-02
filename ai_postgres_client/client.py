"""
File contains description of class PostgresClient.
"""

import os
import logging
from typing import Any, List
from functools import wraps

from psycopg import connect, Error
from psycopg.rows import dict_row


def checkconn(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.client:
            return func(self, *args, **kwargs)
        else:
            raise ValueError("No connection to PostgresClient! Use .connect() before!")

    return wrapper


class PostgresClient:
    """
    A class for working with PostgreSQL.
    """

    def __init__(self) -> None:
        self.client = None

    def _connect_p(self):
        try:
            self.client = connect(
                user=os.environ.get("POSTGRES_USER"),
                host=os.environ.get("POSTGRES_HOST"),
                port=os.environ.get("POSTGRES_PORT"),
                password=os.environ.get("POSTGRES_PASS"),
                # attempt to connect for 3 seconds then raise exception
                connect_timeout=3,
                autocommit=True,
                row_factory=dict_row,
            )
            logging.info("Connected to PostgreSQL.")
        except Exception as e:
            logging.error("Cant connect to PostgeSQL!")
            logging.error(f"{e}")

    def _connect_db(self):
        dbname = os.environ.get("POSTGRES_DBNAME")
        try:
            self.client = connect(
                dbname=dbname,
                user=os.environ.get("POSTGRES_USER"),
                host=os.environ.get("POSTGRES_HOST"),
                port=os.environ.get("POSTGRES_PORT"),
                password=os.environ.get("POSTGRES_PASS"),
                # attempt to connect for 3 seconds then raise exception
                connect_timeout=3,
                row_factory=dict_row,
            )
            logging.info(f"Connected to PostgreSQL database. (dbname={dbname})")
        except Exception as e:
            logging.error(f"Cant connect to PostgeSQL database! (dbname={dbname})")
            logging.error(f"{e}")

    def connect(self):
        self._connect_p()

        dbname = os.environ.get("POSTGRES_DBNAME")

        if not self.check_db_exist(dbname):
            self.create_db(dbname)

        self.close()
        self._connect_db()

    @checkconn
    def close(self):
        try:
            if self.client:
                self.client.close()
                logging.info("Disconnected from PostgreSQL.")
        except (Exception, Error) as e:
            logging.error("psycopg disconnect error!")
            logging.error(f"{e}")

    @checkconn
    def get(self, sql_command: str, return_err: bool = False):
        try:
            cursor = self.client.cursor()
            cursor.execute(sql_command)
            result = list(cursor)
            try:
                cursor.close()
            except Exception as e:
                logging.error("Cant close cursor!")
                logging.error(f"{e}")

            self.client.commit()
            return result

        except (Exception, Error) as e:
            try:
                cursor.close()
            except Exception as e:
                logging.error("Cant close cursor!")
                logging.error(f"{e}")

            self.client.commit()
            logging.error(f"{e}")
            return None if not return_err else {"error": f"{e}"}

    @checkconn
    def execute(self, sql_command: str, return_err: bool = False):
        try:
            cursor = self.client.cursor()
            cursor.execute(sql_command)
            try:
                cursor.close()
            except Exception as e:
                logging.error("Cant close cursor!")
                logging.error(f"{e}")

            self.client.commit()
            return True

        except (Exception, Error) as e:
            try:
                cursor.close()
            except Exception as e:
                logging.error("Cant close cursor!")
                logging.error(f"{e}")

            self.client.commit()
            logging.error(f"{e}")
            return False if not return_err else {"error": f"{e}"}

    def check_db_exist(self, dbname: str):
        sql_command = f"SELECT FROM pg_database WHERE datname = '{dbname}'"
        return self.get(sql_command)

    def get_row(
        self, table_name: str, idx_column: str, idx_val: Any, return_err: bool = False
    ):
        if type(idx_val) == str:
            idx_val = f"'{idx_val}'"
        sql_command = f"""
        SELECT *
        FROM {table_name}
        WHERE {idx_column} = {idx_val};
        """
        result = self.get(sql_command, return_err=return_err)
        if result and not (type(result) == dict and "error" not in result):
            return result[0]
        else:
            return result

    def get_table(
        self,
        table_name: str,
        columns: str = "*",
        add_where: str = None,
        return_err: bool = False,
    ):
        if columns != "*":
            columns = self._get_select_table_columns(table_name, columns)
        sql_command = f"""
        SELECT {columns}
        FROM {table_name} {add_where if add_where else ""};
        """
        return self.get(sql_command, return_err=return_err)

    def _get_select_table_columns(
        self,
        table: str,
        columns: str = None,
    ):
        if columns:
            table = ", ".join([f"{table}.{column}" for column in columns])
        else:
            table = f"{table}.*"
        return table

    def get_two_tables(
        self,
        table_one: str,
        key_one: str,
        table_two: str,
        key_two: str = None,
        columns_one: str = None,
        columns_two: str = None,
        add_where: str = None,
        order_by: str = None,
        return_err: bool = False,
    ):
        """
        f'ORDER BY {call.start_time ASC};'
        """
        if not key_two:
            key_two = key_one

        select_columns_one = self._get_select_table_columns(table_one, columns_one)
        select_columns_two = self._get_select_table_columns(table_two, columns_two)

        sql_command = f"""
        SELECT {select_columns_one}, {select_columns_two}
        FROM {table_one}
        INNER JOIN {table_two} ON {table_two}.{key_two} = {table_one}.{key_one}
        """
        if add_where:
            sql_command += f"""
        WHERE {add_where}
        """
        if order_by:
            sql_command += f"ORDER BY {order_by};"
        else:
            sql_command += ";"

        return self.get(sql_command, return_err=return_err)

    def get_tables(self):
        sql_command = r"SELECT relname from pg_class WHERE relkind='r' and relname !~ '^(pg_|sql_)';"
        tables = self.get(sql_command)
        if not tables:
            tables = []
        return set([table["relname"] for table in tables])

    def create_table(self, table_name: str, columns: str):
        if table_name not in self.get_tables():
            create_table_query = f"""CREATE TABLE {table_name} (
                                        {columns}
                                    );"""
            result = self.execute(create_table_query)
            if result:
                logging.info(
                    f"Table created (dbname:{os.environ.get('POSTGRES_DBNAME')} table:{table_name})"
                )
            else:
                logging.error(
                    f"Cant create table! (dbname:{os.environ.get('POSTGRES_DBNAME')} table:{table_name})"
                )
            return result
        else:
            logging.info(
                f"Table already exist! (dbname:{os.environ.get('POSTGRES_DBNAME')} table:{table_name})"
            )
            return True

    def create_db(self, dbname: str):
        sql_create_database = f"create database {dbname}"
        result = self.execute(sql_create_database)
        if result:
            logging.info(f"Database created (dbname:{dbname})")
        else:
            logging.error(f"Cant create database! (dbname:{dbname})")
        return result

    def remove_table(self, table_name: str):
        create_table_query = f"""DROP TABLE IF EXISTS {table_name};"""
        result = self.execute(create_table_query)
        if result:
            logging.info(
                f"Table removed (dbname:{os.environ.get('POSTGRES_DBNAME')} table:{table_name})"
            )
        else:
            logging.error(
                f"Cant remove table! (dbname:{os.environ.get('POSTGRES_DBNAME')} table:{table_name})"
            )
        return result

    def add_row(self, table_name: str, row: List[Any], return_err: bool = False):
        row = [
            "null"
            if value == None
            else f"{value}"
            if type(value) != str
            else f"'{value}'"
            for value in row
        ]
        sql_command = f"""
        INSERT INTO {table_name} VALUES
        ({", ".join(row)});
        """
        return self.execute(sql_command, return_err=return_err)

    def remove_row(
        self, table_name: str, column: str, value: Any, return_err: bool = False
    ):
        if type(value) == str:
            value = f"'{value}'"
        sql_command = f"""
        DELETE FROM {table_name}
        WHERE {column} = {value};
        """
        return self.execute(sql_command, return_err=return_err)

    def find_value(self, table_name: str, column: str, value: Any):
        if type(value) == str:
            value = f"'{value}'"

        sql_command = f"SELECT {column} FROM {table_name} WHERE {column}={value}"
        result = self.get(sql_command)

        if result:
            return True
        else:
            return False

    def get_value(
        self, table_name: str, idx_column: str, idx_value: Any, search_column: str
    ):
        if type(idx_value) == str:
            idx_value = f"'{idx_value}'"

        sql_command = f"""
        SELECT {search_column} from {table_name} WHERE {idx_column} = {idx_value};
        """
        result = self.get(sql_command)

        if result:
            return result[0][search_column]
        else:
            return None

    def update_value(
        self,
        table_name: str,
        idx_column: str,
        idx_val: Any,
        val_column: str,
        val: Any,
        return_err: bool = False,
    ):
        if type(idx_val) == str:
            idx_val = f"'{idx_val}'"
        if type(val) == str:
            val = f"'{val}'"

        sql_command = f"""UPDATE {table_name}
            SET {val_column} = {val}
            WHERE {idx_column} = {idx_val};"""

        return self.execute(sql_command, return_err=return_err)

    def check_table_is_empty(self, table_name: str) -> bool:
        sql_command = f"""
        SELECT CASE WHEN EXISTS(SELECT 1 FROM {table_name}) THEN FALSE ELSE TRUE END AS IsEmpty;
        """
        return self.get(sql_command)[0]["isempty"]

    def get_sorted_table(
        self,
        table_name: str,
        idx_column: str,
        val_column: str,
        return_column: str = None,
        add_where: str = None,
    ):
        if not return_column:
            return_column = "*"
        sql_command = f"""
        SELECT tbl.{return_column}
        FROM {table_name} tbl
        INNER JOIN
        (
            SELECT *, MIN({val_column}) MinVal
            FROM {table_name}
            {"" if not add_where else add_where}
            GROUP BY {idx_column}
        ) tbl1
        ON tbl1.{idx_column} = tbl.{idx_column}
        WHERE tbl1.MinVal = tbl.{val_column}
        """
        sorted_table = self.get(sql_command)
        if sorted_table:
            return sorted_table
        else:
            return None

    def get_first_min_row(
        self,
        table_name: str,
        idx_column: str,
        val_column: str,
        return_column: str = None,
        add_where: str = None,
    ):
        result = self.get_sorted_table(
            table_name,
            idx_column,
            val_column,
            return_column=return_column,
            add_where=add_where,
        )
        if result:
            return result[0]
        else:
            return None

    def get_first_max_row(
        self,
        table_name: str,
        idx_column: str,
        val_column: str,
        return_column: str = None,
        add_where: str = None,
    ):
        result = self.get_sorted_table(
            table_name,
            idx_column,
            val_column,
            return_column=return_column,
            add_where=add_where,
        )
        if result:
            return result[-1]
        else:
            return None
