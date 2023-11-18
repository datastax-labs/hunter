from dataclasses import dataclass

import psycopg2


@dataclass
class PostgresConfig:
    hostname: str
    port: int
    username: str
    password: str
    database: str


class Postgres:
    __conn = None
    __config = None

    def __init__(self, config: PostgresConfig):
        self.__config = config

    def __get_conn(self) -> psycopg2.extensions.connection:
        if self.__conn is None:
            self.__conn = psycopg2.connect(
                host=self.__config.hostname,
                port=self.__config.port,
                user=self.__config.username,
                password=self.__config.password,
                database=self.__config.database,
            )
        return self.__conn

    def fetch_data(self, query: str):
        cursor = self.__get_conn().cursor()
        cursor.execute(query)
        columns = [c.name for c in cursor.description]
        return (columns, cursor.fetchall())
