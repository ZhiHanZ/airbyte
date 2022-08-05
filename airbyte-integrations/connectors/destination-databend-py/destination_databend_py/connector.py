import logging
from clickhouse_sqlalchemy import make_session
from sqlalchemy import create_engine

default_database = "default"


class ClickhouseConnector():

    def connect(self,
                logger,
                host,
                port,
                username="root",
                password="",
                database=default_database):
        self._uri = f"clickhouse+http://{username}:{password}@{host}:{port}/{database}?protocol=https"
        logger.debug(self._uri)
        self._additonal_headers = dict()
        self.logger = logger
        self._session = None

    def query_with_session(self, statement):

        def parseSQL(sql):
            # for cases like:
            # SELECT parse_json('"false"')::boolean;          => SELECT parse_json('\"false\"')::boolean;
            # select as_object(parse_json('{"a":"b"}'));      => select as_object(parse_json('{\"a\":\"b\"}'));
            # https://stackoverflow.com/questions/49902843/avoid-parameter-binding-when-executing-query-with-sqlalchemy/49913328#49913328
            if '"' in sql:
                if '\'' in sql:
                    return sql.replace('"', '\\\"').replace(
                        ':', '\\:')  #  "  -> \"   : ->  \\:
                return sql.replace('"', '\'')
            else:
                return sql  #  do nothing

        if self._session is None:
            engine = create_engine(self._uri,
                                   connect_args={'sslmode': "none", "ssl": "on"})
            self._session = make_session(engine)
        self.logger.info(parseSQL(statement))
        
        return self._session.execute(parseSQL(statement))

    def reset_session(self):
        if self._session is not None:
            self._session.close()
            self._session = None

    def fetch_all(self, statement):
        cursor = self.query_with_session(statement)
        data_list = list()
        for item in cursor.fetchall():
            data_list.append(list(item))
        cursor.close()
        return data_list


# if __name__ == '__main__':
#     from config import clickhouse_config
#     connector = ClickhouseConnector()
#     connector.connect(**clickhouse_config)
#     print(connector.fetch_all("show databases"))
#     print(connector.fetch_all("select * from t1"))
