import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
        Description: This function can be used to drop tables if they already exists drop_table_queries from sql_queries.py.

        Arguments:
            cur: cursor
            conn: connection to the database

        Returns:
            None
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
        Description: This function can be used to create tables using the create_table_queries from sql_queries.py.

        Arguments:
            cur: cursor
            conn: connection to the database

        Returns:
            None
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')    

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()    
    
    # create and set the schema for the new tables
    schema_query = '''
                    CREATE SCHEMA IF NOT EXISTS dwh;
                    SET search_path TO dwh
                   '''
    cur.execute(schema_query)
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()