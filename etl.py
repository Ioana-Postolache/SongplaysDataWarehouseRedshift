import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query+'.format('+DWH_ROLE_ARN+')')
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    DWH_ROLE_ARN               = config.get("IAM_ROLE","ARN")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn, DWH_ROLE_ARN)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
    