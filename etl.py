import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn, IAM_ROLE, S3_BUCKET):
    '''
        Description: This function can be used to load the staging tables from S3.

        Arguments:
            cur: cursor
            conn: connection to the database
            IAM_ROLE: ARN IAM role
            S3_BUCKET: S3 bucket where the data resides

        Returns:
            None
    '''
    for query in copy_table_queries:
        command = query.format(S3_BUCKET, IAM_ROLE)
        print('executing query: ', command)
        cur.execute(command)
        conn.commit()


def insert_tables(cur, conn):
    '''
         Description: This function can be used to insert data into the newly created tables from the staging tables.

        Arguments:
            cur: cursor
            conn: connection to the database

        Returns:
            None
    '''
    for query in insert_table_queries:
        print('executing query: ', query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    IAM_ROLE               = config.get("IAM_ROLE","ARN")
    S3_BUCKET              = config.get("S3","S3_PERSONAL_BUCKET")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    schema_query = '''
                    SET search_path TO dwh
                   '''
    cur.execute(schema_query)
    
    load_staging_tables(cur, conn, IAM_ROLE, S3_BUCKET)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
    