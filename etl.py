import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    - Parameters:
        - cur: Cursror for redshift cluster
        - conn: Connection object for redshift cluster

    - Iterate over sql commands to copy
    - the data stored in s3 buckets to redshift

    - Returns None
    """
    for query in copy_table_queries:
        try:
            print("*******************")
            print("Executing:")
            print(query)
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)


def insert_tables(cur, conn):
    """
    - Parameters:
        - cur: Cursror for redshift cluster
        - conn: Connection object for redshift cluster

    - Iterate over sql commands to create fact table and dimension tables
    - from the data stored in redshift

    - Returns None
    """
    for query in insert_table_queries:
        try:
            print("*******************")
            print("Executing:")
            print(query)
            cur.execute(query)
            conn.commit()
            print('Executing:')
        except Exception as e:
            print(e)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn = psycopg2.connect(
            "host={} dbname={} user={} password={} port={}".format(
                *config['CLUSTER'].values()))
        print("Connected Successfully....")
        print("*******************")
        cur = conn.cursor()

        load_staging_tables(cur, conn)
        insert_tables(cur, conn)

        conn.close()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()
