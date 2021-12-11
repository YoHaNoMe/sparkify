import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    - Parameters:
        - cur: Cursror for redshift cluster
        - conn: Connection object for redshift cluster

    - Drop all the tables in redshift cluster

    - Returns: None
    """
    for query in drop_table_queries:
        try:
            print("*******************")
            print('Executing query:')
            print(query)
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)


def create_tables(cur, conn):
    """
    - Parameters:
        - cur: Cursror for redshift cluster
        - conn: Connection object for redshift cluster

    - Creates all the tables in redshift cluster

    - Returns: None
    """
    for query in create_table_queries:
        try:
            print("*******************")
            print('Executing query:')
            print(query)
            cur.execute(query)
            conn.commit()
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
        drop_tables(cur, conn)
        create_tables(cur, conn)
        conn.close()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()
