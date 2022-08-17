import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load data into staging tables
    :param cur: cursor of the database 
    :param conn: connection to the database
    """
    for query in copy_table_queries:
        print(f"Executing ... {query}")
        cur.execute(query)
        conn.commit()
        print()


def insert_tables(cur, conn):
    """Insert data into tables
    :param cur: cursor of the database 
    :param conn: connection to the database
    """
    for query in insert_table_queries:
        print(f"Executing ... {query}")
        cur.execute(query)
        conn.commit()
        print()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print(f"Connected: {conn}")
    print()
    
    print("Loading staging tables ...")
    load_staging_tables(cur, conn)
    print()
    
    print("Inserting data into tables ...")
    insert_tables(cur, conn)
    print()

    print("Closing connection ...")
    conn.close()


if __name__ == "__main__":
    main()