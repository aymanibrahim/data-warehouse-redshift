import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop tables
    :param cur: cursor of the database 
    :param conn: connection to the database
    """
    for query in drop_table_queries:
        print(f"Executing ... {query}")
        cur.execute(query)
        conn.commit()
        print()


def create_tables(cur, conn):
    """Create tables
    :param cur: cursor of the database 
    :param conn: connection to the database
    """
    for query in create_table_queries:
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

    print("Dropping tables ...")
    drop_tables(cur, conn)
    print()
    
    print("Creating tables ...")
    create_tables(cur, conn)
    print()

    print("Closing connection ...")
    conn.close()
    


if __name__ == "__main__":
    main()