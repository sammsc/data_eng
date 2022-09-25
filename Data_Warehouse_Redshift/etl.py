import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Load log_data and song_data files into staging tables
            Parameters:
                    cur: database connection cursor
                    conn: connection to database
            Returns:
                    None
    '''

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Load and transform data from staging tables into fact and dimension tables
            Parameters:
                    cur: database connection cursor
                    conn: connection to database
            Returns:
                    None
    '''

    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Loads files into staging tables, and transforms them into fact and dimension tables. 
    
    - Establishes connection with the sparkify database and gets cursor to it.  
    
    - Loads log_data and song_data into staging tables.  
    
    - Transforms them into fact and dimension tables. 
    
    - Finally, closes the connection. 
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()