import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    print('---\nDropping tables')
    for idx, query in enumerate(drop_table_queries):
        print(f"-- dropping table {idx}")
        cur.execute(query)
        conn.commit()
    print('- Tables dropped')


def create_tables(cur, conn):
    print('---\nCreating tables')
    for idx, query in enumerate(create_table_queries):
        print(f"-- creating table {idx}")
        cur.execute(query)
        conn.commit()
    print('- Tables created')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('---\nConnecting to AWS Redshift.')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('- AWS Redshift connected.')

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print('---\nFinished.')

if __name__ == "__main__":
    main()