import string
import psycopg2
import configparser
from urllib.parse import urlparse
from sql_queries import copy_table_queries, insert_table_queries

config = configparser.ConfigParser()
config.read('dwh.cfg')

def recursive_partition_query(conn, cur, query):
    alphabet_list = list(string.ascii_uppercase)
    count = 0
    
    # You can update max_partition to 0 (to load all partitions)
    max_partition = 100 

    queries = []
    for letter_p1 in alphabet_list:
        for letter_p2 in alphabet_list:
            for letter_p3 in alphabet_list:
                print(f"- Creating query for partition [{letter_p1}/{letter_p2}/{letter_p3}]")
                BUCKET = config.get('S3', 'SONG_DATA').strip("'").rstrip("/")
                queries.append(
                    query.format(
                        srcSongData = f"{BUCKET}/{letter_p1}/{letter_p2}/{letter_p3}",
                        songJsonFormat = config.get('S3', 'SONG_JSONPATH'),
                        iamRole = config.get("IAM_ROLE", "ARN").strip("'")
                    )
                )
                count += 1
                if count == max_partition:
                    break
            if count == max_partition:
                break
        if count == max_partition:
            break

    for idx, query in enumerate(queries):
        print(f"-- executing query {idx} of {max_partition}")
        cur.execute(query)
        conn.commit()
    
    
def load_staging_tables(cur, conn):
    print('---\nLoading staging tables')
    for idx, query in enumerate(copy_table_queries):
        if query.find('staging_songs') > -1:
            print(f"-- Executing query for table [staging_songs] ...")
            recursive_partition_query(conn, cur, query)
        else:
            print(f"-- Executing query for table [staging_events] ...")
            BUCKET = config.get('S3', 'LOG_DATA').strip("'").rstrip("/")
            cur.execute(query.format(
                srcLogData = f"{BUCKET}",
                logJsonFormat = config.get('S3', 'LOG_JSONPATH'),
                iamRole = config.get("IAM_ROLE", "ARN").strip("'")
            ))
            conn.commit()

    print('- Staging tables loaded.')


def insert_tables(cur, conn):
    print('---\nInserting records on tables')
    for idx, query in enumerate(insert_table_queries):
        print(f"-- Executing query {idx}: \n{query}")
        cur.execute(query)
        conn.commit()
        print("Done. \n--")

    print('- Records inserted.')


def main():
    try:
        print('---\nConnecting to AWS Redshift.')
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        print('- AWS Redshift connected.')

        # load_staging_tables(cur, conn)
        insert_tables(cur, conn)

        conn.close()

    except Exception as err:
        print(err)


if __name__ == "__main__":
    main()