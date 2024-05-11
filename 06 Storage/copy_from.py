import argparse
import psycopg2
import io
import time

def initialize():
    """ Initialize command line argument parser """
    parser = argparse.ArgumentParser(description='Load CSV data into PostgreSQL using copy_from.')
    parser.add_argument("-f", "--filename", required=True, help="CSV file to import")
    args = parser.parse_args()
    return args

def dbconnect():
    """ Establishes a connection to the PostgreSQL database with hardcoded credentials. """
    connection = psycopg2.connect(
        host="localhost",
        dbname="postgres",
        user="postgres",
        password="1234"
    )
    connection.autocommit = False  # Manage commits manually
    return connection

def ensure_table_exists(conn, tablename):
    """ Ensures that the target table exists; creates it if it does not. """
    with conn.cursor() as cursor:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {tablename} (
                CensusTract NUMERIC,
                State TEXT,
                County TEXT,
                TotalPop INTEGER,
                Men INTEGER,
                Women INTEGER,
                Hispanic DECIMAL,
                White DECIMAL,
                Black DECIMAL,
                Native DECIMAL,
                Asian DECIMAL,
                Pacific DECIMAL,
                Citizen DECIMAL,
                Income DECIMAL,
                IncomeErr DECIMAL,
                IncomePerCap DECIMAL,
                IncomePerCapErr DECIMAL,
                Poverty DECIMAL,
                ChildPoverty DECIMAL,
                Professional DECIMAL,
                Service DECIMAL,
                Office DECIMAL,
                Construction DECIMAL,
                Production DECIMAL,
                Drive DECIMAL,
                Carpool DECIMAL,
                Transit DECIMAL,
                Walk DECIMAL,
                OtherTransp DECIMAL,
                WorkAtHome DECIMAL,
                MeanCommute DECIMAL,
                Employed INTEGER,
                PrivateWork DECIMAL,
                PublicWork DECIMAL,
                SelfEmployed DECIMAL,
                FamilyWork DECIMAL,
                Unemployment DECIMAL
            );
        """)
        conn.commit()
        print(f"Ensured that the table {tablename} exists.")

def preprocess_and_load_data(conn, filename, tablename):
    """ Preprocess CSV file and load data using psycopg2's copy_from method. """
    ensure_table_exists(conn, tablename)
    with conn.cursor() as cursor:
        print(f"Loading data from {filename} into {tablename}")
        start = time.perf_counter()

        # Open and preprocess the CSV file
        with open(filename, 'r') as file:
            next(file)  # Skip the header row
            temp_file = io.StringIO()
            for line in file:
                processed_line = ','.join(['NULL' if x.strip() == '' else x.strip() for x in line.strip().split(',')])
                temp_file.write(processed_line + '\n')
            temp_file.seek(0)  # Rewind the file to the beginning

            # Load the data
            try:
                cursor.copy_from(temp_file, tablename, sep=',', null='NULL')
                conn.commit()
            except Exception as e:
                print("An error occurred during copy_from:", e)
                return

        elapsed = time.perf_counter() - start
        print(f"Finished loading. Elapsed Time: {elapsed:.4f} seconds")

def main():
    args = initialize()
    conn = dbconnect()
    tablename = "censusdatacf"  # Specify your table name here
    preprocess_and_load_data(conn, args.filename, tablename)
    conn.close()

if __name__ == "__main__":
    main()
