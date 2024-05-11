import time
import psycopg2
import argparse
import csv

DBname = "postgres"
DBuser = "postgres"
DBpwd = "1234"
TableName = 'CensusDataI'
Datafile = "filedoesnotexist"  # Ensure this points to the actual data file

def escape_quotes(value):
    """Helper function to escape single quotes in SQL string values."""
    return value.replace("'", "''")

def row2vals(row):
    """Converts a dictionary row into a comma-separated string of SQL values, handling nulls and escaping characters."""
    for key in row:
        if not row[key]:
            row[key] = 'NULL'  # Handle null values appropriately for SQL
        else:
            if isinstance(row[key], str):
                row[key] = escape_quotes(row[key])  # Escape single quotes for SQL strings

    ret = f"""
       {row['CensusTract']}, 
       '{row['State']}',
       '{row['County']}',
       {row['TotalPop']},
       {row['Men']},
       {row['Women']},
       {row['Hispanic']},
       {row['White']},
       {row['Black']},
       {row['Native']},
       {row['Asian']},
       {row['Pacific']},
       {row['Citizen']},
       {row['Income']},
       {row['IncomeErr']},
       {row['IncomePerCap']},
       {row['IncomePerCapErr']},
       {row['Poverty']},
       {row['ChildPoverty']},
       {row['Professional']},
       {row['Service']},
       {row['Office']},
       {row['Construction']},
       {row['Production']},
       {row['Drive']},
       {row['Carpool']},
       {row['Transit']},
       {row['Walk']},
       {row['OtherTransp']},
       {row['WorkAtHome']},
       {row['MeanCommute']},
       {row['Employed']},
       {row['PrivateWork']},
       {row['PublicWork']},
       {row['SelfEmployed']},
       {row['FamilyWork']},
       {row['Unemployment']}
    """
    return ret

def initialize():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--datafile", required=True)
    parser.add_argument("-c", "--createtable", action="store_true")
    args = parser.parse_args()

    global Datafile, CreateDB
    Datafile = args.datafile
    CreateDB = args.createtable

def readdata(fname):
    print(f"Reading data from file: {fname}")
    with open(fname, mode="r") as fil:
        dr = csv.DictReader(fil)
        return [row for row in dr]

def getSQLcmnds(rowlist):
    return [f"INSERT INTO {TableName} VALUES ({row2vals(row)});" for row in rowlist]

def dbconnect():
    return psycopg2.connect(host="localhost", database=DBname, user=DBuser, password=DBpwd, autocommit=True)

def createTable(conn):
    with conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS {TableName};")
        cursor.execute(f"""
            CREATE TABLE {TableName} (
                CensusTract         NUMERIC,
                State               TEXT,
                County              TEXT,
                TotalPop            INTEGER,
                Men                 INTEGER,
                Women               INTEGER,
                Hispanic            DECIMAL,
                White               DECIMAL,
                Black               DECIMAL,
                Native              DECIMAL,
                Asian               DECIMAL,
                Pacific             DECIMAL,
                Citizen             DECIMAL,
                Income              DECIMAL,
                IncomeErr           DECIMAL,
                IncomePerCap        DECIMAL,
                IncomePerCapErr     DECIMAL,
                Poverty             DECIMAL,
                ChildPoverty        DECIMAL,
                Professional        DECIMAL,
                Service             DECIMAL,
                Office              DECIMAL,
                Construction        DECIMAL,
                Production          DECIMAL,
                Drive               DECIMAL,
                Carpool             DECIMAL,
                Transit             DECIMAL,
                Walk                DECIMAL,
                OtherTransp         DECIMAL,
                WorkAtHome          DECIMAL,
                MeanCommute         DECIMAL,
                Employed            INTEGER,
                PrivateWork         DECIMAL,
                PublicWork          DECIMAL,
                SelfEmployed        DECIMAL,
                FamilyWork         DECIMAL,
                Unemployment        DECIMAL
            );
        """)  # Note: No primary key or indexes created here
        print("Table created without constraints or indexes.")

def load(conn, cmdlist):
    with conn.cursor() as cursor:
        print(f"Loading {len(cmdlist)} rows")
        start = time.perf_counter()
        for cmd in cmdlist:
            cursor.execute(cmd)
        
        # Now add constraints and indexes after loading data
        cursor.execute("ALTER TABLE CensusData ADD PRIMARY KEY (CensusTract);")
        cursor.execute("CREATE INDEX idx_CensusData_State ON CensusData(State);")

        elapsed = time.perf_counter() - start
        print(f"Finished Loading. Constraints and Indexes created. Elapsed Time: {elapsed:.4f} seconds")

def main():
    initialize()
    conn = dbconnect()
    if CreateDB:
        createTable(conn)
    rowlist = readdata(Datafile)
    cmdlist = getSQLcmnds(rowlist)
    load(conn, cmdlist)

if __name__ == "__main__":
    main()
