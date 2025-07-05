import pyodbc
import os


# Get connection to the database
def connect_to_db():

    DB_DRIVER = os.getenv('DB_DRIVER')
    DB_SERVER = os.getenv('DB_SERVER')
    DB_DATABASE = os.getenv('DB_DATABASE') 
    
    conn = pyodbc.connect(
        f"Driver={DB_DRIVER};"
        f"Server={DB_SERVER};"
        f"Database={DB_DATABASE};"
        f"Trusted_Connection=yes;"
    )

    return conn


def sanitize_value(value):
    value = value.replace("'", "''")    # Escape single quotes
    value = value.replace("--", "-")    # Escape double dashes
    return str(value)




def insert_rows(table_name, rows):
    if not rows:
        return

    # Get distinct column list from all rows
    columns = list({key for row in rows for key in row.keys()})
    

    # Join together for string output
    col_str = ', '.join(f'[{ sanitize_value(col) }]' for col in columns)
    values_list = []

    for row in rows:
        vals = []
        for col in columns:
            val = row.get(col, 'NULL')
            if isinstance(val, str):
                val = val.replace("'", "''")
                vals.append(f"'{val}'")
            elif val is None:
                vals.append("NULL")
            else:
                vals.append(str(val))
        values_list.append(f"({', '.join(vals)})")

    sql = f"INSERT INTO {table_name} ({col_str}) VALUES {', '.join(values_list)}"

    # cleanup
    sql = sql.replace('None', 'NULL').replace("'NULL'", 'NULL').replace('False', '0').replace('True', '1')

    conn = connect_to_db()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
    except Exception as e:
        print(f"Error inserting rows into {table_name}: {e}")
        print(f"SQL: {sql}")
        raise        
    finally:
        conn.close()



def create_table(table_name, rows, drop_if_exists):
    if not rows:
        return
    
    default_data_type = os.getenv('DEFAULT_TARGET_COLUMN_DATA_TYPE')

    # Get distinct column list from all rows
    columns = list({key for row in rows for key in row.keys()})

    # Join together for string output
    col_str = f' {default_data_type}, '.join(f'[{col}]' for col in columns) + f' {default_data_type}'

    dropSql = f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};\n"

    createSql = f"CREATE TABLE {table_name} ({col_str})"

    conn = connect_to_db()

    try:
        cursor = conn.cursor()
        if drop_if_exists:
            cursor.execute(dropSql)
        cursor.execute(createSql)
        conn.commit()
    
    finally:
        conn.close()        


def execute_non_query(sql):
    conn = connect_to_db()

    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    