import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
import sys
import numpy as np

# Import your transformation script
import transform 

# ==========================================
#      DATABASE HELPERS
# ==========================================

def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print(f"✓ Connected to Database: {db_name}")
    except Error as err:
        print(f"X Connection Error: '{err}'")
    return connection

def insert_data(connection, df, table_name):
    cursor = connection.cursor()
    # Handle NaNs for SQL
    df = df.replace({np.nan: None})
    
    try:
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        
        cols = ",".join(df.columns.tolist())
        placeholders = ",".join(["%s"] * len(df.columns))
        sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
        
        data_tuples = [tuple(x) for x in df.to_numpy()]
        cursor.executemany(sql, data_tuples)
        connection.commit()
        print(f"✓ DB Load: {len(df)} rows -> '{table_name}'")
        
    except Error as err:
        print(f"X DB Load Error {table_name}: {err}")

def create_index(connection, table_name, column_name):
    cursor = connection.cursor()
    index_name = f"idx_{table_name}_{column_name}"
    try:
        sql = f"CREATE INDEX {index_name} ON {table_name} ({column_name})"
        cursor.execute(sql)
        connection.commit()
        print(f"  + Index created: {index_name}")
    except Error as err:
        if "Duplicate key name" in str(err):
            pass # Index exists, ignore
        else:
            print(f"X Index Error on {table_name}: {err}")

# ==========================================
#      FILE EXPORT HELPER (New!)
# ==========================================

def export_to_csv(df, filename):
    """ Saves DataFrame to data/processed/ with standard formatting """
    base_dir = os.path.dirname(os.path.dirname(__file__))
    processed_dir = os.path.join(base_dir, 'data', 'processed')
    
    # Ensure directory exists
    os.makedirs(processed_dir, exist_ok=True)
    
    path = os.path.join(processed_dir, filename)
    
    # Save CSV: Include Header, No Index, Date Format YYYY-MM-DD
    try:
        df.to_csv(path, index=False, header=True, date_format='%Y-%m-%d')
        print(f"✓ File Saved: {filename}")
    except Exception as e:
        print(f"X File Save Error {filename}: {e}")

# ==========================================
#      MAIN ETL PROCESS
# ==========================================

def run_load_process():
    HOST = "localhost"
    USER = "root"
    PASS = "root"
    DB_NAME = "employee_analytics"

    conn = create_db_connection(HOST, USER, PASS, DB_NAME)
    if conn is None: return

    print("\n--- 1. Fetching & Transforming Data ---")
    base_dir = os.path.dirname(os.path.dirname(__file__))
    raw_dir = os.path.join(base_dir, 'data', 'extractRawFiles')
    
    try:
        # Load Raw
        raw_emp = pd.read_csv(os.path.join(raw_dir, 'employees.csv'))
        raw_rev = pd.read_csv(os.path.join(raw_dir, 'performance_reviews.csv'))
        raw_proj = pd.read_csv(os.path.join(raw_dir, 'projects.csv'))
        raw_ass = pd.read_csv(os.path.join(raw_dir, 'project_assignments.csv'))
        raw_dept = pd.read_csv(os.path.join(raw_dir, 'departments.csv'))
        
        # Transform
        clean_emp = transform.clean_employee_data(raw_emp)
        clean_rev = transform.clean_review_data(raw_rev)
        clean_ass = transform.clean_assignment_data(raw_ass)
        
        if 'name' in raw_dept.columns:
            raw_dept['name'] = raw_dept['name'].str.title()
        clean_dept = raw_dept.drop_duplicates()

        clean_proj = transform.clean_project_data(raw_proj) 
        summ_dept = transform.create_dept_summary(clean_emp, clean_proj, clean_dept)
        summ_emp = transform.create_emp_performance(clean_emp, clean_rev, clean_dept)
        
        print("✓ Data Transformation successful")

        # Column Alignment (Fixing the errors you saw earlier)
        if 'department_name' in clean_dept.columns:
             clean_dept = clean_dept.rename(columns={'department_name': 'name'})
        clean_dept = clean_dept[['department_id', 'name']]

        emp_cols = ['employee_id', 'name', 'department_id', 'salary', 'hire_date', 
                    'status', 'bonus_eligible', 'tenure_years', 'salary_bucket']
        clean_emp = clean_emp[emp_cols]

        rev_cols = ['review_id', 'employee_id', 'review_date', 'rating', 'reviewer_id', 
                    'performance_category', 'latest_rating', 'is_self_review']
        clean_rev = clean_rev[[c for c in rev_cols if c in clean_rev.columns]]

        ass_cols = ['employee_id', 'project_id', 'allocation_percentage', 'start_date', 'end_date']
        clean_ass = clean_ass[ass_cols]

    except Exception as e:
        print(f"X Critical Error during Transformation: {e}")
        conn.close()
        return

    # --- 2. EXPORT TO CSV (Processed Zone) ---
    print("\n--- 2. Exporting Processed Data ---")
    export_to_csv(clean_dept, "dim_departments.csv")
    export_to_csv(clean_emp, "dim_employees.csv")
    export_to_csv(clean_rev, "fact_performance_reviews.csv")
    export_to_csv(clean_ass, "fact_project_assignments.csv")
    export_to_csv(summ_dept, "summary_dept_metrics.csv")
    export_to_csv(summ_emp, "summary_emp_performance.csv")

    # --- 3. LOADING TO MYSQL ---
    print("\n--- 3. Loading Data into MySQL ---")
    cursor = conn.cursor()
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    
    tasks = [
        (clean_dept, "dim_departments"),
        (clean_emp, "dim_employees"),
        (clean_rev, "fact_performance_reviews"),
        (clean_ass, "fact_project_assignments"),
        (summ_dept, "summary_dept_metrics"),
        (summ_emp, "summary_emp_performance")
    ]
    
    for df, table in tasks:
        insert_data(conn, df, table)

    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    
    # --- 4. INDEXING ---
    print("\n--- 4. Creating Indexes ---")
    create_index(conn, "dim_employees", "department_id")
    create_index(conn, "fact_performance_reviews", "employee_id")
    create_index(conn, "fact_project_assignments", "employee_id")
    create_index(conn, "fact_project_assignments", "project_id")
    
    conn.close()
    print("\n✓ ETL Process Finished Successfully!")

if __name__ == "__main__":
    run_load_process()