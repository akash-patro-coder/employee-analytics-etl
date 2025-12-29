import logging
import time
import os
import sys
import pandas as pd
from datetime import datetime

# Import our custom modules
import extract  # You might need to make sure extract.py has a run() function or similar
import transform
import validation
import load

# ==========================================
#      CONFIGURATION & LOGGING
# ==========================================

# Create a 'logs' directory if it doesn't exist
log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)

# Generate a log filename with a timestamp
log_filename = os.path.join(log_dir, f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

# Setup Logging config
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename), # Save to file
        logging.StreamHandler(sys.stdout)  # Print to console
    ]
)

logger = logging.getLogger(__name__)

# ==========================================
#      PIPELINE PHASES
# ==========================================

def run_extraction():
    """ Phase 1: Extract Data from Source """
    logger.info(">>> PHASE 1: EXTRACTION STARTED")
    try:
        # If your extract.py has a main function, call it.
        # Otherwise, we assume it's a script that populates 'data/extractRawFiles'
        # For now, we will assume the raw files already exist or we trigger the extraction logic manually.
        
        # Example: Calling the logic directly if available, or just logging that we check files
        # extract.run_extraction() <--- Uncomment if you wrapped extract.py in a function
        
        # Check if files exist
        base_dir = os.path.dirname(os.path.dirname(__file__))
        raw_dir = os.path.join(base_dir, 'data', 'extractRawFiles')
        required_files = ['employees.csv', 'performance_reviews.csv', 'projects.csv', 'project_assignments.csv', 'departments.csv']
        
        missing = [f for f in required_files if not os.path.exists(os.path.join(raw_dir, f))]
        
        if missing:
            raise FileNotFoundError(f"Missing raw files: {missing}. Run extract.py first!")
        
        logger.info("Extraction Phase Completed (Files verified)")
        return raw_dir

    except Exception as e:
        logger.error(f"Extraction Failed: {e}")
        raise

def run_transformation(raw_dir):
    """ Phase 2: Transform & Clean Data """
    logger.info(">>> PHASE 2: TRANSFORMATION STARTED")
    start_time = time.time()
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
        clean_proj = transform.clean_project_data(raw_proj)
        clean_ass = transform.clean_assignment_data(raw_ass)
        
        # Departments (Simple clean)
        if 'name' in raw_dept.columns:
            raw_dept['name'] = raw_dept['name'].str.title()
        clean_dept = raw_dept.drop_duplicates()

        # Generate Aggregates
        summ_dept = transform.create_dept_summary(clean_emp, clean_proj, clean_dept)
        summ_emp = transform.create_emp_performance(clean_emp, clean_rev, clean_dept)
        
        # Align Columns (Using the logic we fixed in load.py)
        # 1. Depts
        if 'department_name' in clean_dept.columns:
             clean_dept = clean_dept.rename(columns={'department_name': 'name'})
        clean_dept = clean_dept[['department_id', 'name']]

        # 2. Employees
        emp_cols = ['employee_id', 'name', 'department_id', 'salary', 'hire_date', 
                    'status', 'bonus_eligible', 'tenure_years', 'salary_bucket']
        clean_emp = clean_emp[emp_cols]

        # 3. Reviews
        rev_cols = ['review_id', 'employee_id', 'review_date', 'rating', 'reviewer_id', 
                    'performance_category', 'latest_rating', 'is_self_review']
        clean_rev = clean_rev[[c for c in rev_cols if c in clean_rev.columns]]

        # 4. Assignments
        ass_cols = ['employee_id', 'project_id', 'allocation_percentage', 'start_date', 'end_date']
        clean_ass = clean_ass[ass_cols]

        logger.info(f"Transformation finished in {time.time() - start_time:.2f} seconds")
        
        return {
            "dim_departments": clean_dept,
            "dim_employees": clean_emp,
            "fact_performance_reviews": clean_rev,
            "fact_project_assignments": clean_ass,
            "summary_dept_metrics": summ_dept,
            "summary_emp_performance": summ_emp,
            "raw_proj": clean_proj # Passed for validation purposes
        }

    except Exception as e:
        logger.error(f"Transformation Failed: {e}")
        raise

def run_validation(data_dict):
    """ Phase 3: Validate Data Quality """
    logger.info(">>> PHASE 3: VALIDATION STARTED")
    try:
        # Retrieve DFs
        emp = data_dict["dim_employees"]
        dept = data_dict["dim_departments"]
        rev = data_dict["fact_performance_reviews"]
        ass = data_dict["fact_project_assignments"]
        proj = data_dict["raw_proj"] # Need raw project data to validate FKs in assignments

        # Run Checks
        issues = []
        
        # Employees
        issues.extend(validation.validate_employees(emp, dept))
        
        # Reviews
        issues.extend(validation.validate_reviews(rev, emp))
        
        # Assignments
        # Note: We need a dataframe with 'project_id' to validate assignments. 
        # We used 'raw_proj' (cleaned project data) for this.
        issues.extend(validation.validate_assignments(ass, proj, emp))
        
        # Projects
        issues.extend(validation.validate_projects(proj))

        if issues:
            logger.warning(f"Validation found {len(issues)} issues:")
            for i in issues:
                logger.warning(f"  - {i}")
            # Decision: Do we stop or continue? 
            # Usually, for warnings, we continue. For Critical errors, we raise.
            # logger.error("Halting pipeline due to data quality issues.")
            # raise ValueError("Data Validation Failed")
        else:
            logger.info("All Data Validation Checks Passed.")

    except Exception as e:
        logger.error(f"Validation Failed: {e}")
        raise

def run_loading(data_dict):
    """ Phase 4: Load to Database & Export CSV """
    logger.info(">>> PHASE 4: LOADING STARTED")
    start_time = time.time()
    
    conn = None
    try:
        # Credentials
        HOST = "localhost"
        USER = "root"
        PASS = "root"
        DB_NAME = "employee_analytics"

        # Connect
        conn = load.create_db_connection(HOST, USER, PASS, DB_NAME)
        if not conn:
            raise ConnectionError("Could not connect to database")

        cursor = conn.cursor()
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

        # 1. Export CSVs
        for table_name, df in data_dict.items():
            if table_name == "raw_proj": continue # Don't export the helper table
            load.export_to_csv(df, f"{table_name}.csv")

        # 2. Load to SQL
        tables_to_load = [k for k in data_dict.keys() if k != "raw_proj"]
        
        for table_name in tables_to_load:
            df = data_dict[table_name]
            load.insert_data(conn, df, table_name)
            logger.info(f"Loaded {table_name} into MySQL")

        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        # 3. Create Indexes (Using load.py's logic manually or calling a helper if we made one)
        logger.info("Creating Indexes...")
        load.create_index(conn, "dim_employees", "department_id")
        load.create_index(conn, "fact_performance_reviews", "employee_id")
        load.create_index(conn, "fact_project_assignments", "employee_id")
        
        logger.info(f"Loading finished in {time.time() - start_time:.2f} seconds")

    except Exception as e:
        logger.error(f"Loading Failed: {e}")
        raise
    finally:
        if conn:
            conn.close()

# ==========================================
#      MAIN ENTRY POINT
# ==========================================

if __name__ == "__main__":
    total_start = time.time()
    logger.info("=== ETL PIPELINE EXECUTION STARTED ===")
    
    try:
        # 1. Extract
        raw_data_path = run_extraction()
        
        # 2. Transform
        transformed_data = run_transformation(raw_data_path)
        
        # 3. Validate
        run_validation(transformed_data)
        
        # 4. Load
        run_loading(transformed_data)
        
        logger.info("=== ETL PIPELINE COMPLETED SUCCESSFULLY ===")
        
    except Exception as e:
        logger.critical(f"=== ETL PIPELINE CRASHED ===")
        logger.critical(str(e))
        sys.exit(1)
        
    logger.info(f"Total Execution Time: {time.time() - total_start:.2f} seconds")