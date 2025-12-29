import logging
import time
import os
import sys
import pandas as pd
from datetime import datetime

# Import Custom Modules
import extract
import transform
import validation
import load
import reporting  # <--- NEW IMPORT

# ==========================================
#      CONFIGURATION & LOGGING
# ==========================================

log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[logging.FileHandler(log_filename), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ==========================================
#      PIPELINE PHASES
# ==========================================

def run_extraction():
    """ Phase 1: Extract & Verify Files """
    logger.info(">>> PHASE 1: EXTRACTION STARTED")
    start = time.time()
    
    base_dir = os.path.dirname(os.path.dirname(__file__))
    raw_dir = os.path.join(base_dir, 'data', 'extractRawFiles')
    required_files = ['employees.csv', 'performance_reviews.csv', 'projects.csv', 'project_assignments.csv', 'departments.csv']
    
    missing = [f for f in required_files if not os.path.exists(os.path.join(raw_dir, f))]
    if missing:
        raise FileNotFoundError(f"Missing raw files: {missing}")
    
    # Calculate Volume Stats for Report
    volume_counts = {}
    for f in required_files:
        path = os.path.join(raw_dir, f)
        count = len(pd.read_csv(path))
        volume_counts[f.replace('.csv', '')] = {'extracted': count}

    duration = time.time() - start
    logger.info(f"Extraction completed in {duration:.2f}s")
    
    return raw_dir, duration, volume_counts

def run_transformation(raw_dir, volume_stats):
    """ Phase 2: Transform & Clean Data """
    logger.info(">>> PHASE 2: TRANSFORMATION STARTED")
    start = time.time()
    
    # Load
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
    
    if 'name' in raw_dept.columns:
        raw_dept['name'] = raw_dept['name'].str.title()
    clean_dept = raw_dept.drop_duplicates()

    # Update Volume Stats with Cleaned Counts
    volume_stats['employees']['cleaned'] = len(clean_emp)
    volume_stats['performance_reviews']['cleaned'] = len(clean_rev)
    volume_stats['projects']['cleaned'] = len(clean_proj)
    volume_stats['project_assignments']['cleaned'] = len(clean_ass)
    volume_stats['departments']['cleaned'] = len(clean_dept)

    # Aggregates
    summ_dept = transform.create_dept_summary(clean_emp, clean_proj, clean_dept)
    summ_emp = transform.create_emp_performance(clean_emp, clean_rev, clean_dept)
    proj_work = transform.create_project_workload(clean_proj, clean_ass)

    # Column Alignment
    if 'department_name' in clean_dept.columns:
            clean_dept = clean_dept.rename(columns={'department_name': 'name'})
    clean_dept = clean_dept[['department_id', 'name']]

    emp_cols = ['employee_id', 'name', 'department_id', 'salary', 'hire_date', 'status', 'bonus_eligible', 'tenure_years', 'salary_bucket']
    clean_emp = clean_emp[[c for c in emp_cols if c in clean_emp.columns]]
    
    rev_cols = ['review_id', 'employee_id', 'review_date', 'rating', 'reviewer_id', 'performance_category', 'latest_rating', 'is_self_review']
    clean_rev = clean_rev[[c for c in rev_cols if c in clean_rev.columns]]

    ass_cols = ['employee_id', 'project_id', 'allocation_percentage', 'start_date', 'end_date']
    clean_ass = clean_ass[[c for c in ass_cols if c in clean_ass.columns]]

    data_dict = {
        "dim_departments": clean_dept,
        "dim_employees": clean_emp,
        "fact_performance_reviews": clean_rev,
        "fact_project_assignments": clean_ass,
        "summary_dept_metrics": summ_dept,
        "summary_emp_performance": summ_emp,
        "raw_proj": clean_proj  # kept for reporting metrics
    }
    
    duration = time.time() - start
    logger.info(f"Transformation completed in {duration:.2f}s")
    return data_dict, duration

def run_validation(data_dict):
    """ Phase 3: Validate """
    logger.info(">>> PHASE 3: VALIDATION STARTED")
    start = time.time()
    
    issues = []
    # Note: We need the raw_proj (clean version) for validation
    proj = data_dict["raw_proj"] 
    
    issues.extend(validation.validate_employees(data_dict["dim_employees"], data_dict["dim_departments"]))
    issues.extend(validation.validate_reviews(data_dict["fact_performance_reviews"], data_dict["dim_employees"]))
    issues.extend(validation.validate_assignments(data_dict["fact_project_assignments"], proj, data_dict["dim_employees"]))
    issues.extend(validation.validate_projects(proj))

    dq_stats = {
        'total_checks': 4, # High level check types
        'passed': 4 if not issues else 0, # Simple pass/fail logic
        'failed': len(issues),
        'critical_issues': issues
    }
    
    if issues:
        logger.warning(f"Validation found {len(issues)} issues")
    else:
        logger.info("Validation Passed")

    duration = time.time() - start
    return dq_stats, duration

def run_loading(data_dict):
    """ Phase 4: Load """
    logger.info(">>> PHASE 4: LOADING STARTED")
    start = time.time()
    
    conn = load.create_db_connection("localhost", "root", "root", "employee_analytics")
    if not conn: raise ConnectionError("DB Connection Failed")

    cursor = conn.cursor()
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

    # Export & Load
    tables_to_load = [k for k in data_dict.keys() if k != "raw_proj"]
    for table in tables_to_load:
        df = data_dict[table]
        load.export_to_csv(df, f"{table}.csv")
        load.insert_data(conn, df, table)

    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    
    # Indexes
    load.create_index(conn, "dim_employees", "department_id")
    load.create_index(conn, "fact_performance_reviews", "employee_id")
    load.create_index(conn, "fact_project_assignments", "employee_id")

    conn.close()
    duration = time.time() - start
    logger.info(f"Loading completed in {duration:.2f}s")
    return duration

# ==========================================
#      MAIN ENTRY POINT
# ==========================================

if __name__ == "__main__":
    total_start = time.time()
    logger.info("=== ETL PIPELINE STARTED ===")
    
    # Stats Containers
    exec_stats = {
        "start_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "phases": {}
    }
    volume_stats = {} # Will be populated in Extraction
    
    try:
        # 1. Extract
        raw_path, dur_ext, volume_stats = run_extraction()
        exec_stats["phases"]["Extraction"] = dur_ext
        
        # 2. Transform
        processed_data, dur_trans = run_transformation(raw_path, volume_stats)
        exec_stats["phases"]["Transformation"] = dur_trans
        
        # 3. Validate
        dq_stats, dur_val = run_validation(processed_data)
        exec_stats["phases"]["Validation"] = dur_val
        
        # 4. Load
        dur_load = run_loading(processed_data)
        exec_stats["phases"]["Loading"] = dur_load
        
        # 5. Generate Report
        exec_stats["end_time"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        exec_stats["total_duration"] = round(time.time() - total_start, 2)
        
        reporting.generate_summary_report(exec_stats, volume_stats, dq_stats, processed_data)
        
        logger.info("=== ETL PIPELINE COMPLETED ===")
        
    except Exception as e:
        logger.critical(f"PIPELINE CRASHED: {e}")
        sys.exit(1)