import pandas as pd
import os
import validation  # <--- LINKING THE NEW MODULE

# ==========================================
#      PART 1: CLEANING & FEATURES
# ==========================================

def clean_employee_data(df):
    """ EMPLOYEES: Clean + Tenure + Salary Bucket """
    df = df.copy()
    if 'status' in df.columns:
        df = df[df['status'] != 'inactive']
    if 'salary' in df.columns:
        df = df[df['salary'] != 0]
    if 'department_id' in df.columns:
        df['department_id'] = df['department_id'].fillna(-1).astype(int)
    if 'hire_date' in df.columns:
        df['hire_date'] = pd.to_datetime(df['hire_date'])
    if 'bonus_eligible' in df.columns:
        df['bonus_eligible'] = df['bonus_eligible'].map({'Y': 1, 'N': 0})

    # Features
    if 'hire_date' in df.columns:
        today = pd.Timestamp.today()
        df['tenure_years'] = ((today - df['hire_date']).dt.days / 365.25).round(1)
    if 'salary' in df.columns:
        df['salary_bucket'] = df['salary'].apply(
            lambda x: 'Low' if x < 50000 else ('Medium' if x <= 80000 else 'High')
        )
    return df

def clean_review_data(df):
    """ REVIEWS: Clean + Category + Latest Rating """
    df = df.copy()
    if 'review_date' in df.columns:
        df['review_date'] = pd.to_datetime(df['review_date'])
    if 'employee_id' in df.columns and 'review_date' in df.columns:
        df = df.sort_values(by=['employee_id', 'review_date'])
        df = df.drop_duplicates(subset=['employee_id', 'review_date'])
    if 'rating' in df.columns:
        df = df[(df['rating'] >= 1.0) & (df['rating'] <= 5.0)]
    if 'reviewer_id' in df.columns and 'employee_id' in df.columns:
        df['is_self_review'] = df['reviewer_id'] == df['employee_id']

    # Features
    if 'rating' in df.columns:
        df['performance_category'] = df['rating'].apply(
            lambda x: 'Excellent' if x >= 4.5 else ('Good' if x >= 3.5 else 'Needs Improvement')
        )
    if 'employee_id' in df.columns and 'rating' in df.columns:
        df['latest_rating'] = df.groupby('employee_id')['rating'].transform('last')
    return df

def clean_project_data(df):
    """ PROJECTS: Clean + Duration + Budget Stats """
    df = df.copy()
    if 'budget' in df.columns:
        df = df.dropna(subset=['budget'])
        df = df[df['budget'] > 0]
    if 'start_date' in df.columns:
        df['start_date'] = pd.to_datetime(df['start_date'])
    if 'end_date' in df.columns:
        df['end_date'] = pd.to_datetime(df['end_date'])
    
    # Features
    today = pd.Timestamp.today()
    temp_end_date = df['end_date'].fillna(today)
    df['project_duration_days'] = (temp_end_date - df['start_date']).dt.days

    if 'budget' in df.columns and 'project_duration_days' in df.columns:
        df['daily_budget_alloc'] = df.apply(
            lambda x: x['budget'] / x['project_duration_days'] if x['project_duration_days'] > 0 else 0, 
            axis=1
        ).round(2)
    return df

def clean_assignment_data(df):
    """ ASSIGNMENTS: Clean """
    df = df.copy()
    if 'allocation_percentage' in df.columns:
        df = df[df['allocation_percentage'] <= 100]
    if 'start_date' in df.columns:
        df['start_date'] = pd.to_datetime(df['start_date'])
    if 'end_date' in df.columns:
        df['end_date'] = pd.to_datetime(df['end_date'])
    if 'start_date' in df.columns and 'end_date' in df.columns:
        valid_dates = (df['end_date'].isna()) | (df['start_date'] <= df['end_date'])
        df = df[valid_dates]
    return df

# ==========================================
#      PART 2: AGGREGATION LOGIC
# ==========================================

def create_dept_summary(emp_df, proj_df, dept_df):
    """ Aggregation 1: Department Summary """
    
    # --- FIX START: Smarter Column Handling ---
    # Check what the department name column is actually called in the CSV
    if 'name' in dept_df.columns:
        # If it's called 'name', rename it to 'department_name' for the summary table
        summary = dept_df[['department_id', 'name']].rename(columns={'name': 'department_name'})
    elif 'department_name' in dept_df.columns:
        # If it's already called 'department_name', just use it
        summary = dept_df[['department_id', 'department_name']]
    else:
        # Fallback if neither exists (prevents crashing, but will result in Unknown)
        print("Warning: Could not find 'name' or 'department_name' in departments.csv")
        summary = dept_df[['department_id']]
        summary['department_name'] = 'Unknown'
    # --- FIX END ---

    # Employee Stats
    emp_stats = emp_df.groupby('department_id').agg(
        total_employees=('employee_id', 'count'),
        avg_salary=('salary', 'mean')
    ).reset_index()

    # Project Stats
    if 'department_id' in proj_df.columns:
        today = pd.Timestamp.today()
        active_mask = (proj_df['end_date'].isna()) | (proj_df['end_date'] > today)
        proj_stats = proj_df[active_mask].groupby('department_id').agg(
            active_projects=('project_id', 'count'),
            total_budget=('budget', 'sum')
        ).reset_index()
    else:
        proj_stats = pd.DataFrame(columns=['department_id', 'active_projects', 'total_budget'])

    # Merge
    final_df = summary.merge(emp_stats, on='department_id', how='left')
    final_df = final_df.merge(proj_stats, on='department_id', how='left')

    # FIX: Explicitly infer types before filling to silence FutureWarning
    pd.set_option('future.no_silent_downcasting', True) # Optional safety
    final_df = final_df.infer_objects(copy=False)

    # Now fill NaNs and Enforce Types
    final_df['total_employees'] = final_df['total_employees'].fillna(0).astype(int)
    final_df['active_projects'] = final_df['active_projects'].fillna(0).astype(int)
    final_df['total_budget'] = final_df['total_budget'].fillna(0.0).astype(float)

    if 'avg_salary' in final_df.columns:
        final_df['avg_salary'] = final_df['avg_salary'].round(2)
        
    return final_df

def create_emp_performance(emp_df, rev_df, dept_df):
    """ Aggregation 2: Employee Performance Summary """
    base_emp = emp_df[['employee_id', 'name', 'department_id']]
    
    # Handle Department Name Column
    if 'name' in dept_df.columns:
        depts = dept_df[['department_id', 'name']].rename(columns={'name': 'department_name'})
    elif 'department_name' in dept_df.columns:
        depts = dept_df[['department_id', 'department_name']]
    else:
        depts = dept_df[['department_id']]
        depts['department_name'] = 'Unknown'

    # Merge Departments onto Employees
    base_emp = base_emp.merge(depts, on='department_id', how='left')

    # ### NEW FIX: Fill 'Ghost' Departments with 'Unknown' ###
    # This catches employees whose department_id does not exist in the departments file
    base_emp['department_name'] = base_emp['department_name'].fillna('Unknown')

    # Calculate Review Stats
    rev_stats = rev_df.sort_values('review_date').groupby('employee_id').agg(
        avg_rating=('rating', 'mean'),
        review_count=('rating', 'count'),
        latest_rating=('rating', 'last'),
        latest_review_date=('review_date', 'max')
    ).reset_index()

    # Merge Stats onto Employees
    final_df = base_emp.merge(rev_stats, on='employee_id', how='left')

    # Clean up
    final_df['avg_rating'] = final_df['avg_rating'].round(2)
    final_df['review_count'] = final_df['review_count'].fillna(0).astype(int)
    
    cols = ['employee_id', 'name', 'department_name', 'avg_rating', 'review_count', 'latest_rating', 'latest_review_date']
    existing_cols = [c for c in cols if c in final_df.columns]
    return final_df[existing_cols]

def create_project_workload(proj_df, assign_df):
    """ Aggregation 3: Project Workload Summary """
    if 'project_name' in proj_df.columns:
        base_proj = proj_df[['project_id', 'project_name']]
    else:
        base_proj = proj_df[['project_id']]

    workload_stats = assign_df.groupby('project_id').agg(
        total_team_size=('employee_id', 'nunique'),
        total_allocation=('allocation_percentage', 'sum'),
        avg_allocation=('allocation_percentage', 'mean')
    ).reset_index()

    final_df = base_proj.merge(workload_stats, on='project_id', how='left')
    final_df = final_df.fillna({'total_team_size': 0, 'total_allocation': 0, 'avg_allocation': 0})
    final_df['total_team_size'] = final_df['total_team_size'].astype(int)
    final_df['avg_allocation'] = final_df['avg_allocation'].round(1)

    return final_df

# ==========================================
#      MAIN EXECUTION
# ==========================================

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(__file__))
    raw_dir = os.path.join(base_dir, 'data', 'extractRawFiles')

    print(f"--- Starting Full Transformation Pipeline with Validation ---")

    # 1. LOAD & CLEAN
    clean_emp = pd.DataFrame()
    path_emp = os.path.join(raw_dir, 'employees.csv')
    if os.path.exists(path_emp):
        clean_emp = clean_employee_data(pd.read_csv(path_emp))
        print(f"✓ Employees Cleaned")

    clean_dept = pd.DataFrame()
    path_dept = os.path.join(raw_dir, 'departments.csv')
    if os.path.exists(path_dept):
        clean_dept = pd.read_csv(path_dept)
        print(f"✓ Departments Loaded")
        
    # --- VALIDATION: EMPLOYEES ---
    if not clean_emp.empty and not clean_dept.empty:
        emp_errors = validation.validate_employees(clean_emp, clean_dept)
        if emp_errors:
            print("X Validation Issues found in Employees:")
            for e in emp_errors: print(f"  - {e}")
        else:
            print("✓ Employees Validation Passed")

    clean_rev = pd.DataFrame()
    path_rev = os.path.join(raw_dir, 'performance_reviews.csv')
    if os.path.exists(path_rev):
        clean_rev = clean_review_data(pd.read_csv(path_rev))
        print(f"✓ Reviews Cleaned")
        
    # --- VALIDATION: REVIEWS ---
    if not clean_rev.empty and not clean_emp.empty:
        rev_errors = validation.validate_reviews(clean_rev, clean_emp)
        if rev_errors:
            print("X Validation Issues found in Reviews:")
            for e in rev_errors: print(f"  - {e}")
        else:
            print("✓ Reviews Validation Passed")

    clean_proj = pd.DataFrame()
    path_proj = os.path.join(raw_dir, 'projects.csv')
    if os.path.exists(path_proj):
        clean_proj = clean_project_data(pd.read_csv(path_proj))
        print(f"✓ Projects Cleaned")

    # --- VALIDATION: PROJECTS ---
    if not clean_proj.empty:
        proj_errors = validation.validate_projects(clean_proj)
        if proj_errors:
            print("X Validation Issues found in Projects:")
            for e in proj_errors: print(f"  - {e}")
        else:
            print("✓ Projects Validation Passed")
        
    clean_ass = pd.DataFrame()
    path_ass = os.path.join(raw_dir, 'project_assignments.csv')
    if os.path.exists(path_ass):
        clean_ass = clean_assignment_data(pd.read_csv(path_ass))
        print(f"✓ Assignments Cleaned")

    # --- VALIDATION: ASSIGNMENTS ---
    if not clean_ass.empty and not clean_proj.empty and not clean_emp.empty:
        ass_errors = validation.validate_assignments(clean_ass, clean_proj, clean_emp)
        if ass_errors:
            print("X Validation Issues found in Assignments:")
            for e in ass_errors: print(f"  - {e}")
        else:
            print("✓ Assignments Validation Passed")


    # 2. AGGREGATE (Only run if we have data)
    print("\n--- Building Aggregations ---")
    
    # Table 1: Department Summary
    if not clean_emp.empty and not clean_proj.empty and not clean_dept.empty:
        dept_summ = create_dept_summary(clean_emp, clean_proj, clean_dept)
        print(f"\n[1] Department Summary ({len(dept_summ)} rows):")
        print(dept_summ.head(3).to_string(index=False))

    # Table 2: Employee Performance Summary
    if not clean_emp.empty and not clean_rev.empty and not clean_dept.empty:
        emp_perf = create_emp_performance(clean_emp, clean_rev, clean_dept)
        print(f"\n[2] Employee Performance Summary ({len(emp_perf)} rows):")
        print(emp_perf.head(3).to_string(index=False))

    # Table 3: Project Workload Summary
    if not clean_proj.empty and not clean_ass.empty:
        proj_work = create_project_workload(clean_proj, clean_ass)
        print(f"\n[3] Project Workload Summary ({len(proj_work)} rows):")
        print(proj_work.head(3).to_string(index=False))