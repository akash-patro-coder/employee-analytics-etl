import pandas as pd

# ==========================================
#      GENERIC CHECK FUNCTIONS
# ==========================================

def check_completeness(df, table_name, pk, required_cols):
    """ Checks for NULL Primary Keys and Required Fields """
    issues = []
    
    # 1. Primary Key Check
    if pk and pk in df.columns:
        if df[pk].isnull().any():
            issues.append(f"[{table_name}] Found NULL values in Primary Key: {pk}")
        if df[pk].duplicated().any():
            issues.append(f"[{table_name}] Found DUPLICATE values in Primary Key: {pk}")

    # 2. Required Columns Check
    for col in required_cols:
        if col in df.columns:
            if df[col].isnull().any():
                issues.append(f"[{table_name}] Found NULL values in required column: {col}")
        else:
            issues.append(f"[{table_name}] Missing required column: {col}")
            
    return issues

def check_consistency(child_df, child_fk, parent_df, parent_pk, table_name, parent_name):
    """ Checks if Foreign Keys exist in the Parent Table """
    issues = []
    if child_fk in child_df.columns and parent_pk in parent_df.columns:
        # Find values in child that are NOT in parent
        # We assume -1 or NaN are "unassigned" and ignore them for this check if desired,
        # but strict consistency usually demands valid keys.
        
        # Get unique foreign keys from child (excluding -1 if you used that for nulls)
        child_keys = child_df[child_df[child_fk] != -1][child_fk].unique()
        parent_keys = parent_df[parent_pk].unique()
        
        # Check for invalid keys
        invalid_keys = [k for k in child_keys if k not in parent_keys]
        
        if invalid_keys:
            issues.append(f"[{table_name}] Consistency Error: {len(invalid_keys)} {child_fk} values do not exist in {parent_name}.")
    return issues

def check_accuracy(df, table_name, condition, error_msg):
    """ Checks if rows meet a specific logical condition """
    issues = []
    # The condition should be a boolean Series. We look for False values.
    # We pass the condition mask directly.
    # Example: df['salary'] > 0
    
    failed_rows = df[~condition]
    if not failed_rows.empty:
        issues.append(f"[{table_name}] Accuracy Error: {len(failed_rows)} rows failed check: {error_msg}")
    return issues

# ==========================================
#      SPECIFIC TABLE VALIDATORS
# ==========================================

def validate_employees(df, dept_df):
    errors = []
    print("   Running Validation on Employees...")
    
    # 1. Completeness
    errors.extend(check_completeness(df, 'Employees', 'employee_id', ['name', 'salary', 'hire_date']))
    
    # 2. Consistency (Dept ID must exist in Departments)
    errors.extend(check_consistency(df, 'department_id', dept_df, 'department_id', 'Employees', 'Departments'))
    
    # 3. Validity (Enum check)
    if 'status' in df.columns:
        valid_statuses = ['active', 'inactive', 'terminated', 'leave']
        invalid_mask = ~df['status'].isin(valid_statuses)
        if invalid_mask.any():
            errors.append(f"[Employees] Validity Error: Found invalid status values.")
            
    # 4. Accuracy (Salary > 0 for active)
    # Note: We filter for 'active' first to avoid flagging inactive employees with 0 salary if that's allowed
    if 'salary' in df.columns and 'status' in df.columns:
        # Check: If status is active, salary must be > 0
        mask = (df['status'] == 'active') & (df['salary'] <= 0)
        if mask.any():
            errors.append("[Employees] Accuracy Error: Active employees found with Salary <= 0")

    return errors

def validate_reviews(df, emp_df):
    errors = []
    print("   Running Validation on Reviews...")
    
    # 1. Completeness
    errors.extend(check_completeness(df, 'Reviews', 'review_id', ['employee_id', 'rating', 'review_date']))
    
    # 2. Consistency (Employee ID must exist)
    errors.extend(check_consistency(df, 'employee_id', emp_df, 'employee_id', 'Reviews', 'Employees'))
    
    # 3. Accuracy (Rating 1.0 - 5.0)
    if 'rating' in df.columns:
        condition = (df['rating'] >= 1.0) & (df['rating'] <= 5.0)
        errors.extend(check_accuracy(df, 'Reviews', condition, "Rating must be between 1.0 and 5.0"))
        
    return errors

def validate_assignments(df, proj_df, emp_df):
    errors = []
    print("   Running Validation on Assignments...")
    
    # 1. Consistency (Project ID AND Employee ID)
    errors.extend(check_consistency(df, 'project_id', proj_df, 'project_id', 'Assignments', 'Projects'))
    errors.extend(check_consistency(df, 'employee_id', emp_df, 'employee_id', 'Assignments', 'Employees'))
    
    # 2. Accuracy (Allocation 0-100)
    if 'allocation_percentage' in df.columns:
        condition = (df['allocation_percentage'] >= 0) & (df['allocation_percentage'] <= 100)
        errors.extend(check_accuracy(df, 'Assignments', condition, "Allocation must be 0-100%"))

    return errors

def validate_projects(df):
    errors = []
    print("   Running Validation on Projects...")
    
    # 1. Completeness
    errors.extend(check_completeness(df, 'Projects', 'project_id', ['project_name', 'start_date']))
    
    # 2. Accuracy (Budget > 0)
    if 'budget' in df.columns:
        # Ignore NaNs for this check, assuming cleaning handled them or they are optional
        condition = (df['budget'].isnull()) | (df['budget'] > 0)
        errors.extend(check_accuracy(df, 'Projects', condition, "Budget must be positive"))
        
    return errors