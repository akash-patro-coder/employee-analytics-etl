import os
import pandas as pd
from datetime import datetime

def generate_summary_report(exec_stats, volume_stats, dq_stats, data_dfs):
    """
    Generates a text report in reports/etl_summary_report.txt
    """
    # 1. Setup Output Path
    base_dir = os.path.dirname(os.path.dirname(__file__))
    report_dir = os.path.join(base_dir, 'reports')
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, 'etl_summary_report.txt')
    
    lines = []
    lines.append("==================================================")
    lines.append(f"ETL PIPELINE SUMMARY REPORT")
    lines.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("==================================================\n")

    # ---------------------------------------
    # 1. Execution Summary
    # ---------------------------------------
    lines.append("1. EXECUTION SUMMARY")
    lines.append("--------------------")
    lines.append(f"Start Time: {exec_stats.get('start_time')}")
    lines.append(f"End Time:   {exec_stats.get('end_time')}")
    lines.append(f"Total Duration: {exec_stats.get('total_duration')} seconds")
    lines.append("\nPhase Durations:")
    for phase, duration in exec_stats.get('phases', {}).items():
        lines.append(f"  - {phase}: {duration:.2f} seconds")
    lines.append("\n")

    # ---------------------------------------
    # 2. Data Volume Summary
    # ---------------------------------------
    lines.append("2. DATA VOLUME SUMMARY")
    lines.append("----------------------")
    lines.append(f"{'Table':<25} | {'Extracted':<10} | {'Cleaned':<10} | {'Removed':<10}")
    lines.append("-" * 65)
    
    total_loaded = 0
    for table, stats in volume_stats.items():
        extracted = stats.get('extracted', 0)
        cleaned = stats.get('cleaned', 0)
        removed = extracted - cleaned
        lines.append(f"{table:<25} | {extracted:<10} | {cleaned:<10} | {removed:<10}")
        total_loaded += cleaned
        
    lines.append("-" * 65)
    lines.append(f"Total Records Loaded to DB: {total_loaded}\n")

    # ---------------------------------------
    # 3. Data Quality Summary
    # ---------------------------------------
    lines.append("3. DATA QUALITY SUMMARY")
    lines.append("-----------------------")
    lines.append(f"Total Checks Performed: {dq_stats.get('total_checks', 0)}")
    lines.append(f"Passed: {dq_stats.get('passed', 0)}")
    lines.append(f"Failed/Issues: {dq_stats.get('failed', 0)}")
    
    if dq_stats.get('critical_issues'):
        lines.append("\nCritical Issues Found:")
        for issue in dq_stats['critical_issues']:
            lines.append(f"  [!] {issue}")
    else:
        lines.append("\nNo Critical Issues Found.")
    lines.append("\n")

    # ---------------------------------------
    # 4. Business Insights
    # ---------------------------------------
    lines.append("4. BUSINESS INSIGHTS")
    lines.append("--------------------")
    
    # Unpack DataFrames for easier access
    dept_summ = data_dfs.get('summary_dept_metrics', pd.DataFrame())
    emp_perf = data_dfs.get('summary_emp_performance', pd.DataFrame())
    dim_emp = data_dfs.get('dim_employees', pd.DataFrame())
    # Note: 'raw_proj' was passed in data_dfs in main.py, helpful for duration
    raw_proj = data_dfs.get('raw_proj', pd.DataFrame()) 

    try:
        # Insight A: Department with Highest Avg Salary
        if not dept_summ.empty and 'avg_salary' in dept_summ.columns:
            top_salary_dept = dept_summ.sort_values('avg_salary', ascending=False).iloc[0]
            lines.append(f"Highest Avg Salary Dept:   {top_salary_dept['department_name']} (${top_salary_dept['avg_salary']:,.2f})")
        
        # Insight B: Top 5 Employees
        if not emp_perf.empty and 'avg_rating' in emp_perf.columns:
            top_5 = emp_perf.sort_values('avg_rating', ascending=False).head(5)
            names = ", ".join(top_5['name'].tolist())
            lines.append(f"Top 5 Employees (Rating):  {names}")

        # Insight C: Dept with Most Active Projects
        if not dept_summ.empty and 'active_projects' in dept_summ.columns:
            top_proj_dept = dept_summ.sort_values('active_projects', ascending=False).iloc[0]
            lines.append(f"Most Active Projects Dept: {top_proj_dept['department_name']} ({top_proj_dept['active_projects']} projects)")

        # Insight D: Longest Tenure
        if not dim_emp.empty and 'tenure_years' in dim_emp.columns:
            longest_emp = dim_emp.sort_values('tenure_years', ascending=False).iloc[0]
            lines.append(f"Longest Tenure Employee:   {longest_emp['name']} ({longest_emp['tenure_years']} years)")

        # Insight E: Avg Project Duration (Global)
        # Note: Calculating per department requires complex joins not always present in raw data.
        # We will show Global Avg Duration instead, derived from the raw project data.
        if not raw_proj.empty and 'project_duration_days' in raw_proj.columns:
            avg_dur = raw_proj['project_duration_days'].mean()
            lines.append(f"Avg Project Duration:      {avg_dur:.1f} days")

    except Exception as e:
        lines.append(f"Could not calculate all insights: {e}")

    lines.append("\n==================================================")
    lines.append("END OF REPORT")
    lines.append("==================================================")

    # Write to file
    with open(report_path, 'w') as f:
        f.write("\n".join(lines))
    
    print(f"✓ Summary Report generated: {report_path}")