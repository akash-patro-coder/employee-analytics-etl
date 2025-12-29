import csv
import os

# Data for all tables
data = {
    "departments.csv": [
        ["department_id", "department_name", "location", "budget", "manager_id"],
        [101, "Engineering", "Bangalore", 5000000, 1],
        [102, "Sales", "Mumbai", 3500000, 2],
        [103, "Product", "Pune", 4200000, 4],
        [104, "Marketing", "Delhi", 3000000, 6],
        [105, "Operations", "Hyderabad", 2800000, ""]
    ],
    "employees.csv": [
        ["employee_id", "name", "department_id", "salary", "hire_date", "manager_id", "bonus_eligible", "status"],
        [1, "Rajesh Kumar", 101, 75000, "2021-03-15", "", "Y", "active"],
        [2, "Priya Sharma", 102, 85000, "2020-07-22", 1, "Y", "active"],
        [3, "Amit Patel", 101, 65000, "2022-01-10", 1, "Y", "active"],
        [4, "Sneha Reddy", 103, 95000, "2019-11-05", "", "Y", "active"],
        [5, "Vikram Singh", 102, 72000, "2023-02-14", 2, "N", "active"],
        [6, "Ananya Gupta", 104, 110000, "2018-06-30", "", "Y", "active"],
        [7, "Rohan Mehta", 101, 48000, "2021-09-18", 1, "Y", "active"],
        [8, "Kavya Iyer", 103, 105000, "2020-04-25", 4, "Y", "active"],
        [9, "Arjun Nair", 102, 52000, "2022-08-12", 2, "Y", "active"],
        [10, "Meera Joshi", 104, 95000, "2021-12-03", 6, "Y", "active"],
        [11, "Sanjay Desai", 101, 45000, "2023-05-20", 1, "N", "active"],
        [12, "Divya Kapoor", 105, 0, "2023-09-01", "", "N", "inactive"],
        [13, "Karthik Rao", 103, 110000, "2019-08-17", 4, "Y", "active"],
        [14, "Pooja Agarwal", 102, 48000, "2022-03-28", 2, "Y", "active"],
        [15, "Nikhil Verma", 104, 68000, "2020-10-11", 6, "Y", "active"],
        [16, "Ritu Malhotra", 101, 46000, "2021-07-06", 1, "Y", "active"],
        [17, "Aditya Bose", 105, 0, "2023-11-15", "", "N", "inactive"],
        [18, "Shruti Pandey", 103, 70000, "2020-01-30", 4, "Y", "active"],
        [19, "Manish Ghosh", 102, 49000, "2023-04-19", 2, "N", "active"],
        [20, "Tanvi Shah", 104, 115000, "2019-03-22", 6, "Y", "active"],
        [21, "Rahul Chatterjee", "", 62000, "2022-11-08", "", "Y", "active"],
        [22, "Lakshmi Menon", 103, 108000, "2020-06-14", 4, "Y", "active"],
        [23, "Varun Saxena", 101, 47000, "2023-07-25", 1, "N", "active"],
        [24, "Nisha Kulkarni", 102, 50000, "2022-05-09", 2, "Y", "active"],
        [25, "Akash Sinha", 104, 98000, "2021-02-17", 6, "Y", "active"]
    ],
    "performance_reviews.csv": [
        ["review_id", "employee_id", "review_date", "rating", "reviewer_id"],
        [1, 2, "2023-06-15", 4.5, 1],
        [2, 3, "2023-06-20", 3.8, 1],
        [3, 5, "2023-07-10", 4.2, 2],
        [4, 7, "2023-06-18", 4.0, 1],
        [5, 8, "2023-07-05", 4.8, 4],
        [6, 9, "2023-07-12", 3.5, 2],
        [7, 10, "2023-06-25", 4.3, 6],
        [8, 13, "2023-07-08", 4.6, 4],
        [9, 14, "2023-07-15", 3.9, 2],
        [10, 15, "2023-06-30", 4.1, 6],
        [11, 16, "2023-06-22", 3.7, 1],
        [12, 18, "2023-07-03", 4.4, 4],
        [13, 22, "2023-07-20", 4.7, 4],
        [14, 24, "2023-07-18", 4.0, 2],
        [15, 25, "2023-06-28", 4.5, 6],
        [16, 3, "2024-06-20", 4.1, 1],
        [17, 5, "2024-07-10", 4.5, 2],
        [18, 8, "2024-07-05", 4.9, 4],
        [19, 9, "2024-07-12", 4.0, 2],
        [20, 10, "2024-06-25", 4.6, 6]
    ],
    "projects.csv": [
        ["project_id", "project_name", "department_id", "start_date", "end_date", "budget", "status"],
        [1, "Mobile App Redesign", 101, "2023-01-15", "2023-06-30", 800000, "completed"],
        [2, "Sales CRM Implementation", 102, "2023-03-01", "2023-09-30", 650000, "completed"],
        [3, "Product Launch Q3", 103, "2023-07-01", "2023-12-31", 1200000, "in_progress"],
        [4, "Marketing Campaign 2023", 104, "2023-02-01", "2023-11-30", 450000, "completed"],
        [5, "Cloud Migration", 101, "2023-05-01", "2024-04-30", 1500000, "in_progress"],
        [6, "Customer Analytics Platform", 103, "2023-08-01", "2024-02-29", 900000, "in_progress"],
        [7, "Regional Expansion", 102, "2023-09-01", "2024-06-30", 750000, "in_progress"],
        [8, "Brand Refresh", 104, "2023-04-01", "2023-10-31", 380000, "completed"]
    ],
    "project_assignments.csv": [
        ["assignment_id", "employee_id", "project_id", "role", "allocation_percentage", "start_date", "end_date"],
        [1, 1, 1, "Lead", 100, "2023-01-15", "2023-06-30"],
        [2, 3, 1, "Developer", 80, "2023-01-15", "2023-06-30"],
        [3, 7, 1, "Developer", 60, "2023-02-01", "2023-06-30"],
        [4, 2, 2, "Lead", 100, "2023-03-01", "2023-09-30"],
        [5, 5, 2, "Consultant", 70, "2023-03-01", "2023-09-30"],
        [6, 9, 2, "Consultant", 50, "2023-04-01", "2023-09-30"],
        [7, 4, 3, "Lead", 100, "2023-07-01", "2023-12-31"],
        [8, 8, 3, "Manager", 90, "2023-07-01", "2023-12-31"],
        [9, 13, 3, "Analyst", 80, "2023-07-01", "2023-12-31"],
        [10, 6, 4, "Lead", 100, "2023-02-01", "2023-11-30"],
        [11, 10, 4, "Specialist", 70, "2023-02-01", "2023-11-30"],
        [12, 15, 4, "Specialist", 60, "2023-03-01", "2023-11-30"],
        [13, 1, 5, "Lead", 80, "2023-05-01", "2024-04-30"],
        [14, 7, 5, "Developer", 70, "2023-06-01", "2024-04-30"],
        [15, 16, 5, "Developer", 60, "2023-05-01", "2024-04-30"],
        [16, 4, 6, "Lead", 100, "2023-08-01", "2024-02-29"],
        [17, 18, 6, "Analyst", 90, "2023-08-01", "2024-02-29"],
        [18, 22, 6, "Analyst", 50, "2023-09-01", "2024-02-29"],
        [19, 2, 7, "Lead", 100, "2023-09-01", "2024-06-30"],
        [20, 14, 7, "Consultant", 80, "2023-09-01", "2024-06-30"],
        [21, 6, 8, "Lead", 80, "2023-04-01", "2023-10-31"],
        [22, 25, 8, "Designer", 100, "2023-04-01", "2023-10-31"],
        [23, 8, 5, "Developer", 50, "2023-05-01", "2024-04-30"],
        [24, 22, 7, "Consultant", 60, "2023-09-01", "2024-06-30"]
    ]
}

print("Generating CSV files...")
for filename, rows in data.items():
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    print(f"Created {filename}")

print("\nSuccess! All 5 CSV files have been created in the current folder.")