import unittest
import pandas as pd
import transform
import validation

class TestETLPipeline(unittest.TestCase):

    def setUp(self):
        """ Runs before every test. Setup dummy data. """
        # 1. Dummy Employee Data
        self.raw_employees = pd.DataFrame({
            'employee_id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'salary': [40000, 70000, 100000], 
            'status': ['active', 'inactive', 'active'],
            'hire_date': ['2020-01-01', '2021-01-01', '2019-01-01'],
            'department_id': [101, 101, 102],
            'bonus_eligible': ['Y', 'N', 'Y']
        })

        # 2. Dummy Department Data
        self.raw_depts = pd.DataFrame({
            'department_id': [101, 102],
            'name': ['HR', 'Tech']
        })

    def test_transformation_cleaning(self):
        """ Test if cleaning filters inactive employees & adds buckets """
        cleaned = transform.clean_employee_data(self.raw_employees)
        
        # Check 1: Inactive employee 'Bob' should be removed
        self.assertEqual(len(cleaned), 2)
        self.assertNotIn(2, cleaned['employee_id'].values)

        # Check 2: Salary Buckets Logic
        # Alice (40k) -> Low
        alice = cleaned[cleaned['name'] == 'Alice'].iloc[0]
        self.assertEqual(alice['salary_bucket'], 'Low')
        # Charlie (100k) -> High
        charlie = cleaned[cleaned['name'] == 'Charlie'].iloc[0]
        self.assertEqual(charlie['salary_bucket'], 'High')

    def test_validation_logic(self):
        """ Test if validator catches missing names """
        # Create bad data (Missing Name)
        bad_data = self.raw_employees.copy()
        bad_data.loc[0, 'name'] = None 
        
        # Run validation
        issues = validation.check_completeness(bad_data, 'TestTable', 'employee_id', ['name'])
        
        # Should find 1 issue
        self.assertTrue(len(issues) > 0)
        self.assertIn("Found NULL values in required column: name", issues[0])

    def test_aggregation_logic(self):
        """ Test if Department Summary calculates averages correctly """
        clean_emp = transform.clean_employee_data(self.raw_employees)
        empty_proj = pd.DataFrame(columns=['project_id', 'department_id', 'end_date', 'budget'])
        
        # Run Aggregation
        summary = transform.create_dept_summary(clean_emp, empty_proj, self.raw_depts)
        
        # Check HR Dept (ID 101) - Only Alice is active (40k)
        hr_row = summary[summary['department_name'] == 'HR'].iloc[0]
        self.assertEqual(hr_row['total_employees'], 1)
        self.assertEqual(hr_row['avg_salary'], 40000)

if __name__ == '__main__':
    unittest.main()