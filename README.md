# Employee Analytics ETL Pipeline

## 📌 Project Purpose

This project implements a robust **Extract, Transform, Load (ETL)** pipeline using Python and MySQL. It processes raw HR data to generate insights on employee performance, project utilization, and department budgets. It demonstrates modern Data Engineering practices including automated data quality checks and star-schema modeling.

## 🏗 Architecture

1. **Extract**: Ingests raw CSV files from `data/extractRawFiles/`.
2. **Transform**:
   - Cleanses data (removes inactive users, handles missing values).
   - Engineers features (Salary Buckets, Tenure, Performance Categories).
   - Resolves "Ghost Departments" by mapping unknown IDs to "Unknown".
3. **Validate**: Automatically checks data constraints (Completeness, Uniqueness, Referential Integrity).
4. **Load**: Loads processed data into a MySQL Star Schema (`dim_employees`, `fact_reviews`, etc.).
5. **Report**: Generates a text summary in `reports/` with execution stats and key insights.

## 🚀 Setup Instructions

### Prerequisites

- Python 3.10+
- MySQL Server 8.0+

### Installation

1. Clone the repository.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
