import pandas as pd
import os

def extract_data(data_folder):
    """
    Extract data from all CSV files in the specified folder.
    """
    extracted_data = {}
    
    # List of expected files
    expected_files = [
        'departments.csv', 
        'employees.csv', 
        'performance_reviews.csv', 
        'projects.csv', 
        'project_assignments.csv'
    ]
    
    print("--- Starting Extraction Process ---")
    
    for file_name in expected_files:
        file_path = os.path.join(data_folder, file_name)
        
        # Check if file exists
        if os.path.exists(file_path):
            try:
                # Read CSV into DataFrame
                df = pd.read_csv(file_path)
                
                # Store in dictionary (remove .csv extension for key)
                table_name = file_name.replace('.csv', '')
                extracted_data[table_name] = df
                
                print(f"✓ Successfully extracted {table_name}: {len(df)} rows")
                
            except Exception as e:
                print(f"X Error reading {file_name}: {e}")
        else:
            print(f"X File not found: {file_path}")
            
    return extracted_data

# --- Small Test Block ---
# This allows us to run this script directly to test it
# --- Modified Test Block to PRINT Data ---
if __name__ == "__main__":
    # 1. Setup Path
    current_script_dir = os.path.dirname(__file__)
    project_root = os.path.dirname(current_script_dir)
    raw_data_path = os.path.join(project_root, 'data', 'raw')
    
    # 2. Run Extraction
    data = extract_data(raw_data_path)
    
    # 3. Define Output Folder
    output_folder = os.path.join(project_root, 'data', 'extractRawFiles')
    os.makedirs(output_folder, exist_ok=True)
    
    # 4. SAVE ALL DATA (The Fixed Loop)
    print("\n--- Saving Files ---")
    
    # This loop goes through every table found (employees, reviews, etc.)
    # and saves it to its own correct file name.
    for table_name, df in data.items():
        output_path = os.path.join(output_folder, f"{table_name}.csv")
        df.to_csv(output_path, index=False)
        print(f"[SUCCESS] Saved {table_name}.csv")

    if not data:
        print("\n[ERROR] No data was extracted. Check your 'data/raw' folder!")