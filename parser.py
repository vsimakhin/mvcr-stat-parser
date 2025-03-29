import pandas as pd
import os
from collections import defaultdict
import re
import json


def extract_date_from_filename(filename):
    """Extract and format date from filename.
    """
    pattern1 = re.compile(r"(\d{2})[-._](\d{4})_TAB_internet_stav_k_.*\.(xls|xlsx)")
    pattern2 = re.compile(r"STAV_K_\d{1,2}[-._](\d{1,2})[-._](\d{4})\.(xls|xlsx)")

    match1 = pattern1.search(filename)
    match2 = pattern2.search(filename)

    if match1:
        return f"{match1.group(1)}.{match1.group(2)}"
    elif match2:
        return f"{match2.group(1).zfill(2)}.{match2.group(2)}"

    return None


def find_stp_row(df):
    """Find the row index containing 'STP'."""
    for idx, row in df.iterrows():
        if any('STP' in str(cell) for cell in row if pd.notna(cell)):
            return idx + 1
    return None


def deduplicate_country_names(country):
    """Deduplicate country names."""
    replacements = {
        "Ruská federace": "Rusko",
    }

    return replacements.get(country, country)


def parse_excel_file(file):
    """Parse a single Excel file and return the extracted data."""
    date = extract_date_from_filename(file)
    if not date:
        print(f"Warning: Could not extract date from filename {file}")
        return {}
    
    print(f"Processing file: {file}")
    
    # Read only necessary parts of the Excel file
    try:
        df = pd.read_excel(f"./source/{file}", header=None)
    except Exception as e:
        print(f"Error reading file {file}: {e}")
        return {}
    
    stp_row = find_stp_row(df)
    if stp_row is None:
        print(f"Warning: No 'STP' row found in {file}")
        return {}
    
    # Extract relevant data
    data = df.iloc[stp_row+1:].reset_index(drop=True)
    if data.empty:
        return {}
    
    # Only select needed columns to reduce memory usage
    columns = data.columns.tolist()
    selected_columns = columns[:2] + columns[-3:]
    data = data[selected_columns]
    
    file_data = {}
    current_country = None
    
    # Process rows until we meet "CELKEM"
    for _, row in data.iterrows():
        # Check for end marker
        if pd.notna(row[0]) and "CELKEM" in str(row[0]).upper():
            break
        
        row_values = row.values
        
        # Update country if not NaN
        if pd.notna(row_values[0]):
            current_country = deduplicate_country_names(row_values[0])
        
        # Skip rows with no country
        if current_country is None or pd.isna(row_values[1]):
            continue
            
        try:
            residence_type = str(row_values[1]).lower()
            
            count_data = {
                "muži": int(0 if pd.isna(row_values[2]) else row_values[2]),
                "ženy": int(0 if pd.isna(row_values[3]) else row_values[3]),
                "celkem": int(0 if pd.isna(row_values[4]) else row_values[4])
            }
            
            # Initialize nested dictionaries as needed
            if current_country not in file_data:
                file_data[current_country] = {date: {}}
            elif date not in file_data[current_country]:
                file_data[current_country][date] = {}
                
            file_data[current_country][date][residence_type] = count_data
            
        except (ValueError, IndexError) as e:
            print(f"Error processing row {row_values}: {e}")
            continue
    
    return file_data


def calculate_totals(parsed_data):
    """Calculate totals for each country and date."""
    for country, country_data in parsed_data.items():
        for date, date_data in country_data.items():
            residence_types = list(date_data.keys())
            
            # Skip if we already have calculated totals
            if "total" in residence_types:
                continue
                
            totals = {"muži": 0, "ženy": 0, "celkem": 0}
            
            for residence_type in residence_types:
                for key in totals:
                    totals[key] += date_data[residence_type].get(key, 0)
            
            parsed_data[country][date]["total"] = totals
    
    return parsed_data


def main():
    """Main function to parse Excel files and calculate statistics."""
    # Get all xlsx files in the current directory
    xlsx_files = [file for file in os.listdir("./source") if file.endswith(('.xls', '.xlsx'))]
    if not xlsx_files:
        print("No Excel files found in the current directory")
        return
    
    # Use defaultdict to simplify nested dictionary creation
    parsed_data = defaultdict(dict)
    
    # Process each Excel file
    for file in xlsx_files:
        file_data = parse_excel_file(file)
        
        # Merge file data into the main parsed_data dictionary
        for country, country_data in file_data.items():
            for date, date_data in country_data.items():
                if date not in parsed_data[country]:
                    parsed_data[country][date] = {}
                parsed_data[country][date].update(date_data)
    
    # Convert defaultdict back to regular dict
    parsed_data = dict(parsed_data)
    
    # Calculate totals
    parsed_data = calculate_totals(parsed_data)
    
    # Sort dates for each country
    sorted_data = {}
    for country, country_data in parsed_data.items():
        # Sort dates (DD.YYYY format) within this country
        sorted_dates = sorted(country_data.keys(), 
                             key=lambda x: (int(x.split('.')[1]), int(x.split('.')[0])))
        
        # Create a new ordered dictionary for this country
        sorted_data[country] = {date: country_data[date] for date in sorted_dates}

    # Save the parsed data to a json file
    with open("./output/parsed_data_formatted.json", 'w', encoding='utf-8') as f:
        json.dump(sorted_data, f, indent=4, ensure_ascii=False)

    with open("./output/parsed_data_raw.json", 'w', encoding='utf-8') as f:
        json.dump(sorted_data, f, ensure_ascii=False)

    return sorted_data


if __name__ == "__main__":
    main()