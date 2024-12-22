"""
Script to pull Department of Interior employee spending data from USAspending.gov API.

The script will:
- Show progress every 1,000 records instead of every page
- Pull all FY2024 data
- Provide a sorted summary of spending by sub-agency and award type
- Save everything to a single CSV file
"""

import os
import time
import requests
import pandas as pd
from datetime import datetime

def fetch_personnel_spending(start_date, end_date):
    """
    Fetch personnel-related spending data from USAspending API for DOI
    """
    url = "https://api.usaspending.gov/api/v2/search/spending_by_transaction/"
    
    payload = {
        "filters": {
            "award_type_codes": [
                "A", "B", "C", "D",  # Contracts
                "02", "03", "04", "05",  # Grants
                "06", "10",  # Direct Payments
                "07", "08",  # Loans
                "09", "11"  # Other
            ],
            "time_period": [
                {
                    "start_date": start_date,
                    "end_date": end_date
                }
            ],
            "agencies": [
                {
                    "type": "awarding",
                    "tier": "toptier",
                    "name": "Department of the Interior"
                }
            ],
            "object_class": ["10"]  # Personnel compensation and benefits
        },
        "fields": [
            "Action Date",
            "Transaction Amount",
            "Award ID",
            "Recipient Name",
            "Awarding Agency",
            "Awarding Sub Agency",
            "Award Type"
        ],
        "page": 1,
        "limit": 100,
        "sort": "Action Date",
        "order": "desc"
    }

    all_results = []
    page = 1
    total_entries = 0
    
    while True:
        payload["page"] = page
        try:
            response = requests.post(url, json=payload)
            
            if response.status_code != 200:
                print(f"Error {response.status_code}: {response.text}")
                break
                
            data = response.json()
            if not data.get('results'):
                break
                
            all_results.extend(data['results'])
            total_entries += len(data['results'])
            
            # Only show progress every 1000 entries
            if total_entries % 1000 == 0:
                print(f"Fetched {total_entries} total records...")
            
            # Check if we've reached the last page
            if len(data['results']) < payload["limit"]:
                print(f"Final count: {total_entries} records")
                break
                
            page += 1
            time.sleep(0.5)  # Small delay between requests
            
        except requests.exceptions.RequestException as e:
            print(f"Error making request: {e}")
            break
            
    return all_results

def process_results(data):
    """Process API results into a pandas DataFrame"""
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    
    # Convert dates and amounts
    if 'Action Date' in df.columns:
        df['Action Date'] = pd.to_datetime(df['Action Date'])
    
    if 'Transaction Amount' in df.columns:
        df['Transaction Amount'] = pd.to_numeric(df['Transaction Amount'])
        df['Formatted Amount'] = df['Transaction Amount'].apply(
            lambda x: f"${x:,.2f}" if pd.notnull(x) else "$0.00"
        )
    
    return df

def main():
    # FY2024 date range
    start_date = "2023-10-01"
    end_date = "2024-09-30"
    
    print(f"\nFetching DOI personnel spending data for FY2024 ({start_date} to {end_date})")
    
    # Create output directory
    output_dir = 'output/personnel'
    os.makedirs(output_dir, exist_ok=True)
    
    # Fetch and process data
    results = fetch_personnel_spending(start_date, end_date)
    df = process_results(results)
    
    if not df.empty:
        # Save to CSV
        filename = os.path.join(output_dir, f'doi_personnel_spending_fy2024.csv')
        df.to_csv(filename, index=False)
        print(f"\nData saved to: {filename}")
        print(f"Total records: {len(df)}")
        
        # Print summary statistics
        if 'Transaction Amount' in df.columns:
            total_spending = df['Transaction Amount'].sum()
            print(f"\nTotal personnel spending: ${total_spending:,.2f}")
            
            print("\nSpending by Sub-Agency:")
            by_subagency = df.groupby('Awarding Sub Agency')['Transaction Amount'].agg(['sum', 'count'])
            by_subagency = by_subagency.sort_values('sum', ascending=False)  # Sort by amount
            for name, row in by_subagency.iterrows():
                print(f"{name:50} Count: {row['count']:,} \tAmount: ${row['sum']:,.2f}")
            
            print("\nSpending by Award Type:")
            by_award_type = df.groupby('Award Type')['Transaction Amount'].agg(['sum', 'count'])
            by_award_type = by_award_type.sort_values('sum', ascending=False)  # Sort by amount
            for name, row in by_award_type.iterrows():
                print(f"{name:50} Count: {row['count']:,} \tAmount: ${row['sum']:,.2f}")
    else:
        print("No data found")

if __name__ == "__main__":
    main()
