"""
Use https://api.usaspending.gov/ to pull govt spending data. Shows actual spending, not awards, for a period of time.
"""

import os
import sys
import time
import requests
import pandas as pd
from datetime import datetime, timedelta

def get_week_ranges(start_date, end_date):
    """Generate weekly date ranges between start and end dates"""
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    current = start
    while current < end:
        week_end = min(current + timedelta(days=6), end)
        yield (
            current.strftime('%Y-%m-%d'),
            week_end.strftime('%Y-%m-%d')
        )
        current = week_end + timedelta(days=1)

def fetch_award_history(start_date, end_date):
    """Fetch award transaction history for a specific date range"""
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
            ]
        },
        "fields": [
            "Award ID",
            "Mod",
            "Recipient Name",
            "Action Date",
            "Transaction Amount",
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
    
    while True:
        payload["page"] = page
        try:
            response = requests.post(url, json=payload)
            if response.status_code != 200:
                print(f"Error {response.status_code}: {response.text}")
                break
                
            data = response.json()
            if not data['results']:
                break
                
            all_results.extend(data['results'])
            
            # Check if we've reached the last page
            if len(data['results']) < payload["limit"]:
                break
                
            page += 1
            
        except requests.exceptions.RequestException as e:
            print(f"Error making request: {e}")
            break
            
    return all_results

def process_results(data):
    """Process API results into a pandas DataFrame"""
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    
    # Convert action_date to datetime
    if 'Action Date' in df.columns:
        df['Action Date'] = pd.to_datetime(df['Action Date'])
    
    # Format currency values
    if 'Transaction Amount' in df.columns:
        df['Transaction Amount'] = pd.to_numeric(df['Transaction Amount'])
        df['Amount'] = df['Transaction Amount'].apply(
            lambda x: f"${x:,.2f}" if pd.notnull(x) else "$0.00"
        )
    
    return df

def main():
    # FY2024 date range
    start_date = "2023-10-01"
    end_date = "2024-09-30"
    
    output_dir = 'output/fy2024'
    os.makedirs(output_dir, exist_ok=True)
    
    total_weeks = sum(1 for _ in get_week_ranges(start_date, end_date))
    print(f"\nProcessing FY2024 data in {total_weeks} weekly chunks")
    
    all_data = []
    for week_num, (week_start, week_end) in enumerate(get_week_ranges(start_date, end_date), 1):
        print(f"\nWeek {week_num}/{total_weeks}: {week_start} to {week_end}")
        
        data = fetch_award_history(week_start, week_end)
        if data:
            df = process_results(data)
            if not df.empty:
                print(f"Found {len(df)} transactions")
                all_data.append(df)
                
                # Save weekly chunk
                filename = os.path.join(output_dir, f'doi_awards_week_{week_num}.csv')
                df.to_csv(filename, index=False)
                print(f"Saved to: {filename}")
            else:
                print("No transactions found for this week")
        else:
            print("Failed to fetch data for this week")
            
        # Optional: Add a small delay between requests to be nice to the API
        time.sleep(1)
    
    # Combine all weeks and save total
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        total_filename = os.path.join(output_dir, 'doi_awards_fy2024_complete.csv')
        final_df.to_csv(total_filename, index=False)
        print(f"\nComplete FY2024 dataset saved to: {total_filename}")
        print(f"Total transactions: {len(final_df)}")
        
        # Summary statistics
        total_amount = final_df['Transaction Amount'].sum()
        print(f"Total amount: ${total_amount:,.2f}")
        print("\nMonthly Summary:")
        monthly = final_df.groupby(final_df['Action Date'].dt.to_period('M'))['Transaction Amount'].sum()
        for period, amount in monthly.items():
            print(f"{period}: ${amount:,.2f}")
            
        print("\nSpending by Award Type:")
        award_type_map = {
            'A': 'Contracts', 'B': 'Contracts', 'C': 'Contracts', 'D': 'Contracts',
            '02': 'Grants', '03': 'Grants', '04': 'Grants', '05': 'Grants',
            '06': 'Direct Payments', '10': 'Direct Payments',
            '07': 'Loans', '08': 'Loans',
            '09': 'Other', '11': 'Other'
        }
        final_df['Award Category'] = final_df['Award Type'].map(award_type_map)
        category_summary = final_df.groupby('Award Category')['Transaction Amount'].agg(['sum', 'count'])
        for category, row in category_summary.iterrows():
            print(f"{category:15} Count: {row['count']:,} \tAmount: ${row['sum']:,.2f}")

if __name__ == "__main__":
    main()
