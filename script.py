import pandas as pd
from datetime import datetime, timedelta

def calculate_daily_aggregates(date_str):
    file_path = f'input/{date_str}.csv'
    try:
        df = pd.read_csv(file_path)
        aggregated_data = df.groupby('email')['action'].value_counts().unstack().fillna(0).astype(int)
        aggregated_data.columns = ['create_count', 'read_count', 'update_count', 'delete_count']
        aggregated_data['email'] = aggregated_data.index
        output_file = f'intermediate/{date_str}.csv'
        aggregated_data.to_csv(output_file, index=False)
    except FileNotFoundError:
        print(f"File {file_path} not found.")

def calculate_weekly_aggregates(end_date_str):
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    start_date = end_date - timedelta(days=7)
    date_range = [start_date + timedelta(days=i) for i in range(7)]
    aggregated_data = pd.DataFrame()

    for date in date_range:
        date_str = date.strftime('%Y-%m-%d')
        file_path = f'intermediate/{date_str}.csv'
        try:
            daily_df = pd.read_csv(file_path)
            aggregated_data = pd.concat([aggregated_data, daily_df], ignore_index=True)
        except FileNotFoundError:
            print(f"File {file_path} not found.")

    aggregated_data = aggregated_data.groupby('email')[['create_count', 'read_count', 'update_count', 'delete_count']].sum().reset_index()
    output_file = f'output/{end_date_str}.csv'
    aggregated_data.to_csv(output_file, index=False)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python script.py <YYYY-mm-dd>")
        sys.exit(1)
    date_str = sys.argv
    end_date = datetime.strptime(date_str, '%Y-%m-%d')
    start_date = end_date - timedelta(days=7)
    date_range = [start_date + timedelta(days=i) for i in range(7)]

    for date in date_range:
        date_str = date.strftime('%Y-%m-%d')
        calculate_daily_aggregates(date_str)

    calculate_weekly_aggregates(sys.argv)
