import netCDF4 as nc
import pandas as pd
import numpy as np
import os
import subprocess
from datetime import datetime, timedelta
import json
import argparse

def download_msm_data(start_date, end_date, save_dir):
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    current_dt = start_dt

    while current_dt <= end_dt:
        year = current_dt.strftime('%Y')
        month_day = current_dt.strftime('%m%d')
        year_dir = os.path.join(save_dir, year)
        if not os.path.exists(year_dir):
            os.makedirs(year_dir)
        
        url = f"http://database.rish.kyoto-u.ac.jp/arch/jmadata/data/gpv/netcdf/MSM-S/{year}/{month_day}.nc"
        output_path = os.path.join(year_dir, f"{month_day}.nc")

        print(f"Downloading: {url}")
        subprocess.run(["curl", "-o", output_path, url], check=True)

        current_dt += timedelta(days=1)

def extract_msm_data_to_csv(nc_file, target_lat, target_lon, output_csv):
    dataset = nc.Dataset(nc_file)
    
    lats = dataset.variables['lat'][:]
    lons = dataset.variables['lon'][:]
    
    lat_idx = np.abs(lats - target_lat).argmin()
    lon_idx = np.abs(lons - target_lon).argmin()
    
    data = {'time': []}
    
    times = dataset.variables['time'][:]
    data['time'] = nc.num2date(times, units=dataset.variables['time'].units)
    
    variables_of_interest = ['psea', 'sp', 'u', 'v', 'temp', 'rh', 'r1h', 'ncld', 'dswrf']
    for var_name in variables_of_interest:
        var = dataset.variables[var_name]
        values = var[:, lat_idx, lon_idx]
        scale_factor = getattr(var, 'scale_factor', 1.0)
        add_offset = getattr(var, 'add_offset', 0.0)
        # corrected_values = values * scale_factor + add_offset # TODO 補正をするとおかしい値になる。生存圏データベースの値はすでに補正済みということ？
        corrected_values = values

        # Convert temperature to Celsius if the variable is 'temp'
        if var_name == 'temp':
            corrected_values -= 273.15  # Convert Kelvin to Celsius

        data[var_name] = corrected_values

    # Calculate wind direction and speed
    data['wind_direction'] = (270 - np.degrees(np.arctan2(data['v'], data['u']))) % 360
    data['wind_speed'] = np.sqrt(data['u']**2 + data['v']**2)
    
    df = pd.DataFrame(data)
    df.to_csv(output_csv, index=False)
    
    print(f"Data successfully extracted to {output_csv}")

def combine_csv_files(csv_dir, combine_start_date, combine_end_date, output_csv, daily_output_dir, monthly_output_dir):
    start_dt = datetime.strptime(combine_start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(combine_end_date, '%Y-%m-%d')

    all_files = []
    for year in range(start_dt.year, end_dt.year + 1):
        year_dir = os.path.join(csv_dir, str(year))
        if os.path.exists(year_dir):
            for root, _, files in os.walk(year_dir):
                for file in files:
                    if file.endswith('.csv'):
                        file_date = datetime.strptime(f"{year}{file[:4]}", '%Y%m%d')
                        if start_dt <= file_date <= end_dt:
                            all_files.append(os.path.join(root, file))

    if not all_files:
        print(f"No CSV files found for the specified date range: {combine_start_date} to {combine_end_date}.")
        return

    try:
        all_data = pd.concat((pd.read_csv(f) for f in sorted(all_files)), ignore_index=True)
        all_data.to_csv(output_csv, index=False)
        print(f"Combined CSV saved to {output_csv}")

        # Calculate wind direction and speed for combined data
        all_data['wind_direction'] = (270 - np.degrees(np.arctan2(all_data['v'], all_data['u']))) % 360
        all_data['wind_speed'] = np.sqrt(all_data['u']**2 + all_data['v']**2)

        # Daily averages
        all_data['datetime'] = pd.to_datetime(all_data['time'])
        all_data['date'] = all_data['datetime'].dt.date
        numeric_columns = all_data.select_dtypes(include=[np.number]).columns
        daily_avg = all_data.groupby('date')[numeric_columns].mean()

        daily_output_path = os.path.join(daily_output_dir, f"{combine_start_date.replace('-', '')}-{combine_end_date.replace('-', '')}_daily.csv")
        if not os.path.exists(daily_output_dir):
            os.makedirs(daily_output_dir)
        daily_avg.to_csv(daily_output_path)
        print(f"Daily averages saved to {daily_output_path}")

        # Monthly averages
        all_data['month'] = all_data['datetime'].dt.to_period('M')
        monthly_avg = all_data.groupby('month')[numeric_columns].mean()

        monthly_output_path = os.path.join(monthly_output_dir, f"{combine_start_date.replace('-', '')}-{combine_end_date.replace('-', '')}_monthly.csv")
        if not os.path.exists(monthly_output_dir):
            os.makedirs(monthly_output_dir)
        monthly_avg.to_csv(monthly_output_path)
        print(f"Monthly averages saved to {monthly_output_path}")

    except Exception as e:
        print(f"Error while combining CSV files: {e}")

def process_download_and_csv(config_file):
    with open(config_file, 'r') as f:
        config = json.load(f)

    start_date = config['download_start_date']
    end_date = config['download_end_date']
    target_lat = config['target_latitude']
    target_lon = config['target_longitude']
    output_dir = config['output_directory']

    save_dir = os.path.join(output_dir, 'netcdf')
    csv_dir = os.path.join(output_dir, 'csv')

    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)

    download_msm_data(start_date, end_date, save_dir)

    for year_dir in sorted(os.listdir(save_dir)):
        year_path = os.path.join(save_dir, year_dir)
        year_csv_dir = os.path.join(csv_dir, year_dir)
        if not os.path.exists(year_csv_dir):
            os.makedirs(year_csv_dir)
        if os.path.isdir(year_path):
            for nc_file in sorted(os.listdir(year_path)):
                if nc_file.endswith('.nc'):
                    nc_file_path = os.path.join(year_path, nc_file)
                    month_day = os.path.splitext(nc_file)[0]
                    csv_file_path = os.path.join(year_csv_dir, f"{month_day}.csv")
                    extract_msm_data_to_csv(nc_file_path, target_lat, target_lon, csv_file_path)

def process_combine_csv(config_file):
    with open(config_file, 'r') as f:
        config = json.load(f)

    combine_start_date = config['combine_start_date']
    combine_end_date = config['combine_end_date']
    output_dir = config['output_directory']
    csv_dir = os.path.join(output_dir, 'csv')
    combined_csv_dir = os.path.join(csv_dir, 'combined')
    daily_output_dir = os.path.join(csv_dir, 'daily')
    monthly_output_dir = os.path.join(csv_dir, 'monthly')
    combined_csv_path = os.path.join(combined_csv_dir, f"{combine_start_date.replace('-', '')}-{combine_end_date.replace('-', '')}.csv")

    if not os.path.exists(combined_csv_dir):
        os.makedirs(combined_csv_dir)

    combine_csv_files(csv_dir, combine_start_date, combine_end_date, combined_csv_path, daily_output_dir, monthly_output_dir)

def main():
    parser = argparse.ArgumentParser(description="MSM data processing script.")
    parser.add_argument("command", choices=["download", "combine"], help="Specify the operation: 'download' to download and process data, 'combine' to combine CSVs.")
    parser.add_argument("config", help="Path to the JSON configuration file.")

    args = parser.parse_args()

    if args.command == "download":
        process_download_and_csv(args.config)
    elif args.command == "combine":
        process_combine_csv(args.config)

if __name__ == "__main__":
    main()
