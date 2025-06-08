

import os
import datetime as dt
from datetime import datetime, timedelta
import pickle
import pandas as pd
import numpy
import math
import numpy as np
import time

from oauth2client.service_account import ServiceAccountCredentials
import gspread

def authenticate():
    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive.file',
         'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('cs.json', scope)
    client = gspread.authorize(creds)
    return client







exe_path = r"C:/Users/DANNY/Desktop/extractcsvfromFOREXFACTORY.exe"
# Run the .exe file
os.system(exe_path)

time.sleep(4)


# Define the folder path
folder_path = r"C:\Users\DANNY\Desktop\projectsMAIN\forexfactoryweeks"
# List ll CSV files in the directory
csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
# Get the full paths of all CSV files
csv_file_paths = [os.path.join(folder_path, f) for f in csv_files]
# Fin the latest CSV file based on modification time
latest_file = max(csv_file_paths, key=os.path.getmtime)
# Load the latest CSV file into a DataFrame
df = pd.read_csv(latest_file)


filtered_df = df[df['Country'] == "USD"]
filtered_df2 = filtered_df[filtered_df['Title'] == "Unemployment Rate"]
filtered_df3 = filtered_df[filtered_df['Title'] == "Core CPI m/m"]
filtered_df4 = filtered_df[filtered_df['Title'] == "Core PCE Price Index m/m"]
filtered_df5 = filtered_df[filtered_df['Title'] == "Federal Funds Rate"]


filtered_df11 = df[df['Country'] == "EUR"]
filtered_df12 = filtered_df11[filtered_df11['Title'] == "ECB Press Conference"]

filtered_df111 = df[df['Country'] == "JPY"]
filtered_df122 = filtered_df111[filtered_df111['Title'] == "BOJ Policy Rate"]



result = pd.concat([filtered_df2, filtered_df3,filtered_df4,filtered_df5,filtered_df12,filtered_df122], ignore_index=True)

result['Date'] = pd.to_datetime(result['Date'], errors='coerce')
result['Date'] = result['Date'].dt.strftime('%d-%m-%Y')
result = result[['Date','Title','Country','Previous']]



#############################   SEND TO SPREADSHEETS   #################################

client = authenticate()


sht1 = client.open_by_key('1rwtnGjSjvr-t6kSFgaL6x--C3QJKN42sy92ya325_8A')
worksheet = sht1.worksheet("spyCALANDER(weeklyCALANDERscrape.py)")

            
result = result.replace([np.nan, np.inf, -np.inf], '')
worksheet.update("A18",[result.columns.values.tolist()] + result.values.tolist())

