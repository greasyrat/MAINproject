import yfinance as yf
import requests
import os
import matplotlib
import pandas as pd
from io import StringIO
import datetime
import numpy as np
#from functions import get_today_and_yesterday

import gspread
import oauth2client
#from functions import get_today_and_yesterday
from oauth2client.service_account import ServiceAccountCredentials

################### SET UP DATES , for storage  for ALL FUNCITONS #############

def get_midpoint(bin_interval):
    # Access the lower and upper bounds of the interval
    lower_bound = bin_interval.left
    upper_bound = bin_interval.right
    # Return the midpoint
    return (lower_bound + upper_bound) / 2


def get_today_and_yesterday():
    # Get today's date
    today = datetime.datetime.now().date()  # Get only the date part (year, month, day)
    
    # Calculate yesterday's date
    yesterday= today - datetime.timedelta(days=1)
    
    # Adjust if yesterday is on a weekend
    if yesterday.weekday() == 5:  # If yesterday was Saturday
        yesterday -= datetime.timedelta(days=1)  # Move to Friday
    elif yesterday.weekday() == 6:  # If yesterday was Sunday
        yesterday -= datetime.timedelta(days=2)  # Move to Friday
    
    # Return both dates as datetime.date objects
    return today, yesterday
# only for workdays , 1 day apart
today, yesterday = get_today_and_yesterday()


#_________________________________________________________________________________________
############# PREVIUOS DAY CSV                                                     FOR LATER


csv_paths = []
csv_data = [f for f in os.listdir(r'caps2025') if f.endswith('.csv')]
# Get the full path for each file
csv_files= [os.path.join(r'caps2025', f) for f in csv_data]
# Get the most recent file based on modification time
print(csv_files)
previous_file = max(csv_files, key=os.path.getmtime)
#_________________________________________________________________________________________




#_________________________________________________________________________________________
####################################     companies non US DATA collection +  #######################

url = "https://companiesmarketcap.com/?download=csv"  # Replace with the actual URL
# Step 2: Make a GET request to download the CSV
response = requests.get(url) 
csv_data = StringIO(response.text)
df = pd.DataFrame()
df = pd.read_csv(csv_data)



str_dtx = "caps2025/caps"+str(today.strftime("%Y-%m-%d"))+".csv"
############## SAVE TO CSV LATEST DATA  ############
df.to_csv(str_dtx, index=False)
#_________________________________________________________________________________________



#__________________________________________________________________________________________
############################################
USonly = df[df['country'].isin(['United States'])]
totalmrktcap = USonly['marketcap'].sum()
# NEED to aggregate marketcaps for top 6 CURRENCY entites/countires
nonUS = df[df['country'] != 'United States']
totalmrktcapnonUS = nonUS['marketcap'].sum()
#__________________________________________________________________________________________










#df = pd.read_csv(r"caps2025\caps2025-04-10.csv")
#df2 = pd.read_csv(r"caps2025\caps2025-04-09.csv")







#_________________________________________________________________________________________

df2 = pd.read_csv(previous_file)






filtered_df2 = df2[df2['country'].isin(['United States'])]
UStotcap = filtered_df2['marketcap'].sum()

PREVnonUS = df2[df2['country'] != 'United States']
nonUStotcap = PREVnonUS['marketcap'].sum()

merged_df = pd.merge(df, df2, on='Symbol', how='inner')
precentCHANGE = (merged_df['price (USD)_x']-merged_df['price (USD)_y'])/merged_df['price (USD)_y']
precentCHANGE2 = pd.DataFrame(precentCHANGE)
output1 = precentCHANGE2.join(merged_df, how='inner')

output1.rename(columns={output1.columns[0]: 'precent'}, inplace=True)
outputFINAL = output1.iloc[:, [0, 3, 4,6,5]]
############# END of DATA prep ###############

print(outputFINAL)






PREVnonUFINAL = outputFINAL[outputFINAL['country_x'] != 'United States']
nonUSmean_change = round((PREVnonUFINAL['precent'] * PREVnonUFINAL['marketcap_x']).sum() / PREVnonUFINAL['marketcap_x'].sum(),5)
nonUSmean_index = (PREVnonUFINAL['price (USD)_x'] * PREVnonUFINAL['marketcap_x']).sum() / PREVnonUFINAL['marketcap_x'].sum()



filtered_df = PREVnonUFINAL[(PREVnonUFINAL['precent'] >= -0.06) & (PREVnonUFINAL['precent'] <= 0.06)]
filtered_dfnonUSmean = (filtered_df['precent'] * filtered_df['marketcap_x']).sum() / filtered_df['marketcap_x'].sum()


print(nonUSmean_change)


UStotcap/(nonUStotcap+UStotcap)











PREvUSFINAL = outputFINAL[outputFINAL['country_x'] == 'United States']
PREVnonUFINAL = PREVnonUFINAL[PREVnonUFINAL['precent'] != 0.000]

bins = [0.06,0.05875,0.0575,0.05625,0.05375,0.0525,0.05125,0.05,0.04875,0.04625,
        0.045,0.04375,0.0425,0.04125,0.04,0.03875,0.0375,0.03625,0.035,0.03375,
        0.0325,0.03125,0.03,0.02875,0.0275,0.02625,0.025,0.02375,0.0225,0.02125,
        0.02,0.01875,0.0175,0.01625,0.015,0.01375,0.0125,0.01125,0.01,0.00875,
        0.0075,0.00625,0.005,0.00375,0.0025,0.00125,0,-0.00125,-0.0025,-0.00375,
        -0.005,-0.00625,-0.0075,-0.00875,-0.01,-0.01125,-0.0125,-0.01375,-0.015,
        -0.01625,-0.0175,-0.01875,-0.02,-0.02125,-0.0225,-0.02375,-0.025,-0.02625,
        -0.0275,-0.02875,-0.03,-0.03125,-0.0325,-0.03375,-0.035,-0.03625,-0.0375,
        -0.03875,-0.04,-0.04125,-0.0425,-0.04375,-0.045,-0.04625,-0.0475,-0.04875,
        -0.05,-0.05125,-0.0525,-0.05375,0.055,-0.05625,-0.0575,-0.05875]  

bins.reverse()
bins.sort()

# These are the edges of the bins
bin_labels = [0.05875,0.0575,0.05625,0.05375,0.0525,0.05125,0.05,0.04875,0.04625,
  
        0.045,0.04375,0.0425,0.04125,0.04,0.03875,0.0375,0.03625,0.035,0.03375,
        0.0325,0.03125,0.03,0.02875,0.0275,0.02625,0.025,0.02375,0.0225,0.02125,
        0.02,0.01875,0.0175,0.01625,0.015,0.01375,0.0125,0.01125,0.01,0.00875,
        0.0075,0.00625,0.005,0.00375,0.0025,0.00125,0,-0.00125,-0.0025,-0.00375,
        -0.005,-0.00625,-0.0075,-0.00875,-0.01,-0.01125,-0.0125,-0.01375,-0.015,
        -0.01625,-0.0175,-0.01875,-0.02,-0.02125,-0.0225,-0.02375,-0.025,-0.02625,
        -0.0275,-0.02875,-0.03,-0.03125,-0.0325,-0.03375,-0.035,-0.03625,-0.0375,
        -0.03875,-0.04,-0.04125,-0.0425,-0.04375,-0.045,-0.04625,-0.0475,-0.04875,
        -0.05,-0.05125,-0.0525,-0.05375,0.055,-0.05625,-0.0575,-0.05875]  # Labels for each bin

bin_labels.reverse()
bin_labels.sort()
#_________________________________________________________________________________________














#_________________________________________________________________________________________
df3 = pd.DataFrame(PREVnonUFINAL)
print("***********************************")
print(df3)
df3['binned'] = pd.cut(df3['precent'], bins=bins)
df_binned = df3.groupby('binned')['marketcap_x'].sum().reset_index()

print(df3)
print("***********************************")

#########  CALCULATE MEDIAN  #################
df_binned2 = df_binned




df_binned2['cumulative_weight'] = df_binned2['marketcap_x'].cumsum()
total_weight = df_binned2['marketcap_x'].sum()
# Apply the function to get midpoints
df_binned2['midpoint'] = df_binned2['binned'].apply(get_midpoint)
print(df_binned2)
# Find the row where cumulative weight exceeds half the total weight
median_idx1 = df_binned2[df_binned2['cumulative_weight'] >= total_weight/2]
median_idx1 = median_idx1.iloc[0]
val1= median_idx1['midpoint']
val11=abs((total_weight/2)- median_idx1['cumulative_weight'])

median_idx2 = df_binned2[df_binned2['cumulative_weight'] <= total_weight/2]
median_idx2 = median_idx2.iloc[-1]
val2= median_idx2['midpoint']
val22= abs((total_weight/2)-median_idx2['cumulative_weight'])
print(val11)
print(val22)


#weighted_average = (val1 * val11 + val2 * val22) / (val11 + val22)
#medianvalue = weighted_average

medianvalue = 0


























csv_y1 = "nonUSmarketcapBINNED"+ today.strftime("%Y-%m-%d")+".csv"
df_flipped = df_binned.iloc[::-1].reset_index(drop=True)






df_flipped.to_csv(csv_y1, index=False)







# Apply the function to calculate midpoints
# Apply the function to calculate midpoints



#df_flipped1['midpoint'] = df_flipped1['binned'].apply(get_midpoint)
#df_flipped1['marketcap_x'] = pd.to_numeric(df_flipped1['marketcap_x'], errors='coerce')
#df_flipped1['midpoint'] = pd.to_numeric(df_flipped1['midpoint'], errors='coerce')
#nonUSmean1 = (df_flipped1['midpoint'] * df_flipped1['marketcap_x']).sum() / df_flipped1['marketcap_x'].sum()






#_________________________________________________________________________________________




################
def get_today_and_yesterday():
    # Get today's date
    today = datetime.datetime.now().date()  # Get only the date part (year, month, day)
    
    # Calculate yesterday's date
    yesterday= today - datetime.timedelta(days=1)
    
    # Adjust if yesterday is on a weekend
    if yesterday.weekday() == 5:  # If yesterday was Saturday
        yesterday -= datetime.timedelta(days=1)  # Move to Friday
    elif yesterday.weekday() == 6:  # If yesterday was Sunday
        yesterday -= datetime.timedelta(days=2)  # Move to Friday
    
    # Return both dates as datetime.date objects
    return today, yesterday

################
today, yesterday = get_today_and_yesterday()

##############

########################### GPSREAD code ######################
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/spreadsheets',
         'https://www.googleapis.com/auth/drive.file',
         'https://www.googleapis.com/auth/drive']

# Reading Credentails from ServiceAccount Keys file
credentials = ServiceAccountCredentials.from_json_keyfile_name('cs.json', scope)
# intitialize the authorization object       
gc = gspread.authorize(credentials)
sht1 = gc.open_by_key('1rwtnGjSjvr-t6kSFgaL6x--C3QJKN42sy92ya325_8A')
####################################################



####################   SEND BINNED non US DATA  ###############



#        str_dtx = "nonUSmarketcapBINNED"+str(today.strftime("%Y-%m-%d"))+".csv"
#           df_binned = pd.DataFrame()
#          df_binned = pd.read_csv(str_dtx)



df_binned['date'] = today.strftime('%Y-%m-%d')
df_binned = df_binned.drop('midpoint', axis=1)
df_binned = df_binned.drop('cumulative_weight', axis=1)
print(df_binned)
df_binned = df_binned.applymap(lambda x: str(x) if isinstance(x, pd.Interval) else x)
print("@@@@@@@@@@@@@@@@@@@@@@@@@@@")
df_flipped = df_binned.iloc[::-1].reset_index(drop=True)


worksheet = sht1.worksheet("nonUSp%DAILY")
df_flipped = df_flipped.replace([np.nan, np.inf, -np.inf], '')
worksheet.update([df_flipped.columns.values.tolist()] + df_flipped.values.tolist())



df44 = pd.DataFrame({'': [nonUSmean_change,nonUSmean_index,filtered_dfnonUSmean]})
xxx = 'D98'
df44 = df44.replace([np.nan, np.inf, -np.inf], '')
worksheet.update(xxx,[df44.columns.values.tolist()] + df44.values.tolist())








######################### STORE DIALY DATA FOR non US #############################

csv_df = pd.read_csv(r"PROJCTalpha\nonUSdaily.csv")
# Create a new DataFrame with the values you want to replace the last row with
# Make sure the new row has the same structure (same number of columns) as the original CSV
new_values = {
    'nonUSmean_change': nonUSmean_change,  # replace with actual column name and new value
    'nonUSmean_index': nonUSmean_index, 
    'filtered_dfnonUSmean' :filtered_dfnonUSmean,
    'medianvalue' : medianvalue
}

new_row = pd.DataFrame([new_values])
# Append the new row to the DataFrame using pd.concat
csv_df = pd.concat([csv_df, new_row], ignore_index=True)
# Save the updated DataFrame back to the CSV
csv_df.to_csv(r"PROJCTalpha\nonUSdaily.csv", index=False)


worksheet = sht1.worksheet("CHARTS")
GP2price1 = 'L193'
csv_df = csv_df.replace([np.nan, np.inf, -np.inf], '')
worksheet.update(GP2price1,[csv_df.columns.values.tolist()] + csv_df.values.tolist())
















####################   SEND updated NvolTOTdaily DATA  ###############

str_dtx1 = r"PROJCTalpha\NvolTOTdaily.csv"
dailys = pd.read_csv(str_dtx1)

worksheet = sht1.worksheet("CHARTS")
GP2price = 'U193'
dailys_cleaned = dailys.fillna('')
dailys_cleaned = dailys_cleaned.replace([np.nan, np.inf, -np.inf], '')
worksheet.update(GP2price,[dailys_cleaned.columns.values.tolist()] + dailys_cleaned.values.tolist())





#______________________________________________________ PRIMER STUFF ____________________

# Load the CSV file into a DataFrame
csv_df = pd.read_csv(r"PROJCTalpha\NvolTOTdaily.csv")
# Create a new DataFrame with the values you want to replace the last row with
# Make sure the new row has the same structure (same number of columns) as the original CSV
new_value1 = 0
new_values = {
    'NvolTOT': new_value1,  # replace with actual column name and new value
    'Date': 'new_day',  # Example column for date
}

new_row = pd.DataFrame([new_values])
# Append the new row to the DataFrame using pd.concat
csv_df = pd.concat([csv_df, new_row], ignore_index=True)
# Save the updated DataFrame back to the CSV
csv_df.to_csv(r"PROJCTalpha\NvolTOTdaily.csv", index=False)




















pairs = ["EURUSD=X","MXNUSD=X","CADUSD=X",
         "BRLUSD=X","GBPUSD=X","CHFUSD=X",
         "SEKUSD=X","JPYUSD=X","KRWUSD=X",
         "HKDUSD=X","SGDUSD=X","IDRUSD=X",
         "THBUSD=X","INRUSD=X","TWDUSD=X",
         "ILSUSD=X"] ################################################## !!!!!!!!!!!!!!!!!!!!          EURUSD  CHANGED   ############################################

wieghts = ["12.464","0.470","3.050",
         "0.691","3.585","2.3",
         "1.05","5.07","1.02",
         "0.89","0.54","0.4",
         "0.470","1.9","1.7",
         "0.5"]
#pair = pairs[1]

# 12.464 T
#for pair in pairs:
 #   pair1 = str(pair) +str("USD=X")
  #  EUR = yf.download(pair1,yesterday,today)
   # EUR_percent_change_atclose = (EUR.iloc[1, 1]-EUR.iloc[0, 1])/EUR.iloc[0, 1]
    #EUR_wieght = 12.464:
#
# USE REAL TIMEDATA     COMPARE TO PREVIOUS DAY DATASET

latest_pairs_price = pd.DataFrame(columns=['PAIR', 'LAST'])

for pair in pairs:
    pairprice = yf.Ticker(pair)
    latest_priceEUR = pairprice.history(period="1d")['Close'].iloc[0]
    df1 = pd.DataFrame([[pair,latest_priceEUR]], columns=['PAIR', 'LAST'])
    latest_pairs_price = pd.concat([latest_pairs_price,df1])

latest_pairs_price.insert(2, 'wieghts', wieghts)


############################### SAVE DATA by date



for pair in pairs:
    pairprice = yf.Ticker(pair)
    hist = pairprice.history(period="1d")
    
    if hist.empty:
        print(f"Warning: No data for {pair}")
        continue  # skip this pair
    
    latest_priceEUR = hist['Close'].iloc[0]
    df1 = pd.DataFrame([[pair, latest_priceEUR]], columns=['PAIR', 'LAST'])
    latest_pairs_price = pd.concat([latest_pairs_price, df1], ignore_index=True)

print(latest_pairs_price)















# fetch prevday data to calculate precents
str_dt3 = "Pairs"+str(yesterday.strftime("%Y-%m-%d"))+".csv"
df = pd.read_csv(str_dt3)

latest_pairs_price= latest_pairs_price.reset_index(drop=True)
df= df.reset_index(drop=True)
LATEST_percent_change = pd.DataFrame(columns=['percent_diff'])    
LATEST_percent_change['percent_diff'] = ((latest_pairs_price['LAST'] - df['LAST']) / df['LAST']) * 100
LATEST_percent_change.insert(1, 'wieghts', wieghts)


LATEST_percent_change['percent_diff'] = pd.to_numeric(LATEST_percent_change['percent_diff'])
LATEST_percent_change['wieghts'] = pd.to_numeric(LATEST_percent_change['wieghts'])

# calcylted average wieghted
total_sum = LATEST_percent_change['wieghts'].sum()
nonUSpairs_weighted_average = (LATEST_percent_change['percent_diff'] *
                                LATEST_percent_change['wieghts']).sum() / LATEST_percent_change['wieghts'].sum()

# SAVE FINAL DATA POINT
latest_pairs_price['percent_diff'] = LATEST_percent_change['percent_diff']
str_dt2 = "Pairs"+str(today.strftime("%Y-%m-%d"))+".csv"
latest_pairs_price.to_csv(str_dt2, index=False)
latest_pairs_price = pd.DataFrame(columns=['PAIR', 'LAST'])

