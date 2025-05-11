#imports
import requests
import json
import csv
import pandas as pd
import numpy as np
import threading
import os
from datetime import date, datetime, timedelta
import concurrent.futures
import time
import pickle
import queue
#disable ssl warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import math


from auth import authenticate







import gspread
import oauth2client
from oauth2client.service_account import ServiceAccountCredentials







def replace_nan_in_dataframe(df):
    # Replace NaN with None
    df_filled = df.applymap(lambda x: None if isinstance(x, float) and (math.isnan(x) or math.isinf(x)) else x)
    return df_filled










'''to dp list:
    -add a feature to check auth status
    -separate the data gathering and the data processing into different threads
    -add asyncronous data gathering functionality
    -add print statements to show the progress of the gathering functions
    -add yestterdays end day distro to the dashboard
    ---read from yesterdays nvols_vols_and_bins directory aand generate it at the appropriate position somehow
    -add the last 30 min distro to the dashboard  
    ---every min,save the time and start accumulating data for that minute, then at the end of the minute, stop accumulating and put it in a queue, next min do the same thing, 30 min after the first min in the queue, de-bin that minute.
    -add a bunch of comments for clarity and for dannys resume
    -add a feature to the dash that allowes to scroll the distro back and forth in time
    -add a feature where at the end of the day it continues to update dash till the 23:00 data is included
    -make a distribution scroller that allows you to scroll through the distro over time
    '''
global nvols_vols_and_bins  #global variable to store the distribution data, the previous volumes and the spx data
global shsred_queue #global variable to store the queue that will be used to send the data to the dashboard
shared_queue = queue.Queue() #create a thread safe queue to store the data and send it to the dashboard

with open('sp500_ticks.csv', 'r') as file:  #read the sp500 tickers from a csv file
    reader = csv.reader(file)
    sp500_conids = []
    for i, row in enumerate(reader):        
        sp500_conids.append(row[2])

# this function cuts the conids list into a string that can be used in the api call
def cut_conids(x,y): 
    conid = sp500_conids[x:y]
    conid = "conids="+",".join(conid)
    return conid

MAX_RETRIES = 5  # Define a maximum number of retries for reauthentication

#this function saves the nvols_vols_and_bins data to a pickle file
def save_nvols_vols_and_bins(data, directory):  # Save the data to a pickle file
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")    #get the current time
    filepath = os.path.join(directory, f"nvols_vols_and_bins_{timestamp}.pkl")  #create a path to the pickle file
    with open(filepath, 'wb') as file: #write the data to the pickle file
        pickle.dump(data, file, protocol=pickle.HIGHEST_PROTOCOL)  #pickle the data to the file

#this function loads the latest data from the nvols_vols_and_bins directory in case the program crashes
def load_previous_data(directory):    # Load the file that came right before the latest data from the nvols_vols_and_bins directory
    print("loading previous data...")
    files = [f for f in os.listdir(directory) if f.endswith('.pkl')]    #get all the pickle files in the directory
    if len(files) < 2:  #if there are less than 2 pickle files in the directory, return None
        print("no previous data")
        return None
    
    full_paths = [os.path.join(directory, f) for f in files]    #get the full paths of the pickle files

    latest_file = max(full_paths, key=os.path.getctime)   #get the path of the latest file
    previous_files = sorted(full_paths, key=os.path.getctime)[:-1]  #get all files except the latest one and sort them by creation time
    previous_file = previous_files[-1] if previous_files else None  #get the path of the file that came right before the latest file

    if previous_file:
        with open(previous_file, 'rb') as file:   #open the previous file
            return pickle.load(file)    #load the data from the previous file
    else:
        return None
    
#this function is used to get the pickle file from m minutes ago
'''def get_previous_pickle_file(directory, minutes_ago):  # Get the pickle file from m minutes ago
    files = [f for f in os.listdir(directory) if f.endswith('.pkl')]    #get all the pickle files in the directory
    if len(files) < 2:  #if there are less than 2 pickle files in the directory, return None
        return None
    full_paths = [os.path.join(directory, f) for f in files]    #get the full paths of the pickle files
    previous_files = sorted(full_paths, key=os.path.getctime)[:-1]  #get all files except the latest one and sort them by creation time
    # get the file that was created m minutes ago
    previous_file = [f for f in previous_files if datetime.now() - datetime.fromtimestamp(os.path.getctime(f)) > timedelta(minutes=minutes_ago)]'''

#defining the error classes
class ReauthenticationError(Exception):
    pass

class QueryError(Exception):
    pass

class ValidationError(Exception):
    pass

#this function reauthenticates the api session
def reauth():
    base_url = "https://localhost:5000/v1/api"
    endpoint = "iserver/reauthenticate"
    request_url = "/".join([base_url, endpoint])
    response = requests.post(url=request_url, verify=False)
    if response.status_code == 200:
        print("Reauthenticated successfully.")
    else:
        error_message = f"Failed to reauthenticate: {response.status_code}\n{response.text}"
        raise ReauthenticationError(error_message)
reauth()







#this function return the status of the api session
def confirmStatus():
    base_url = "https://localhost:5000/v1/api"
    endpoint = "iserver/auth/status"
    auth_req = requests.get(f"{base_url}/{endpoint}", verify=False)
    print(auth_req)
    print(auth_req.text)
    return auth_req.status_code





confirmStatus()

#this function validates the api session
def validate_session():
    base_url = "https://localhost:5000/v1/api"
    endpoint = "sso/validate"
    request_url = "/".join([base_url, endpoint])
    response = requests.get(url=request_url, verify=False)
    if response.status_code == 200:
        print("Session validated successfully.")
    else:
        print(f"Failed to validate session: {response.status_code}\n{response.text}")
        print(response.text)
validate_session()








#this function accesses the account endpoint which is necessary to access the market data endpoint
def access_account_endpoint():
    base_url = "https://localhost:5000/v1/api"
    endpoint = "iserver/accounts"
    request_url = "/".join([base_url, endpoint])
    response = requests.get(url=request_url, verify=False)
    if response.status_code == 200:
        print("Account endpoint accessed successfully.")
    else:
        print(f"Failed to access account endpoint: {response.status_code}\n{response.text}")
        print(response.text)
access_account_endpoint()








######################these functions handle the reauthentication, validation and account endpoint access errors######################
def handle_reauthentication(retry_count=0):
    if retry_count >= MAX_RETRIES:
        print("Maximum reauthentication attempts reached.")
        return
    print("Reauthenticating...")
    try:
        reauth()
    except ReauthenticationError:
        handle_reauthentication(retry_count + 1)

def handle_validation(retry_count=0):
    if retry_count >= MAX_RETRIES:
        print("Maximum validation attempts reached.")
        return
    print("Validating session...")
    try:
        reauth()
        validate_session()
    except:
        print("Failed to validate session")
        handle_validation(retry_count + 1)

def handle_account_endpoint(retry_count=0):
    if retry_count >= MAX_RETRIES:
        print("Maximum account endpoint access attempts reached.")
        return
    print("Accessing account endpoint...")
    try:
        reauth()
        time.sleep(1)
        validate_session()
        time.sleep(1)
        access_account_endpoint()
    except:
        print("Failed to access account endpoint")
        handle_account_endpoint(retry_count + 1)

# this function is the centralized error handler
def handle_error(error_type=ReauthenticationError, retry_count=0):
    if error_type == ReauthenticationError:
        handle_reauthentication(retry_count)
    elif error_type == ValidationError:
        handle_validation(retry_count)
    elif error_type == QueryError:
        handle_account_endpoint(retry_count)

######################these functions gather the data##########################

def market_snapshot(x,y): #this function recieves 2 arguments, x and y, which are the start and end indices of the conids list, respectively and returns the market data
    base_url = "https://localhost:5000/v1/api"
    endpoint = "iserver/marketdata/snapshot"

    #                 ADD 

    # 7289 - Market Cap.
    fields = "fields=55,31,83,7762,7289"

    
    conid = cut_conids(x,y)
    # Updated this line to include a "/" between base_url and endpoint
    request_url = "/".join([base_url, endpoint]) + "?" + conid + "&" + fields
    md_req = requests.get(url=request_url, verify=False)
    # Added a check for a successful response before attempting to parse JSON
    if md_req.status_code == 200:
        md_json = md_req.json()
        return(md_json)
    if md_req.status_code == 500 and "Please query /accounts first" in md_req.text:
        raise QueryError(f'Failed to retrieve data: {md_req.status_code}\n{md_req.text}')
    elif md_req.status_code == 400 and "Validation error" in md_req.text:
        raise ValidationError(f'Failed to retrieve data: {md_req.status_code}\n{md_req.text}')
    elif md_req.status_code == 401:
        raise ReauthenticationError(f'Failed to access account endpoint: {md_req.status_code}\n{md_req.text}')


















#this function is used to make sure the snapshot function doesnt time out and to make sure the data is valid
MAX_RETRIES = 7
def run_with_timeout(func, timeout=15, *args):
    retries = 0
    while retries < MAX_RETRIES:
        print("try", retries)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = executor.submit(func, *args)
            try:
                result = future.result(timeout=timeout)
                
                 # Check if the result is an empty DataFrame
                if isinstance(result, pd.DataFrame) and result.empty:
                    print("Empty DataFrame received! Restarting...")
                    retries += 1
                    continue  # Continue to the next round in the while loop

                # Check if the result is a list and if the first dict in the list has fewer than 10 keys
                if (isinstance(result, list) and result and isinstance(result[0], dict) and len(result[0]) < 5) or result == None:
                    time.sleep(10)
                    print("The first dictionary has fewer than 5 keys! Restarting...")
                    retries += 1
                    continue  # Continue to the next round in the while loop
                
                return result
                
            except (ReauthenticationError, ValidationError, QueryError) as e:
                print(e)
                handle_error(type(e))
                retries += 1
            except concurrent.futures.TimeoutError:
                print("Function took too long! Restarting...")
                retries += 1

data = run_with_timeout(market_snapshot,15,0,416)
######################these functions handle the data gathering and processing threads##########################
def get_spx_close(spx_data): #this function recieves the spx data and returns yesterdays close price which is calculated from the current spx price and the %change
    spx_close = spx_data['Last'].item()/(1+spx_data['percent_change'].item()/100)
    return spx_close
######################this function initializes the script ##########################
def init():
    print("initializing session...") #initialize the session
    for i in range(4):
        reauth()    #reauthenticate 4 times (just to be safe)
        time.sleep(3)   #sleep for 5 seconds between each reauthentication
    today = date.today().strftime("%Y-%m-%d")   #get the current date
    # Naming the new directories with the current date as the name of the directory to store the data
    raw_data_directory = f"data_{today}"
    nvols_vols_and_bins_directory = f"nvols_vols_and_bins_{today}"
    # Create the directories if they don't exist
    if not os.path.exists(nvols_vols_and_bins_directory):
        os.makedirs(nvols_vols_and_bins_directory)
    if not os.path.exists(raw_data_directory):
        os.makedirs(raw_data_directory)




    # Load the latest data from the nvols_vols_and_bins directory
    nvols_vols_and_bins = load_previous_data(nvols_vols_and_bins_directory)
    # If there is no data, initialize the data





    #############################################################################                                                !      !     !       !
    if nvols_vols_and_bins == None:
        market_snapshot(0, num_ticks) #initialize the data subscription
        init_spx = run_with_timeout(market_snapshot,15,0,1) #get the initial spx data











        #convert the spx data to a dataframe and rename the columns
        init_spx = pd.DataFrame(init_spx)
        init_spx.rename(columns={'31': 'Last', '55': 'Symbol', '83': 'percent_change', '7762': 'Volume'}, inplace=True)
        #print(init_spx['percent_change'])
        #init_spx['cap']= init_spx['cap'].str.replace('B', '').str.replace(',', '').replace('', np.nan).astype(float)
        if datetime.now().time() > datetime.strptime("00:00:00", "%H:%M:%S").time():
            init_spx['percent_change'] = init_spx['percent_change'].astype(float) #convert the %change to a float
            init_spx['Last'] = pd.to_numeric(init_spx['Last']) #convert the last price to a float
            #round the spx data to the closest intiger that's a multiple of 5
            spx_close = get_spx_close(init_spx)# the problem is here
        else:
            spx_close = float(['Last'][0])
        
        spx_close_round = round(spx_close/5)*5
        
        spx_series = {'time': [],'last': [], 'percent_change':[]}
        standard_deviation_series = {'stds': [], 'time': []}
        prev_volumes = np.zeros(num_ticks-1)
        total_nvols = np.zeros(num_ticks-1)
        low_res_bins = np.arange(spx_close_round-300,spx_close_round+300,5)
        low_res_nvols_in_bins = np.zeros(len(low_res_bins))

        #print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")


        mid_res_bins = np.arange(spx_close_round-300,spx_close_round+300,2.5)

        #bins = np.linspace(-0.1, 0.1, 480)
        #print(bins)
        #print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")

        mid_res_nvols_in_bins = np.zeros(len(mid_res_bins))


        #origional    mid_res_nvols_in_bins = np.zeros(len(mid_res_bins))
        #start = 0
        #end = 1  # adjust this as needed for your data range
        #step = 0.0004166666667
        #bins = np.arange(start, end + step, step)
        #print(bins)
        #mid_res_nvols_in_bins = np.zeros(len(bins))

        Pdata_res_bins = np.arange(spx_close_round-300,spx_close_round+300,2.5)
        Pdata_res_nvols_in_bins = np.zeros(len(Pdata_res_bins))


        

        high_res_bins = np.arange(spx_close_round-300,spx_close_round+300,1)
        high_res_nvols_in_bins = np.zeros(len(high_res_bins))
        ultra_res_bins = np.arange(spx_close_round-300,spx_close_round+300,0.1)
        ultra_res_nvols_in_bins = np.zeros(len(ultra_res_bins))
        nvols_vols_and_bins = {'SPX': spx_series,'SPX_close':spx_close,'standard_deviation':standard_deviation_series,'total_nvols':total_nvols, 'prev_volumes':prev_volumes, 
                            'low_res_bins': low_res_bins, 'low_res_nvols': low_res_nvols_in_bins, 'low_res_nvols_diff': np.zeros(len(low_res_bins)),
                            'mid_res_bins': mid_res_bins, 'mid_res_nvols': mid_res_nvols_in_bins, 'mid_res_nvols_diff': np.zeros(len(mid_res_bins)),
                            'Pdata_res_bins': Pdata_res_bins, 'Pdata_res_nvols_in_bins': Pdata_res_nvols_in_bins, 'Pdata_res_nvols_diff': np.zeros(len(Pdata_res_bins)),
                            'high_res_bins': high_res_bins, 'high_res_nvols': high_res_nvols_in_bins, 'high_res_nvols_diff': np.zeros(len(high_res_bins)),
                            'ultra_res_bins': ultra_res_bins, 'ultra_res_nvols': ultra_res_nvols_in_bins, 'ultra_res_nvols_diff': np.zeros(len(ultra_res_bins))} 
    
    print("init done")
    return nvols_vols_and_bins, raw_data_directory, nvols_vols_and_bins_directory









#######################this function pre-processes the data before binning and displaying##########################
global multipliers_vector
def data_prep(data,nvols_vols_and_bins,num_batch):
    #vol_multipliers = {'TSLA':0.2,'NVDA':0.4,'AMD':0.6,'AMZN':0.85}            
    orig_df = pd.DataFrame(data)   #convert the data to a dataframe
    orig_df.rename(columns={'31': 'Last', '55': 'Symbol', '83': 'percent_change', '7762': 'Volume','7289':'cap'}, inplace=True) #rename the columns
    #separate the first row of the dataframe (SPX) into a separate dataframe called spx, then remove it from the original dataframe, while keeoing only the percent change column in the second dataframe 
    #print(orig_df.columns)
    #print(orig_df['percent_change'])







    #                                                  # MUST SORT BY CAP SIZE



    capsDATA = pd.DataFrame(orig_df[['percent_change','cap']])
    capsDATA['percent_change'] = capsDATA['percent_change'].astype(float)
    capsDATA['cap'] = capsDATA['cap'].str.replace('B', '').replace('', 0).str.replace(',', '').astype(float)
    capsDATA = capsDATA.sort_values(by='cap', ascending=False).reset_index(drop=True)
    capsDATA = capsDATA.dropna()

    print(capsDATA)


    

    df_sorted1 = capsDATA.sort_values(by='percent_change')
    # Calculate the cumulative sum of the weights
    df_sorted1['cum_weight'] = df_sorted1['cap'].cumsum()
    # Find the total sum of weights
    total_weight1 = df_sorted1['cap'].sum()
    # Find the weighted median: locate the value where the cumulative weight is >= half of the total weight
    min1mediaCAPS = df_sorted1.loc[df_sorted1['cum_weight'] >= total_weight1 / 2, 'percent_change'].iloc[0]



    min1meanCAPS = (capsDATA['percent_change'] * capsDATA['cap']).sum() / capsDATA['cap'].sum()













    spx = orig_df.iloc[0:1].copy()
    spx = spx                                                                                                                                                                                                             [['_updated', 'percent_change', 'Last']] #keep only the percent change and volume columns
    nvols_vols_and_bins['SPX']['time'].append(datetime.now())   #add the time to the spx series
    nvols_vols_and_bins['SPX']['last'].append(float(spx['Last'][0])) #add the last price to the spx series
    nvols_vols_and_bins['SPX']['percent_change'].append(spx['percent_change'].item()/100)   #add the %change to the spx series



    df = orig_df.iloc[1:].copy() #separaate the second dataframe containing all the stocks data from the original dataframe
    # change marketcap values to numbers, ALL OF THEM ARE IN Bills so just remove :B:

    # print(df['cap'])
    df['cap']= df['cap'].str.replace('B', '').replace('', 0).str.replace(',', '').astype(float)

    # print(df['cap'])
    df['percent_change'] = df['percent_change'].astype(float) #convert the %change column to float
    df['percent_change1'] = df['percent_change'].astype(float)
    #print(df['percent_change'])
    df['Volume'] = df['Volume'].replace('', 0).astype(float) #convert the volume column to float
    momentary_vol = np.subtract(df['Volume'].to_numpy(), nvols_vols_and_bins['prev_volumes']) #calculate the change in volume
    #check if the company is in the vol_multipliers dictionary and multiply the volume by the appropriate multiplier
    momentary_vol_norm = np.multiply(momentary_vol,multipliers_vector)
    norm_non_norm_diff = np.subtract(momentary_vol,momentary_vol_norm)  #calculate the difference between the normalized and non-normalized notional volume
    df['Last'] = pd.to_numeric(df['Last'], errors='coerce').fillna(0) #convert the last column to float


    

    df['percent_change'] = ((1+(df['percent_change']/100)))*nvols_vols_and_bins['SPX_close'] #convert the %change to a decimal





    df['total_nvols'] = nvols_vols_and_bins['total_nvols'] #add the total notional volume to the dataframe
    nvols_norm = np.multiply(momentary_vol_norm,df['Last'].to_numpy()) #calculate the normalized notional volume
    nvols_non_norm = np.multiply(norm_non_norm_diff,df['Last'].to_numpy()) #calculate the non-normalized notional volume
    nvols_norm[nvols_norm < 0] = 0 #set the notional volume to 0 if the volume is negative    
    nvols_non_norm[nvols_non_norm < 0] = 0 #set the notional volume to 0 if the volume is negative                
    df['norm_nvols'] = nvols_norm #add the notiona volume to the dataframe
    Nvolchng = round(df['norm_nvols'].sum()/1000000)   ##################################################### store varible of total Nvol sum change latest ############
    df['norm_diff'] = norm_non_norm_diff
    df['total_nvols'] = np.add(nvols_norm,df['total_nvols']) #add the notional volume to the total notional volume
    spxLAST = orig_df['Last'].iloc[0]
    #print(spxLAST)



    min1mean = (df['percent_change1'] * df['norm_nvols']).sum() / df['norm_nvols'].sum()




    df_sorted = df.sort_values(by='percent_change1')
    # Calculate the cumulative sum of the weights
    df_sorted['cum_weight'] = df_sorted['norm_nvols'].cumsum()
    # Find the total sum of weights
    total_weight = df_sorted['norm_nvols'].sum()
    # Find the weighted median: locate the value where the cumulative weight is >= half of the total weight
    min1median = df_sorted.loc[df_sorted['cum_weight'] >= total_weight / 2, 'percent_change1'].iloc[0]










    NvollatestSUM = round(df['total_nvols'].sum()/1000000000,1)
    #print(df.columns)
    ### __________________________________________________________________________   PRICE DATA END   _____________________
    # PREPARE THE cap symbol and p% dataset
    #Pdata = orig_df['symbol','percent_change','cap']
    #Pdata['percent_change'] = Pdata['percent_change'].astype(float)
    #df['cap'] = orig_df['cap'].astype(float)
    ### __________________________________________________________________________   PRICE DATA END PREP  _____________________

    #            #                  #                  #              #                    SEND DATA to G shets here                  #           #

    # # Update the Google Sheets worksheet with new data
    # #starting_cell = 'N2'
    # #worksheet = sht1.get_worksheet(0)
    # #worksheet.update(starting_cell,[df_binned.columns.values.tolist()] + df_binned.values.tolist())
    # # SEND THIS TO GSHETS 

    today = datetime.today()
    numpy_array = (np.fromstring(nvols_vols_and_bins['mid_res_nvols']))
    #print(nvols_vols_and_bins['mid_res_nvols'])

    numpy_array1 = pd.DataFrame(numpy_array)
    numpy_array1['date'] = today.strftime('%Y-%m-%d')
    #numpy_array2 = numpy_array1.iloc[:, 0]
    #df33 = pd.DataFrame([Nvolchng, NvollatestSUM, spxLAST,None, numpy_array2], columns=['value'])
    #print(df33)
    nvols_vols_and_bins['total_nvols'] = df['total_nvols'].to_numpy() #update the total notional volume
    nvols_vols_and_bins['prev_volumes'] = df['Volume'].to_numpy() #update the previous volumes



    #keys = nvols_vols_and_bins.keys()

    # Convert to list (optional, depending on your needs)
    #keys_list = list(keys)
    #print(keys_list)    

    #                                                                                                                                       CSV  EXPORT PLACE


    # current_time = datetime.now().strftime('%H-%M-%S') #get the current time
    # csv_path = os.path.join(raw_data_directory, f"num_batch-{num_batch}_time-{current_time}.csv") # create a path to the csv file 
    # csv_path1 = "NvolchangeLAST-"+current_time+".csv" # create a path to the csv file 
    # #os.makedirs(csv_path1, exist_ok=True)
    # orig_df.to_csv(csv_path, index=False) #write the dataframe to a csv file
    # folder_path = r'C:\Users\DANNY\Desktop\csvTOexport'
    # # Make sure the folder exists, if not, create it
    # os.makedirs(folder_path, exist_ok=True)
    # # Full path for the CSV file
    # full_path = os.path.join(folder_path, csv_path1)
    # # Write the DataFrame to the CSV file
    # df3.to_csv(full_path, index=False, header=False)


    #print(nvols_vols_and_bins['mid_res_nvols'].astype(float))
    return nvols_vols_and_bins, df,NvollatestSUM,Nvolchng,spxLAST,capsDATA,min1mean,min1median,min1mediaCAPS,min1meanCAPS






#######################this function bins the data and aggregates it##########################
def bin_and_aggregate(df, nvols_vols_and_bins):
    reses = ['low', 'mid' ,'high', 'ultra']

    for res in reses:
        # Calculate bin indices for percent_change column
        bin_indices = np.digitize(df['percent_change'], nvols_vols_and_bins[f'{res}_res_bins'])
        # Create a new DataFrame for aggregation
        aggregation_df = pd.DataFrame({'percent_change': df['percent_change'], 'nvols': df['norm_nvols'], 'bin_indices': bin_indices})  #adjust the nvol keys and IMPORT THE CORRECT SCRIIPT
        # Group by bin_indices and sum nvols values
        aggregated_data = aggregation_df.groupby('bin_indices')['nvols'].sum()
        # Ensure the length of aggregated data matches the resolution's buckets
        expected_length = len(nvols_vols_and_bins[f'{res}_res_bins'])  # Get the expected length from initialized bins
        #if len(aggregated_data) < expected_length:
        if 1==1:#len(aggregated_data) < expected_length:
            # Fill missing bins with zeros
            aggregated_data = aggregated_data.reindex(range(1, expected_length + 1), fill_value=0)
        # Update the nvols_vols_and_bins dictionary with aggregated values
        nvols_vols_and_bins[f'{res}_res_nvols'] += aggregated_data.to_numpy()
        # make the first and last bins 0
        nvols_vols_and_bins[f'{res}_res_nvols'][0] = 0
        nvols_vols_and_bins[f'{res}_res_nvols'][-1] = 0




        # bin the difference between the normalized and non-normalized notional volume
        # bin indices are the same as the percent changes are the same
        # Create a new DataFrame for aggregation
        aggregation_df = pd.DataFrame({'percent_change': df['percent_change'], 'nvols': df['norm_diff'], 'bin_indices': bin_indices})
        # Group by bin_indices and sum nvols values
        aggregated_data = aggregation_df.groupby('bin_indices')['nvols'].sum()


        # Ensure the length of aggregated data matches the resolution's buckets
        expected_length = len(nvols_vols_and_bins[f'{res}_res_bins'])
        if len(aggregated_data) < expected_length:
            # Fill missing bins with zeros
            aggregated_data = aggregated_data.reindex(range(1, expected_length + 1), fill_value=0)
        # Update the nvols_vols_and_bins dictionary with aggregated values
        nvols_vols_and_bins[f'{res}_res_nvols_diff'] += aggregated_data.to_numpy()
    return nvols_vols_and_bins





#######################this function bins the data and aggregates it##########################
# def bin_caps(df, nvols_vols_and_bins):
#     peses = ['Pdata']

#     for pes in peses:
#         # Calculate bin indices for percent_change column
#         bin_indices = np.digitize(df['percent_change'], nvols_vols_and_bins[f'{pes}_res_bins'])
#         # Create a new DataFrame for aggregation
#         aggregation_df = pd.DataFrame({'percent_change': df['percent_change'], 'caps': df['caps'], 'bin_indices': bin_indices})  #adjust the nvol keys and IMPORT THE CORRECT SCRIIPT
#         # Group by bin_indices and sum nvols values
#         aggregated_data = aggregation_df.groupby('bin_indices')['caps'].sum()
#         # Ensure the length of aggregated data matches the resolution's buckets
#         expected_length = len(nvols_vols_and_bins[f'{pes}_res_bins'])  # Get the expected length from initialized bins
#         if len(aggregated_data) < expected_length:
#             # Fill missing bins with zeros
#             aggregated_data = aggregated_data.reindex(range(1, expected_length + 1), fill_value=0)
#         # Update the nvols_vols_and_bins dictionary with aggregated values
#         nvols_vols_and_bins[f'{pes}_res_nvols'] += aggregated_data.to_numpy()
#         # make the first and last bins 0
#         nvols_vols_and_bins[f'{pes}_res_nvols'][0] = 0
#         nvols_vols_and_bins[f'{pes}_res_nvols'][-1] = 0


#     return nvols_vols_and_bins
#######################this function is the main loop##########################


def extract_datetime_from_filename(filename):
    # Extract the datetime part of the filename
    datetime_str = filename.split('_')[-2] + "_" + filename.split('_')[-1].split('.')[0]
    return datetime.strptime(datetime_str, "%Y-%m-%d_%H-%M-%S")



def main_loop():
    open_time = datetime.strptime("00:00:00", "%H:%M:%S").time()
    close_time = datetime.strptime("23:56:00", "%H:%M:%S").time()


    # Initial authentication    
    client = authenticate()
    print("--------------gsheets authenticated")


    #set up a time zone where the data is null around 18:00
    time_before_null = datetime.strptime("17:59:50", "%H:%M:%S").time()
    time_after_null = datetime.strptime("18:00:25", "%H:%M:%S").time()   
    time_sum = 0 #variable to keep track of the time it takes to gather the data on avg
    global num_ticks
    num_ticks = 419 #number of ticks to gather
    global raw_data_directory
    global nvols_vols_and_bins_directory
    global start_vol
    global latest_90_files
    latest_90_files = []
    nvols_vols_and_bins, raw_data_directory, nvols_vols_and_bins_directory= init()
    while datetime.now().time() < open_time: #wait for the market to open
        print("Waiting for market to open")
        time.sleep(2.5)
        validate_session() #validate the session so it doesnt time out
    print("Market is open")
    num_batch=0
    print("getting initial market data")
    run_with_timeout(market_snapshot,15, 0, num_ticks)
    data = run_with_timeout(market_snapshot,15, 0, num_ticks)
    if data == None:
        print("data is none")
        data = run_with_timeout(market_snapshot, 0, num_ticks)





















    # MUST CONNECT MARKETCAPS SIZES FROM companymarketcaps.com daily (or most recent) scrape


    #           HERE i will get data for

    # MARKETCAP POSITOIONS
    # SPX(-) calculatoin realtime
    # GROUPS 1-4 calc

    #     GP1 Mp% vs MDp%    VS       nonGP1  Mp% vs MDp%




    data = pd.DataFrame(data)


    #print(list(data.columns))












    #check if the previous volumes are all 0, if they are, set the start volume to the current volume, otherwise, keep the previous volumes from the pickle file
    if np.array_equal(nvols_vols_and_bins['prev_volumes'], np.zeros(num_ticks-1)):
        start_vol =  pd.to_numeric(data['7762'][1:]).to_numpy()
        nvols_vols_and_bins['prev_volumes'] = start_vol
    print("pre market data gathered")
    print("starting main loop")
    print("initiaalizing the api session...")
    #setting up the multipliers vector
    global multipliers_vector
    multipliers_vector = np.ones(num_ticks-1)
    #vol_multipliers = {'TSLA':0.2,'NVDA':0.4,'AMD':0.6,'AMZN':0.85}           
    multipliers_vector[4] = 0.85 #AMZN
    multipliers_vector[5] = 0.4 #NVDA
    multipliers_vector[7] = 0.2 #TSLA
    multipliers_vector[31] = 0.6    #AMD
    #main loop that runs while the market is open
    while datetime.now().time() < close_time:
        #check if the time is between 18:00 and 18:01, if it is, wait for 30 seconds, resubscribe and then continue
        if time_before_null < datetime.now().time() < time_after_null:
            print("market is null")
            time.sleep(30)
            print(run_with_timeout(market_snapshot,15, 0, num_ticks))
            print(run_with_timeout(market_snapshot,15, 0, num_ticks))
            continue

        start_time = time.time()   #get the start time of this revolution of the loop
        num_batch+=1    #increment the batch number

        if num_batch%10 == 0:  #reauthenticate every 10 batches
            validate_session()
        if num_batch%50 == 0: #validate the session every 50 batches
            client = authenticate()
            print("--------------gsheets authenticated")
            access_account_endpoint()
        if num_batch%100 == 0: #access the account endpoint every 100 batches
            reauth()
        #print("batch number:",num_batch,flush=True)
        #print("gathering data",flush=True)
        data = run_with_timeout(market_snapshot,15, 0, num_ticks)   #gather the data



        if data == None:    #if the data is none, try again
            print("data is none",flush=True)
            data = run_with_timeout(market_snapshot,15, 0, num_ticks)
        #print("data gathered",flush=True)
        #print("pre-processing data",flush=True)

        #              FETCH DATA TO SEND

        #  mid_res_nvols   DESTRIBUTION
        #  NvollatestSUM
        #  Nvolchng
        #  spxLAST
        nvols_vols_and_bins, df,NvollatestSUM,Nvolchng,spxLAST,capsDATA,min1mean,min1median,min1mediaCAPS,min1meanCAPS = data_prep(data,nvols_vols_and_bins,num_batch,)  #pre-process the data
        

        GP1Mp = (capsDATA['percent_change'].iloc[:9] * capsDATA['cap'].iloc[:9]).sum() / capsDATA['cap'].iloc[:9].sum()
        GP234Mp = (capsDATA['percent_change'].iloc[9:] * capsDATA['cap'].iloc[9:]).sum() / capsDATA['cap'].iloc[9:].sum()


        GP2Mp = (capsDATA['percent_change'].iloc[9:33] * capsDATA['cap'].iloc[9:33]).sum() / capsDATA['cap'].iloc[9:].sum()
        GP3Mp = (capsDATA['percent_change'].iloc[33:123] * capsDATA['cap'].iloc[33:123]).sum() / capsDATA['cap'].iloc[33:123].sum()
        GP4Mp = (capsDATA['percent_change'].iloc[123:] * capsDATA['cap'].iloc[123:]).sum() / capsDATA['cap'].iloc[123:].sum()

        # # Calculate the bin edges based on 0.0416666% increments of the reference column
        # # Convert the percentage increment to a multiplier (0.0416666% of each value in reference_column)
        # bin_edges = pData['percent_change'] * (1 + 0.00125)  # 0.0416666% increment
        # bin_edges = sorted(bin_edges)
        # # Create the bi ns using the `pd.cut()` function to categorize based on the bin edges
        # pData['binned'] = pd.cut(df['cap'], bins=bin_edges, include_lowest=True, right=False)

        # # Show the resulting DataFrame
        #print(pData)







        percent_changeSPX = pd.DataFrame(nvols_vols_and_bins['SPX']['percent_change'])
        mid_res_nvolsDEST = pd.DataFrame(nvols_vols_and_bins['mid_res_nvols'].astype(float))
        #print(mid_res_nvolsDEST)

        #                                                               SEND TO GOOGLE SHEETS    START
        #                                                                SEND TO GOOGLE SHEETS    START
        #                                                                SEND TO GOOGLE SHEETS    START
        #  mid_res_nvols   DESTRIBUTION
        #  NvollatestSUM
        #  Nvolchng
        #  spxLAST
        # add empty rows then ADD the 3 critical variables into df
        empty_rows = pd.DataFrame([[0] * len(mid_res_nvolsDEST.columns)] * 5, columns=mid_res_nvolsDEST.columns)
        df3 = pd.concat([mid_res_nvolsDEST, empty_rows], ignore_index=True)
        df3.at[244, 0] = NvollatestSUM
        df3.at[243, 0] = Nvolchng
        df3.at[242, 0] = spxLAST       
        #df3[0] = df3[0][::-1].reset_index(drop=True)
        #print(df3)
        #  mid_res_nvols   DESTRIBUTION
        #  NvollatestSUM
        #  Nvolchng
        #  spxLAST

        # ##########                                             10,30,90MIN YELLOWs                ##################
        # ##########                                             10,30,90MIN YELLOWs                ################## 
            
        # part that refrences the data storage

        today3 = (datetime.now() - timedelta(days=                                    0                                   )).strftime("%Y-%m-%d")


        folder_path3 = 'nvols_vols_and_bins_'+today3
        # Get a list of all files in the folder
        files = [f for f in os.listdir(folder_path3) if f.endswith('.pkl')]
        # flip the file order by sorting by datetime of creation
        files.sort(key=extract_datetime_from_filename, reverse=True)
        latest_90_files = files[:90]

        if len(latest_90_files) > 0:
            # get 90 latest files in whole folder
            file1 = latest_90_files[0]
            if len(latest_90_files) > 9:
                file10 = latest_90_files[9]
            else:
                file10 = latest_90_files[0]
            #
            if len(latest_90_files) > 29:
                file30 = latest_90_files[29]
            else:
                file30 = latest_90_files[0]
            #
            if len(latest_90_files) > 89:
                file90 = latest_90_files[89]
            else:
                file90 = latest_90_files[0]
            # Output the latest 50 files
            filepath7 = 'nvols_vols_and_bins_'+today3+'/' + file1
            df1 = pd.read_pickle(filepath7)
            # 
            filepath6 = 'nvols_vols_and_bins_'+today3+'/' + file10
            df10 = pd.read_pickle(filepath6)
            #
            filepath5 = 'nvols_vols_and_bins_'+today3+'/' + file30
            df30 = pd.read_pickle(filepath5)
            #
            filepath55 = 'nvols_vols_and_bins_'+today3+'/' + file90
            df90 = pd.read_pickle(filepath55)
            #
            #
            #
            #
            dfdiff10  =  df10['mid_res_nvols'] - df1['mid_res_nvols']
            # files now listed by datetime of creaiton
            diff10MIN = pd.DataFrame(dfdiff10)
            #diff10MIN = diff10MIN[::-1].reset_index(drop=True)
            #
            dfdiff30  =  df30['mid_res_nvols'] - df1['mid_res_nvols']
            # files now listed by datetime of creaiton
            diff30MIN = pd.DataFrame(dfdiff30)
            #diff30MIN = diff30MIN[::-1].reset_index(drop=True)
            #
            dfdiff90 =  df90['mid_res_nvols'] - df1['mid_res_nvols']
            # files now listed by datetime of creaiton
            diff90MIN = pd.DataFrame(dfdiff90)
            #diff90MIN = diff90MIN[::-1].reset_index(drop=True)
                








            # _________________________________ADD Nvol sum and tick data______________________________________________________________

            # _________________________________ADD Nvol sum and tick data______________________________________________________________

            # _________________________________ADD Nvol sum and tick data______________________________________________________________
            # Ensure dictionary keys exist before appending
            if 'Nvoltotallast' not in nvols_vols_and_bins:
                nvols_vols_and_bins['Nvoltotallast'] = np.array([])  # Start with an empty array

            if 'Nvol_tick_sum' not in nvols_vols_and_bins:
                nvols_vols_and_bins['Nvol_tick_sum'] = np.array([])

            if 'GP1Mprice' not in nvols_vols_and_bins:
                nvols_vols_and_bins['GP1Mprice'] = np.array([]) 

            if 'GP234Mprice' not in nvols_vols_and_bins:
                nvols_vols_and_bins['GP234Mprice'] = np.array([])      


            ##################################################################   groups 2, 3, 4
            if 'GP2Mprice' not in nvols_vols_and_bins:
                nvols_vols_and_bins['GP2Mprice'] = np.array([])

            if 'GP3Mprice' not in nvols_vols_and_bins:
                nvols_vols_and_bins['GP3Mprice'] = np.array([]) 

            if 'GP4Mprice' not in nvols_vols_and_bins:
                nvols_vols_and_bins['GP4Mprice'] = np.array([])      
            #########################################################################
            if 'min1mean' not in nvols_vols_and_bins:
                nvols_vols_and_bins['min1mean'] = np.array([]) 

            if 'min1median' not in nvols_vols_and_bins:
                nvols_vols_and_bins['min1median'] = np.array([]) 

            if 'min1mediaCAPS' not in nvols_vols_and_bins:
                nvols_vols_and_bins['min1mediaCAPS'] = np.array([]) 

            if 'min1meanCAPS' not in nvols_vols_and_bins:
                nvols_vols_and_bins['min1meanCAPS'] = np.array([])           
            

            # add existing data into active live pkl files
            nvols_vols_and_bins['Nvoltotallast'] = np.append([nvols_vols_and_bins['Nvoltotallast']], NvollatestSUM)
            nvols_vols_and_bins['Nvol_tick_sum'] = np.append([nvols_vols_and_bins['Nvol_tick_sum']], Nvolchng)

            nvols_vols_and_bins['min1mean'] = np.append([nvols_vols_and_bins['min1mean']], min1mean)
            nvols_vols_and_bins['min1median'] = np.append([nvols_vols_and_bins['min1median']], min1median)
            nvols_vols_and_bins['min1mediaCAPS'] = np.append([nvols_vols_and_bins['min1mediaCAPS']], min1mediaCAPS)
            nvols_vols_and_bins['min1meanCAPS'] = np.append([nvols_vols_and_bins['min1meanCAPS']], min1meanCAPS)

            nvols_vols_and_bins['GP1Mprice'] = np.append([nvols_vols_and_bins['GP1Mprice']], GP1Mp/100)
            nvols_vols_and_bins['GP234Mprice'] = np.append([nvols_vols_and_bins['GP234Mprice']], GP234Mp/100)



            nvols_vols_and_bins['GP2Mprice'] = np.append([nvols_vols_and_bins['GP2Mprice']], GP2Mp/100)
            nvols_vols_and_bins['GP3Mprice'] = np.append([nvols_vols_and_bins['GP3Mprice']], GP3Mp/100)
            nvols_vols_and_bins['GP4Mprice'] = np.append([nvols_vols_and_bins['GP4Mprice']], GP4Mp/100)





            tempDF  =  nvols_vols_and_bins['Nvol_tick_sum']
            Nvolticks = pd.DataFrame(tempDF)
            tempDF1  =  nvols_vols_and_bins['GP1Mprice']
            GP1priceticks = pd.DataFrame(tempDF1)
            tempDF2  =  nvols_vols_and_bins['GP234Mprice']
            GP234priceticks = pd.DataFrame(tempDF2)


            tempDF22  =  nvols_vols_and_bins['GP2Mprice']
            GP2priceticks = pd.DataFrame(tempDF22)
            tempDF23  =  nvols_vols_and_bins['GP3Mprice']
            GP3priceticks = pd.DataFrame(tempDF23)
            tempDF233  =  nvols_vols_and_bins['GP4Mprice']
            GP4priceticks = pd.DataFrame(tempDF233)

            tempDF2334  =  nvols_vols_and_bins['min1mean']
            min1mean = pd.DataFrame(tempDF2334)

            tempDF23354  =  nvols_vols_and_bins['min1median']
            min1median = pd.DataFrame(tempDF23354)

            tempDF233542  =  nvols_vols_and_bins['min1mediaCAPS']
            min1mediaCAPS = pd.DataFrame(tempDF233542)

            tempDF2233542  =  nvols_vols_and_bins['min1meanCAPS']
            min1meanCAPS = pd.DataFrame(tempDF2233542)


            # SEND df to sheets
            # SEND df to sheets
            # SEND df to sheets

            sht1 = client.open_by_key('1rwtnGjSjvr-t6kSFgaL6x--C3QJKN42sy92ya325_8A')
            worksheet = sht1.worksheet("DEST.liveDATA")

            ##############################    SEND TO   SPXtrender1     ########################################

            #   10, 30,  90 MIN YELLOW

            
            diff10MINposition = 'U2'
            worksheet.update(diff10MINposition,[diff10MIN.columns.values.tolist()] + diff10MIN.values.tolist())
            print("s ---------------- sent 10MIN yellow --------")
            diff30MINposition = 'v2'
            worksheet.update(diff30MINposition,[diff30MIN.columns.values.tolist()] + diff30MIN.values.tolist())
            print("s ---------------- sent 30MIN yellow --------")
            diff90MINposition = 'W2'
            worksheet.update(diff90MINposition,[diff90MIN.columns.values.tolist()] + diff90MIN.values.tolist())
            print("s ---------------- sent 90MIN yellow --------")


            #                                                                                                                          VARREF
            df3 = replace_nan_in_dataframe(df3)
            Nvolticks = replace_nan_in_dataframe(Nvolticks)
            GP1priceticks = replace_nan_in_dataframe(GP1priceticks)
            GP234priceticks = replace_nan_in_dataframe(GP234priceticks)
            percent_changeSPX = replace_nan_in_dataframe(percent_changeSPX)
            GP2priceticks = replace_nan_in_dataframe(GP2priceticks)
            GP3priceticks = replace_nan_in_dataframe(GP3priceticks)
            GP4priceticks = replace_nan_in_dataframe(GP4priceticks)

            min1mean = replace_nan_in_dataframe(min1mean)
            min1median = replace_nan_in_dataframe(min1median)
            min1mediaCAPS = replace_nan_in_dataframe(min1mediaCAPS)
            min1meanCAPS = replace_nan_in_dataframe(min1meanCAPS)
            #  mid_res_nvols   DESTRIBUTION

            midRESnvolsCOL = 'T2'
            df3 = df3.replace([np.nan, np.inf, -np.inf], '')
            worksheet.update(midRESnvolsCOL,[df3.columns.values.tolist()] + df3.values.tolist())
            # 1min series to spdheets
            nvol1min = 'M2'
            Nvolticks = Nvolticks.replace([np.nan, np.inf, -np.inf], '')
            worksheet.update(nvol1min,[Nvolticks.columns.values.tolist()] + Nvolticks.values.tolist())
            # 1min GP1p %  series to spdheets
            GP1price = 'P2'
            GP1priceticks = GP1priceticks.replace([np.nan, np.inf, -np.inf], '')
            worksheet.update(GP1price,[GP1priceticks.columns.values.tolist()] + GP1priceticks.values.tolist())
            # 1min  GP234 p %  series to spdheets
            GP234price = 'Q2'
            GP234priceticks = GP234priceticks.replace([np.nan, np.inf, -np.inf], '')
            worksheet.update(GP234price,[GP234priceticks.columns.values.tolist()] + GP234priceticks.values.tolist())
            #  SPX percent_change SERIES LIVE
            sendto = 'Z2'
            percent_changeSPX = percent_changeSPX.replace([np.nan, np.inf, -np.inf], '')
            worksheet.update(sendto,[percent_changeSPX.columns.values.tolist()] + percent_changeSPX.values.tolist())



            #  min1mean SERIES LIVE
            sendto = 'AK2'
            min1mean = min1mean.replace([np.nan, np.inf, -np.inf], '')
            worksheet.update(sendto,[min1mean.columns.values.tolist()] + min1mean.values.tolist())

            #  min1median SERIES LIVE
            sendto = 'AJ2'
            min1median = min1median.replace([np.nan, np.inf, -np.inf], '')
            worksheet.update(sendto,[min1median.columns.values.tolist()] + min1median.values.tolist())
            sendto = 'AP2'
            min1mediaCAPS = min1mediaCAPS.replace([np.nan, np.inf, -np.inf], '')
            worksheet.update(sendto,[min1mediaCAPS.columns.values.tolist()] + min1mediaCAPS.values.tolist())
            sendto = 'AQ2'
            min1meanCAPS = min1meanCAPS.replace([np.nan, np.inf, -np.inf], '')
            worksheet.update(sendto,[min1meanCAPS.columns.values.tolist()] + min1meanCAPS.values.tolist())


            GP2price = 'AG2'
            worksheet.update(GP2price,[GP2priceticks.columns.values.tolist()] + GP2priceticks.values.tolist())
            GP3price = 'AH2'
            worksheet.update(GP3price,[GP3priceticks.columns.values.tolist()] + GP3priceticks.values.tolist())
            GP4price = 'AI2'
            worksheet.update(GP4price,[GP4priceticks.columns.values.tolist()] + GP4priceticks.values.tolist())

        #___________________________________________END OF EXPORINTG_____________________________________________________________










        #___________________________________________UPDATE latest total Nvol for daily storage___________________________________________________________

        csv_df = pd.read_csv('NvolTOTdaily.csv')
        # Create a new DataFrame with the values you want to replace the last row with
        # Make sure the new row has the same structure (same number of columns) as the original CSV
        today = datetime.today()
        today55 = today.strftime('%d-%m-%Y')
        new_values = {
            'NvolTOT': NvollatestSUM,  # replace with actual column name and new value
            'date': today55,  # Example column for date
        }
        new_row = pd.DataFrame([new_values])
        # Replace the last row in the CSV DataFrame with the new row
        csv_df.iloc[-1] = new_row.iloc[0]
        # Save the updated DataFrame back to the CSV
        csv_df.to_csv('NvolTOTdaily.csv', index=False)
        print("updated daily sum")





        # ____________________________________________ STORE DATA _________________________________________________________
        #print("binning data",flush=True)
        nvols_vols_and_bins = bin_and_aggregate(df,nvols_vols_and_bins) #bin the data and aggregate it


        #nvols_vols_and_bins = bin_caps(df,nvols_vols_and_bins)


        save_nvols_vols_and_bins(nvols_vols_and_bins, nvols_vols_and_bins_directory)    #save the data to a pickle file
        ###################################################################################################################

        if num_batch%5 == 0:    
            print("distro Queue size before put:", shared_queue.qsize(),flush=True)
            shared_queue.put(nvols_vols_and_bins)   #put the data in the queue to be sent to the dashboard
            print("distro Queue size before read:", shared_queue.qsize(),flush=True)
        ###################################################################################             print("Active threads:", threading.active_count(),flush=True)
        end_time = time.time()  #get the end time of this revolution of the loop
        time_sum += end_time - start_time   #add the time it took to gather the data to the time_sum variable
        print("NvolTOTAL: ", NvollatestSUM,"B",flush=True,)  #print total latest Nvol
        print("NvolLASTtick: ",Nvolchng,"M",flush=True)  #print the latest Nvol change per tick
        print("SPX: ",spxLAST,flush=True)

        print("test:::min1mean: ",min1mean.iloc[-1],flush=True)
        print("test:::min1median: ",min1median.iloc[-1],flush=True)
        print("test:::min1mediaCAPS: ",min1mediaCAPS.iloc[-1],flush=True)       
        print("test:::min1meanCAPS: ",min1meanCAPS.iloc[-1],flush=True)   
        print("tick-time:",end_time - start_time,flush=True)   #print the time it took to gather the data
        ticktime = end_time - start_time
        #print("Average time: ", time_sum/num_batch,flush=True)  #print the average time it takes to gather the data

        #if the time is after 22:59:20, save the nvols_vols_and_bins to a variable called end_day_distro
        if datetime.now().time() > datetime.strptime("22:59:45", "%H:%M:%S").time():
            end_day_distro = nvols_vols_and_bins
            print("$$$end day distro saved, market is closed, breaking main loop$$$",flush=True)
            break

        # KEEP LOOP AT 60s on point
        time.sleep(60-ticktime)
    print("its over for today, piss off")
    #continue pushing end_day_distro to the queue till 23:10
    #while datetime.now().time() < datetime.strptime("23:50:00", "%H:%M:%S").time():
        #print("pushing end day distro to queue")
        #shared_queue.put(end_day_distro)
        #time.sleep(5)
