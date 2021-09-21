import requests
import numpy as np
import sys
from datetime import datetime, timedelta
import time
import pandas as pd
import argparse
import os

DEFAUT_DATE_DELTA = 30
# API_Key = "QEZVFI816I1TYVZTQVY7MWE4BMDBDW3KHE", "UFMJNZ1NHXDZ8ZXJPJBPVUKGAZGV4R9EZ4"
PANCAKESWAP_ADDRESSES = ["0x10ED43C718714eb63d5aA57B78B54704E256024E", "0xF09B7EB9DfB93CC02a2fd6D4c683715c8BDe5b30", "0x758f64ef25781Ee699C69e7DF0fc09BdCf5C6D04"]
TRANSACTION_BASEURL = "https://api.bscscan.com/api?module=account&action=txlist&address={0}&startblock=0&endblock=999999999&sort=asc&apikey={1}" 
BEP20_TOEKEN_BASEURL = "https://api.bscscan.com/api?module=account&action=tokentx&address={0}&startblock=0&endblock=999999999&sort=asc&apikey={1}"
BNB_PRICE_BASEURL = "https://api.bscscan.com/api?module=stats&action=bnbprice&apikey={0}"
INTERNAL_TRANSACTION_BASEURL = "https://api.bscscan.com/api?module=account&action=txlistinternal&txhash={0}&apikey={1}"

EXPORT_CSV_BASE_FOLDER = os.path.join(os.getcwd() + "\Output")

if not os.path.exists(EXPORT_CSV_BASE_FOLDER):
    os.makedirs(EXPORT_CSV_BASE_FOLDER)

parser = argparse.ArgumentParser(description='Description of your program')
parser.add_argument('-t','--timeframe', help='timeframe', default=DEFAUT_DATE_DELTA)
parser.add_argument('-api', '--apikey', help = 'Api Key', default = "FQ7Z3A4VMACSW9K3P2UZ2Y9H4HTQXHM7FA")
# parser.add_argument('-w','--walletlist', help='wallet list', required=True)
args = parser.parse_args()
print(args.timeframe)
print(args.apikey)


# -------------------------------------------------------------------------
second_df = pd.DataFrame(columns = ['Wallet Address', \
    'Purchased BNB Value', 'Sold BNB Value', \
    'Purchased USD Value', 'Sold USD Value'
    ])
third_columns=["Token Name", "Number of Buyer Wallets", "Purchased BNB Value", "Purchased USD Value", \
    "Number of Seller Wallets", "Sold BNB Value", "Sold USD Value", \
        "Difference Between Buyer Number and Seller Number", "Difference Between Purchased and Sold BNB", "Difference Between Purchased and Sold USD"]
default_dict = {
    "Token Name" : "Unknown", 
    "Number of Buyer Wallets" : 0, 
    "Purchased BNB Value" : 0, 
    "Purchased USD Value" : 0, 
    "Number of Seller Wallets" : 0, 
    "Sold BNB Value" : 0, 
    "Sold USD Value" : 0

}
third_df = pd.DataFrame(columns=third_columns)
# -------------------------------------------------------------------------

# get transaction list by wallet address
def get_txnlist_by_address(address):
    url = TRANSACTION_BASEURL.format(address, args.apikey)
    response = requests.get(url)
    response = response.json()
    txnlist = None
    if response["status"] == "1" and response["message"]=="OK":
        txnlist = response["result"]
    return txnlist

# check whether the Hex address exists in addresses list
def isExist(address, addresses):
    lower_addresses = [adr.lower() for adr in addresses]
    if address.lower() in lower_addresses:
        return True
    else:
        return False

# filter transaction list by address(default : pancakeswap)
def filter_txnlist_by_address(txnlist):
    for transaction in txnlist:
        if not isExist(transaction['from'], PANCAKESWAP_ADDRESSES) and not isExist(transaction['to'], PANCAKESWAP_ADDRESSES):
            txnlist.remove(transaction)
    return txnlist

# filter transaction list by timestamp(timerange)
def filter_txnlist_by_timeframe(txnlist, timeframe):
    sttime = int((datetime.today() - timedelta(days=timeframe)).timestamp())

    return [transaction for transaction in txnlist if int(transaction['timeStamp']) >= sttime]

# calculate the percentage of transactions
def calc_percentage(sold_list, purchased_list):
    return (np.array(sold_list) * 100 / np.array(purchased_list)).tolist()
    # return [ai / bi for ai, bi in zip(purchased_list, sold_list)]


# get the BEP-20 Tokens transaction by address
def get_tokenlist_by_address(address):
    url = BEP20_TOEKEN_BASEURL.format(address, args.apikey)
    response = requests.get(url)
    response = response.json()
    token_txnlist = None
    if response["status"] == '1' and response["message"] == "OK":
        token_txnlist = response["result"]

    return token_txnlist


# convert the timestamp to date
def get_date_from_TimeStamp(timestamp):
    time = int(timestamp)
    date = datetime.utcfromtimestamp(time).strftime('%Y-%m-%d')
    return date

# get the BNB balance in defined day
def get_historicalBNBprice(date_delta = DEFAUT_DATE_DELTA):
    url = BNB_PRICE_BASEURL.format(args.apikey)
    response = requests.get(url).json()
    if response["status"] == "1" and response["message"]=="OK":
        return response["result"]["ethusd"]
    return None

# add the In/Out filed in dataframe
def add_In_Out_field(dataframe, address):
    newField_list = []
    for index in range(len(dataframe['from'])):
        if dataframe['from'][index].lower() == address.lower():
            newField_list.append('Out')
        elif dataframe['to'][index].lower() == address.lower():
            newField_list.append("In")
    dataframe["In/Out"] = newField_list

    return dataframe

# calculate the purchased and sold token amount in transaction
def get_token_amount_by_token(token_dataframe):

    # purchased_amount = 0
    # sold_amount = 0
    purchased_amount_list = []
    sold_amount_list = []

    for index, row in token_dataframe.iterrows():
        token_amount = row['value']
        token_decimal = row['tokenDecimal']
        if row["In/Out"] == "In":
            try:
                purchased_amount_list.append(int(token_amount) / (10**int(token_decimal)))
            except:
                print("Exception in purchased token amount")
        else:
            try:
                sold_amount_list.append(int(token_amount) / (10**int(token_decimal)))
            except:
                print("Exception in sold token amount")    

    return sum(purchased_amount_list), sum(sold_amount_list)

# calculate the purchased and sold BNB value in normal transaction by txnhash
def get_BNB_amount_by_txnhash(txnhash, In_Out):

    # https://bscscan.com/tx/0xa414d2333d766adde07638b5576f2ce0ee1555086f6da8aa1b916afaf634b66c
    url = INTERNAL_TRANSACTION_BASEURL.format(txnhash, args.apikey)
    response = requests.get(url)
    response = response.json()
    internal_txnlist = []

    purchased_bnb_value = 0
    sold_bnb_value = 0
    timestamp = 0
    if response["status"] == '1' and response["message"] == "OK":
        internal_txnlist = response["result"]
        last_result = internal_txnlist[-1]
        timestamp = last_result['timeStamp']
        if In_Out == "Out":
            purchased_bnb_value = float(last_result['value'])/(10**18)
        else:
            sold_bnb_value = float(last_result['value'])/(10**18)
     
    return purchased_bnb_value, sold_bnb_value, timestamp


# calculate the token amount, BNB value
def get_transaction_info(unique_token_df, BNB_price):

    purchased_token_amount = 0
    sold_token_amount = 0

    purchased_BNB_amount = 0
    sold_BNB_amount = 0

    purchased_USD_amount = 0
    sold_USD_amount = 0

    if unique_token_df.empty:
        return purchased_token_amount, sold_token_amount, purchased_BNB_amount, sold_BNB_amount, purchased_USD_amount, sold_USD_amount

    # calculate the purchased token amount and sold token amount

    purchased_token_amount, sold_token_amount = get_token_amount_by_token(unique_token_df)

    # remove the duplicate rows by token txn hash value
    hashnInOut_df = unique_token_df.drop_duplicates(subset=['hash', 'In/Out'], keep='first')

    req_count = 0
    first_time = time.time()
    for txnhash, In_Out in zip(hashnInOut_df['hash'], hashnInOut_df["In/Out"]):
        temp_purchased_BNB_value = 0
        temp_sold_BNB_value = 0
        # get the internal transaction by txn hash
        temp_purchased_BNB_value, temp_sold_BNB_value, last_timestamp = get_BNB_amount_by_txnhash(txnhash, In_Out)
        
        req_count += 1
        consumed_time = time.time() - first_time
        print(consumed_time)
        if req_count >=5 and consumed_time < 1:
            req_count = 0
            print("deley {0} second".format(consumed_time))
            time.sleep(1 - consumed_time)


        #get the BNB and USD
        if last_timestamp != 0:
            date = get_date_from_TimeStamp(last_timestamp)
            # date_prices = [item for item in historical_BNB_prices if item["UTCDate"] == date]
            
            purchased_BNB_amount += temp_purchased_BNB_value
            sold_BNB_amount += temp_sold_BNB_value
            # if date_prices:
            purchased_USD_amount += float(BNB_price) * temp_purchased_BNB_value
            sold_USD_amount += float(BNB_price) * temp_sold_BNB_value

    return purchased_token_amount, sold_token_amount, purchased_BNB_amount, sold_BNB_amount, purchased_USD_amount, sold_USD_amount


# get the wallet list from text file
def get_all_wallet_list_from_file(file_name):
    wallet_list = []
    with open(file_name) as f:
        lines = f.readlines()
        wallet_list = [line.strip() for line in lines]
    return wallet_list
    
# get the profit/loss
def calc_profit_and_loss(purchsed_list, sold_list):
    return (np.array(sold_list) - np.array(purchsed_list)).tolist()
    
# get USD value from token BNB value
def get_USD_list_from_BNBnDate(timeStamp_list, BNB_list, historical_BNB_prices):

    # timeStamp_list = [timestamp for timestamp in sorted_df['timeStamp']]

    date_list = get_date_from_TimeStamp(timeStamp_list)
    usd_list = []
    for index, date in enumerate(date_list):
        date_price = next(item for item in historical_BNB_prices if item["UTCDate"] == date)
        usd_list.append(float(date_price["value"]) * BNB_list[index])
    return usd_list


def process_by_address(address, BNB_price):

    global third_df

    print("CSV file of Wallet Address : {} is generating...".format(address))

    # 1. get BEP20 token list and normal transaction list within defined date range

    token_list = get_tokenlist_by_address(address)
    # txn_list = get_txnlist_by_address(address)
    if token_list is None:# or txn_list is None:
        return
    token_list = filter_txnlist_by_timeframe(token_list, int(args.timeframe))

    # txn_list = filter_txnlist_by_timeframe(txn_list, int(args.timeframe))
    if not token_list:# and not txn_list:
        return None
    token_df = pd.DataFrame(token_list)
    # txn_df = pd.DataFrame(txn_list)

    # sort them by token name
    sorted_df = token_df.sort_values("tokenName")


    # add new field for Out and In to sorted token dataframe
    sorted_df = add_In_Out_field(sorted_df, address)
    # add new field for Out and In to normal transaction dataframe
    # txn_df = add_In_Out_field(txn_df, address)

    # # get the unique token name list
    unique_token_list = np.unique([token for token in sorted_df['tokenName']]).tolist()

    t_purchased_token_amount_list= []
    t_sold_token_amount_list = []
    t_purchased_BNB_amount_list = []
    t_sold_BNB_amount_list = []
    t_purchased_USD_amount_list = []
    t_sold_USD_amount_list = []

    unique_token_df_In_Out_list = []
    new_token_list = []

    for unique_token in unique_token_list:

        # check if there are both of In and Out transactions in each token transaction
        unique_token_df = sorted_df[sorted_df['tokenName'] == unique_token]
        unique_token_df_In_Out_list = [In_Out_Item  for In_Out_Item in unique_token_df['In/Out']]

        purchased_token_amount, sold_token_amount, purchased_BNB_amount, sold_BNB_amount, purchased_USD_amount, sold_USD_amount \
            = get_transaction_info(unique_token_df, BNB_price)

        if not (third_df['Token Name'] == unique_token).any():
            default_dict['Token Name'] = unique_token
            third_df = third_df.append(default_dict, ignore_index=True)

        if (unique_token_df['In/Out'] == "In").any():
            third_df.loc[third_df['Token Name'] == unique_token, "Number of Buyer Wallets"] += 1
            third_df.loc[third_df['Token Name'] == unique_token, "Purchased BNB Value"] += purchased_BNB_amount
            third_df.loc[third_df['Token Name'] == unique_token, "Purchased USD Value"] += purchased_USD_amount
        if (unique_token_df['In/Out'] == "Out").any():
            third_df.loc[third_df['Token Name'] == unique_token, "Number of Seller Wallets"] += 1
            third_df.loc[third_df['Token Name'] == unique_token, "Sold BNB Value"] += sold_BNB_amount
            third_df.loc[third_df['Token Name'] == unique_token, "Sold USD Value"] += sold_USD_amount

        
        if "In" in unique_token_df_In_Out_list and "Out" in unique_token_df_In_Out_list:
                
            new_token_list.append(unique_token)
            t_purchased_token_amount_list.append(purchased_token_amount)
            t_sold_token_amount_list.append(sold_token_amount)
            t_purchased_BNB_amount_list.append(purchased_BNB_amount)
            t_sold_BNB_amount_list.append(sold_BNB_amount)
            t_purchased_USD_amount_list.append(purchased_USD_amount)
            t_sold_USD_amount_list.append(sold_USD_amount)

    # create a new dataframe for exporting the csv
    columns = ['tokenName', 'Wallet Address', 'Purchased Token Amount', 'Sold Token Amount', 'Percentage Difference Between Purchased and Sold Tokens', \
        'Purchased BNB Value', 'Sold BNB Value', "Percentage Difference Between Purchased and Sold BNB", 'Profit/Loss BNB',\
        'Purchased USD Value', 'Sold USD Value', "Percentage Difference Between Purchased and Sold USD", 'Profit/Loss USD'\
        ]

    new_token_df = pd.DataFrame(columns=columns)

    
    new_token_df["tokenName"] = new_token_list
    new_token_df['Wallet Address'] = [address] * len(new_token_list)
    new_token_df["Purchased Token Amount"]  = t_purchased_token_amount_list
    new_token_df["Sold Token Amount"]  = t_sold_token_amount_list
    new_token_df["Purchased BNB Value"]  = t_purchased_BNB_amount_list
    new_token_df["Sold BNB Value"]  = t_sold_BNB_amount_list
    new_token_df["Purchased USD Value"]  = t_purchased_USD_amount_list
    new_token_df["Sold USD Value"]  = t_sold_USD_amount_list

    # calculate the percentage between purchased and sold token
    token_amount_percentage_list = calc_percentage(t_sold_token_amount_list, t_purchased_token_amount_list)
    new_token_df["Percentage Difference Between Purchased and Sold Tokens"] = token_amount_percentage_list

    # calculate the percentage between purchased and sold BNB
    BNB_amount_percentage_list = calc_percentage(t_sold_BNB_amount_list, t_purchased_BNB_amount_list)
    new_token_df["Percentage Difference Between Purchased and Sold BNB"] = BNB_amount_percentage_list

    # calculate the percentage between purchased and sold USD
    USD_amount_percentage_list = calc_percentage(t_sold_USD_amount_list, t_purchased_USD_amount_list)
    new_token_df["Percentage Difference Between Purchased and Sold USD"] = USD_amount_percentage_list

    #calculatet the profit/loss of BNB and USD
    BNB_profit_and_loss_list = calc_profit_and_loss(t_purchased_BNB_amount_list, t_sold_BNB_amount_list)
    new_token_df["Profit/Loss BNB"] = BNB_profit_and_loss_list

    USD_profit_and_loss_list = calc_profit_and_loss(t_purchased_USD_amount_list, t_sold_USD_amount_list)
    new_token_df["Profit/Loss USD"] = USD_profit_and_loss_list

    # export the csv file
    output_file = os.path.join(EXPORT_CSV_BASE_FOLDER, address + ".csv")
    new_token_df.to_csv(output_file, index=False)
    print("{}.csv file was generated".format(address))

    # Get the sum of valid(both are not zero) BNB and USD values for each wallet address
    wallet_element = new_token_df[(new_token_df["Purchased BNB Value"] > 0) & (new_token_df["Sold BNB Value"] > 0)].sum()
    return {
        "Wallet Address" : address,
        "Purchased BNB Value" : wallet_element["Purchased BNB Value"], 
        "Sold BNB Value" : wallet_element["Sold BNB Value"],
        "Purchased USD Value" : wallet_element["Purchased USD Value"], 
        "Sold USD Value" : wallet_element["Sold USD Value"]
    }




def main():
    global second_df, third_df
    # get historical BNB price list
    BNB_price = get_historicalBNBprice()

    address_list = get_all_wallet_list_from_file("wallets_for_parse - Copy.txt")
    for address in address_list:
        wallet_elements = process_by_address(address, BNB_price)
        if wallet_elements is None:
            print("There isn't any transaction within the specified timeframe!")
        else:
            second_df = second_df.append(wallet_elements, ignore_index=True)
    second_df["Percentage Difference Between Purchased and Sold BNB"] = calc_percentage(second_df['Sold BNB Value'].tolist(), second_df['Purchased BNB Value'].tolist())
    second_df["Percentage Difference Between Purchased and Sold USD"] = calc_percentage(second_df['Sold USD Value'].tolist(), second_df['Purchased USD Value'].tolist())
    second_df["Profit/Loss BNB"] = calc_profit_and_loss(second_df['Purchased BNB Value'].tolist(), second_df['Sold BNB Value'].tolist())
    second_df["Profit/Loss USD"] = calc_profit_and_loss(second_df['Purchased USD Value'].tolist(), second_df['Sold USD Value'].tolist())
    second_df.to_csv(os.path.join(EXPORT_CSV_BASE_FOLDER, "all_" + datetime.now().strftime("%d-%m-%Y_%H_%M") + ".csv"), index=False)


    third_df['Difference Between Buyer Number and Seller Number'] = calc_profit_and_loss(third_df['Number of Buyer Wallets'].tolist(), third_df['Number of Seller Wallets'].tolist())
    third_df['Difference Between Purchased and Sold BNB'] = calc_profit_and_loss(third_df['Purchased BNB Value'].tolist(), third_df['Sold BNB Value'].tolist())
    third_df['Difference Between Purchased and Sold USD'] = calc_profit_and_loss(third_df['Purchased USD Value'].tolist(), third_df['Sold USD Value'].tolist())
    third_df.to_csv(os.path.join(EXPORT_CSV_BASE_FOLDER, datetime.now().strftime("%d-%m-%Y_%H_%M") + ".csv"), index=False)
    print("Success!!!")
    

if __name__ == "__main__":
    main()
