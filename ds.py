import pandas as pd
import numpy as np
import glob
import csv
import os
import time
# Assigning path to the path variable
path = "/Users/srikaramara/temp"
all_files = glob.glob(path)
# Initializing "li" array to store the csv files.
li = []
# Setting up count variable to Zero[0]
count =0

# Looping to take the first n number of files from the directory.

for file in os.listdir(path):
    f = path+"/"+file
    if count<1:
        print(f) # Printing the files names taken from the directory to check which files are read from the directory.
        df = pd.read_csv(f, index_col=None, header=0) # Reading the csv files and adding it to dataframe df.
        li.append(df) # concatenating the next dataframe to the previous dataframe using "concat".
    count+=1 # Increasing the count variable to match the number of files to read from the directory and to exit out of the loop.
tr_data = pd.concat(li, ignore_index=True, sort=True)
print(tr_data.count())

start_time_prog = time.time()
print("_______________________________________________________________________________________________________________________________________________________________________________________________")
#Importing time module to calculate the running time of the question.
start_time = time.time()

### First Question ###

final_df = tr_data.drop_duplicates(subset=['tx_hash']) # blk no is dropped duplicates dataframe
dff_hash = final_df.groupby(["block_number","block_time"])
dff = dff_hash.gas.sum().reset_index()
dff = dff.sort_values(by = "gas", kind='heapsort')
dff.rename(columns={'gas': 'block_gas'}, inplace=True) # dff = first q
# dff.to_csv('/Users/srikaramara/Desktop/first_q.csv')
print(dff)
print("--- First Question Ran in %s seconds ---" % (time.time() - start_time))


### Second Question ###

start_time = time.time()
tmp_1 = dff_hash.size().reset_index(name='transactions')
tmp_1 = tmp_1.sort_values(by = "transactions", kind='heapsort')
print(tmp_1)
print("--- Second Question Ran in %s seconds ---" % (time.time() - start_time))

### Third Question ###

start_time = time.time()
fff = final_df['total_gas'].subtract(final_df['gas'])
final_df['transaction_fee'] = fff
thir_q = final_df.sort_values(by = "transaction_fee", kind='heapsort')
print(thir_q[['tx_hash','block_number','block_time','transaction_fee']])
print("--- Third Question Ran in %s seconds ---" % (time.time() - start_time))

### Fourth Question ###
start_time = time.time()

fourth_q = final_df.sort_values(["block_number", "gas_price"],ascending = (True, True), kind = 'heapsort')[['tx_hash','gas_price','block_number','block_time']]
#fourth_q.to_csv('/Users/srikaramara/Desktop/four.csv')
print(fourth_q)
print("--- Fourth Question Ran in %s seconds ---" % (time.time() - start_time))

### Fifth Question ###

start_time = time.time()

fifth_q = tr_data.sort_values(["block_number", "from_addr","to_addr"],ascending = (True, True,True), kind = 'heapsort')[['from_addr','to_addr','tx_hash','block_number','block_time']]
print(fifth_q)

print("--- Fifth Question Ran in %s seconds ---" % (time.time() - start_time))

### Sixth Question ###
start_time = time.time()

six_pd = thir_q.loc[thir_q['block_number'] == 11344115] # O(n) Simple Search
# six_pd.to_csv('/Users/srikaramara/Desktop/sixth.csv')
print(six_pd[['tx_hash','transaction_fee','tx_index_in_block','block_number','block_time']])

print("--- Sixth Question Ran in %s seconds ---" % (time.time() - start_time))

### Seventh Question ###

start_time = time.time()

sev_pd = thir_q.loc[thir_q['tx_hash'] == '0xd9da28fefdcd33f0bfee00b4b159c092c8e1f627d224c2856216d5ccedfdbdf3']

# sev_pd.to_csv('/Users/srikaramara/Desktop/seventh.csv')
print(sev_pd [['tx_hash','transaction_fee','tx_index_in_block','block_number','block_time']])

print("--- Seventh Question Ran in %s seconds ---" % (time.time() - start_time))

### Eigth Question ###

start_time = time.time()

eight_df = final_df.loc[final_df['from_addr'] == '0x47ddfddff875851ba18526cb30e0d35868c8c79a']

print(eight_df[['from_addr','tx_hash','block_number','block_time','transaction_fee']])

# eight_df.to_csv('/Users/srikaramara/Desktop/eigth.csv')
print("--- Eigth Question Ran in %s seconds ---" % (time.time() - start_time))

### Nineth Question ###

start_time = time.time()

nineth_df = final_df.loc[final_df['to_addr'] == '0x7a250d5630b4cf539739df2c5dacb4c659f2488d']

print(nineth_df[['from_addr','tx_hash','block_number','block_time','transaction_fee']])

# nineth_df.to_csv('/Users/srikaramara/Desktop/nineth_df.csv')

print("--- Nineth Question Ran in %s seconds ---" % (time.time() - start_time))

### Tenth Question ###
start_time = time.time()

from_addr = '0xcfa770b0f8970286c839724f94d46db8a71be39a'
mx = tr_data.loc[tr_data['from_addr'] == from_addr]['token_qty'].max()
mn = tr_data.loc[tr_data['from_addr'] == from_addr]['token_qty'].min()

d = {'from_addr': from_addr, 'max_token_transfer': [mx], 'min_token_tranfer': [mn]}
tenth_pd = pd.DataFrame(data=d)

# tenth_pd.to_csv('/Users/srikaramara/Desktop/tenth_df.csv')
print(tenth_pd)

print("--- Tenth Question Ran in %s seconds ---" % (time.time() - start_time))

### Tenth Question ###
start_time = time.time()

to_addr = '0xf979e6b2fae9e1c5c3cd43ea9fae5e0711c823ae'
mx = tr_data.loc[tr_data['to_addr'] == to_addr]['token_qty'].max()
mn = tr_data.loc[tr_data['to_addr'] == to_addr]['token_qty'].min()

d = {'to_addr': to_addr, 'max_token_transfer': [mx], 'min_token_tranfer': [mn]}
tenth_pd_to = pd.DataFrame(data=d)
print(tenth_pd_to)

print("--- Tenth Question to_addr Ran in %s seconds ---" % (time.time() - start_time))


print ("______________________________________________________________________________________________________________________________________________________________________________________________")
end_time_prog = time.time()

print("Total Execution time for the program to run is %s " % (end_time_prog - start_time_prog))
