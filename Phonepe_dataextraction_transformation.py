#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install mysql.connector.python')
get_ipython().system('pip install gitpython')
get_ipython().system('pip install pandas')


# In[2]:


import pandas as pd
import mysql.connector as sql
import os
import json
import git
from git.repo.base import Repo

#Data Extraction
os.environ["GIT_PYTHON_REFRESH"] = "quiet"

Repo.clone_from("https://github.com/PhonePe/pulse.git", "F:\CapstoneProject\Phonepe Data")

repo = Repo("F:\CapstoneProject\Phonepe Data")
origin = repo.remote(name="origin")
origin.pull()



# In[3]:


path1 = "F:\\CapstoneProject\\Phonepe Data\\data\\aggregated\\transaction\\country\\india\\state"
aggregated_transaction_list = os.listdir(path1)

column1 ={"State":[], "Year":[], "Quarter":[], "Transaction_type":[], "Transaction_count":[], "Transaction_amount":[]}
for state in aggregated_transaction_list:
    current_state = os.path.join(path1, state) + "\\"
    aggregated_year_list = os.listdir(current_state)

    for year in aggregated_year_list:
        current_year = os.path.join(current_state, year) + "\\"
        aggregated_file_list = os.listdir(current_year)

        for file in aggregated_file_list:
            current_file = os.path.join(current_year, file)
            data = open(current_file, "r")
            A = json.load(data)

            for i in A['data']['transactionData']:
                name = i['name']
                count = i['paymentInstruments'][0]['count']
                amount = i['paymentInstruments'][0]['amount']
                column1['Transaction_type'].append(name)
                column1['Transaction_count'].append(count)
                column1['Transaction_amount'].append(amount)
                column1['State'].append(state)
                column1['Year'].append(year)
                column1['Quarter'].append(int(file.strip('.json')))
df_agg_trans = pd.DataFrame(column1)


# In[4]:


df_agg_trans


# In[28]:


path2 = "F:\\CapstoneProject\\Phonepe Data\\data\\aggregated\\user\\country\\india\\state"
aggregated_user_list = os.listdir(path2)

column2 ={"State":[], "Year":[], "Quarter":[], "Brands":[], "Count":[], "Percentage":[]}
for state in aggregated_user_list:
    current_state = os.path.join(path2, state) + "\\"
    aggregated_year_list = os.listdir(current_state)

    for year in aggregated_year_list:
        current_year = os.path.join(current_state, year) + "\\"
        aggregated_file_list = os.listdir(current_year)

        for file in aggregated_file_list:
            current_file = os.path.join(current_year, file)
            data = open(current_file, 'r')
            B = json.load(data)
            try:
                for i in B['data']['usersByDevice']:
                    brand_name = i["brand"]
                    counts = i["count"]
                    percents = i["percentage"]
                    column2["Brands"].append(brand_name)
                    column2["Count"].append(counts)
                    column2["Percentage"].append(percents)
                    column2["State"].append(state)
                    column2["Year"].append(year)
                    column2["Quarter"].append(int(file.strip('.json')))
            except:
                pass
df_agg_user = pd.DataFrame(column2)


# In[29]:


pd.DataFrame(column2)


# In[7]:


path3 = "F:\\CapstoneProject\\Phonepe Data\\data\\map\\transaction\\hover\\country\\india\\state"

map_transaction_list = os.listdir(path3)

column3 = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'Count': [],
            'Amount': []}

for state in map_transaction_list:
    current_state = os.path.join(path3, state) + "\\"
    map_year_list = os.listdir(current_state)
    
    for year in map_year_list:
        current_year = os.path.join(current_state, year) + "\\"
        map_file_list = os.listdir(current_year)
        
        for file in map_file_list:
            current_file = os.path.join(current_year, file)
            data = open(current_file, 'r')
            C = json.load(data)
            
            for i in C["data"]["hoverDataList"]:
                district = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                column3["District"].append(district)
                column3["Count"].append(count)
                column3["Amount"].append(amount)
                column3['State'].append(state)
                column3['Year'].append(year)
                column3['Quarter'].append(int(file.strip('.json')))
                
df_map_trans = pd.DataFrame(column3)


# In[8]:


df_map_trans


# In[9]:


path4 = "F:\\CapstoneProject\\Phonepe Data\\data\\map\\user\\hover\\country\\india\\state"

map_user_list = os.listdir(path4)

column4 = {"State": [], "Year": [], "Quarter": [], "District": [],
            "RegisteredUser": [], "AppOpens": []}

for state in map_user_list:
    current_state = os.path.join(path4, state) + "\\"
    map_year_list = os.listdir(current_state)
    
    for year in map_year_list:
        current_year = os.path.join(current_state, year) + "\\"
        map_file_list = os.listdir(current_year)
        
        for file in map_file_list:
            current_file = os.path.join(current_year, file)
            data = open(current_file, 'r')
            D = json.load(data)
            
            for i in D["data"]["hoverData"].items():
                district = i[0]
                registereduser = i[1]["registeredUsers"]
                appOpens = i[1]['appOpens']
                column4["District"].append(district)
                column4["RegisteredUser"].append(registereduser)
                column4["AppOpens"].append(appOpens)
                column4['State'].append(state)
                column4['Year'].append(year)
                column4['Quarter'].append(int(file.strip('.json')))
                
df_map_user = pd.DataFrame(column4)


# In[10]:


df_map_user


# In[11]:


path5 = "F:\\CapstoneProject\\Phonepe Data\\data\\top\\transaction\\country\\india\\state"

top_transaction_list = os.listdir(path5)
column5 = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Transaction_count': [],
            'Transaction_amount': []}

for state in top_transaction_list:
    current_state = os.path.join(path5, state) + "\\"
    top_year_list = os.listdir(current_state)
    
    for year in top_year_list:
        current_year = os.path.join(current_state, year) + "\\"
        top_file_list = os.listdir(current_year)
        
        for file in top_file_list:
            current_file = os.path.join(current_year, file)
            data = open(current_file, 'r')
            E = json.load(data)
            
            for i in E['data']['pincodes']:
                name = i['entityName']
                count = i['metric']['count']
                amount = i['metric']['amount']
                column5['Pincode'].append(name)
                column5['Transaction_count'].append(count)
                column5['Transaction_amount'].append(amount)
                column5['State'].append(state)
                column5['Year'].append(year)
                column5['Quarter'].append(int(file.strip('.json')))
df_top_trans = pd.DataFrame(column5)


# In[12]:


df_top_trans


# In[13]:


path6 = "F:\\CapstoneProject\\Phonepe Data\\data\\top\\user\\country\\india\\state"

top_user_list = os.listdir(path5)
column6 = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [],
            'RegisteredUsers': []}

for state in top_user_list:
    current_state = os.path.join(path6, state) + "\\"
    top_year_list = os.listdir(current_state)
    
    for year in top_year_list:
        current_year = os.path.join(current_state, year) + "\\"
        top_file_list = os.listdir(current_year)
        
        for file in top_file_list:
            current_file = os.path.join(current_year, file)
            data = open(current_file, 'r')
            F = json.load(data)
            
            for i in F['data']['pincodes']:
                name = i['name']
                registeredUsers = i['registeredUsers']
                column6['Pincode'].append(name)
                column6['RegisteredUsers'].append(registeredUsers)
                column6['State'].append(state)
                column6['Year'].append(year)
                column6['Quarter'].append(int(file.strip('.json')))
df_top_user = pd.DataFrame(column6)


# In[14]:


df_top_user


# In[15]:


#.json to .csv file conversion
df_agg_trans.to_csv('aggregated_transaction.csv',index=False)
df_agg_user.to_csv('aggregated_user.csv',index=False)
df_map_trans.to_csv('map_transaction.csv',index=False)
df_map_user.to_csv('map_user.csv',index=False)
df_top_trans.to_csv('top_transaction.csv',index=False)
df_top_user.to_csv('top_user.csv',index=False)


# In[16]:


# MySQL connection
mydb=sql.connect(host='127.0.0.1',
                 user='root',
                 password='Qwerty@09876',
                 database='phonepe_pulse')
cursor=mydb.cursor()


# aggregated_transaction
cursor.execute("CREATE TABLE aggregated_transaction (State varchar(100), Year int, Quarter int, Transaction_type varchar(100), Transaction_count int, Transaction_amount double)")
query = "INSERT INTO aggregated_transaction (State, Year, Quarter, Transaction_type, Transaction_count, Transaction_amount) VALUES (%s, %s, %s, %s, %s, %s)"

for index, row in df_agg_trans.iterrows():
    values = (row['State'], row['Year'], row['Quarter'], row['Transaction_type'], row['Transaction_count'], row['Transaction_amount'])
    cursor.execute(query, values)

mydb.commit()


# In[17]:


# aggregated_user
cursor.execute("CREATE TABLE  aggregated_user (State varchar(100), Year int, Quarter int, Brands varchar(100), Count int, Percentage double)")
query = "INSERT INTO aggregated_user (State, Year, Quarter, Brands, Count, Percentage) VALUES (%s, %s, %s, %s, %s, %s)"

for index, row in df_agg_user.iterrows():
    values = (row['State'], row['Year'], row['Quarter'], row['Brands'], row['Count'], row['Percentage'])
    cursor.execute(query, values)

mydb.commit()


# In[18]:


# map_transaction
cursor.execute("CREATE TABLE  map_transaction (State varchar(100), Year int, Quarter int, District varchar(100), Count int, Amount double)")
query = "INSERT INTO map_transaction (State, Year, Quarter, District, Count, Amount) VALUES (%s, %s, %s, %s, %s, %s)"

for index, row in df_map_trans.iterrows():
    values = (row['State'], row['Year'], row['Quarter'], row['District'], row['Count'], row['Amount'])
    cursor.execute(query, values)

mydb.commit()


# In[19]:


# map_user
cursor.execute("CREATE TABLE  map_user (State varchar(100), Year int, Quarter int, District varchar(100), RegisteredUser int, AppOpens int)")
query = "INSERT INTO map_user (State, Year, Quarter, District, RegisteredUser, AppOpens) VALUES (%s, %s, %s, %s, %s, %s)"

for index, row in df_map_user.iterrows():
    values = (row['State'], row['Year'], row['Quarter'], row['District'], row['RegisteredUser'], row['AppOpens'])
    cursor.execute(query, values)

mydb.commit()


# In[20]:


# top_transaction
cursor.execute("CREATE TABLE top_transaction (State varchar(100), Year int, Quarter int, Pincode varchar(100), Transaction_count int, Transaction_amount double)")
query = "INSERT INTO top_transaction (State, Year, Quarter, Pincode, Transaction_count, Transaction_amount) VALUES (%s, %s, %s, %s, %s, %s)"

for index, row in df_top_trans.iterrows():
    values = (row['State'], row['Year'], row['Quarter'], row['Pincode'], row['Transaction_count'], row['Transaction_amount'])
    cursor.execute(query, values)

mydb.commit()


# In[21]:


# top_user
cursor.execute("CREATE TABLE top_user (State varchar(100), Year int, Quarter int, Pincode varchar(100), RegisteredUsers int)")
query = "INSERT INTO top_user (State, Year, Quarter, Pincode, RegisteredUsers) VALUES (%s, %s, %s, %s, %s)"

for index, row in df_top_user.iterrows():
    values = (row['State'], row['Year'], row['Quarter'], row['Pincode'], row['RegisteredUsers'])
    cursor.execute(query, values)

mydb.commit()

