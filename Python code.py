#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[7]:


file_path = "C:/Users/himal/Downloads/archive - 2025-08-01T231313.369/HI-Medium_Trans.csv"


# In[9]:


df=pd.read_csv(file_path, nrows =5)
df


# In[10]:


#Read the file in chunks
chunk_size = 100000
chunks=[]
row_limit = 50000
print("Reading file in chunks..")
for chunk in pd.read_csv(file_path,chunksize=chunk_size):
    chunks.append(chunk)
    if sum(len(c) for c in chunks) >= row_limit:
        break


# In[11]:


#Combine first 50k rows into one dataframe
sample_df = pd.concat(chunks).head(row_limit)


# In[12]:


#Show basic info
print("\n Columns in the dataset:")
print(sample_df.columns.tolist())

print("\nFirst 5 rows:")
print(sample_df.head())


# In[13]:


sample_file= ("C:/Users/himal/Desktop/Project/BMO Project/Trans.csv")
sample_df.to_csv(sample_file,index=False)
print("\n A 50k-row sample file has been saved as:{sample_file}")


# In[23]:


import pandas as pd

# ✅ Load the files
trans_df = pd.read_csv("C:/Users/himal/Desktop/Project/BMO Project/Trans.csv")
accounts_df = pd.read_csv("C:/Users/himal/Desktop/Project/BMO Project/HI-Medium_accounts.csv")

# ✅ Convert both Account columns to string (so merge works)
trans_df["Account"] = trans_df["Account"].astype(str)
accounts_df["Account Number"] = accounts_df["Account Number"].astype(str)

# ✅ Merge Transactions with Accounts (using 'Account' field)
merged_from = trans_df.merge(accounts_df, left_on="Account", right_on="Account Number", how="left")

# ✅ Rename columns to clarify
merged_from = merged_from.rename(columns={
    "Bank Name": "From Bank Name",
    "Entity Name": "From Entity Type"
})

# ✅ Preview first 10 rows
merged_from.head(10)


# In[24]:


# ✅ Merge for the TO side using 'Account.1' column
merged_full = merged_from.merge(accounts_df,
                                left_on="Account.1",
                                right_on="Account Number",
                                how="left",
                                suffixes=("", "_to"))

# ✅ Rename TO side columns
merged_full = merged_full.rename(columns={
    "Bank Name": "To Bank Name",
    "Entity Name": "To Entity Type"
})

# ✅ Preview first 10 rows
merged_full.head(10)


# In[25]:


import pandas as pd


# In[26]:


#Load the patterns file
patterns_df = pd.read_csv("C:/Users/himal/Desktop/Project/BMO Project/HI-Medium_Patterns.txt", sep="\t")


# In[27]:


print("Rows and Columns:",patterns_df.shape)
print("\n Column Names:",patterns_df.columns.tolist())


# In[28]:


#Preview the first 10 rows
patterns_df.head(10)


# In[ ]:





# In[51]:


import numpy as np

# 1️⃣ Detect STACK or CYCLE from BEGIN or END lines
patterns_df['Pattern_Type'] = patterns_df['BEGIN LAUNDERING ATTEMPT - STACK'].apply(
    lambda x: 'STACK' if 'STACK' in x else ('CYCLE' if 'CYCLE' in x else None)
)

# 2️⃣ Fill DOWN so CYCLE moves downward
patterns_df['Pattern_Type'] = patterns_df['Pattern_Type'].ffill()

# 3️⃣ Also fill UP so STACK from END lines moves UP to tag accounts above
patterns_df['Pattern_Type'] = patterns_df['Pattern_Type'].bfill()

# 4️⃣ Extract account numbers
patterns_df['Account_Numbers'] = patterns_df['BEGIN LAUNDERING ATTEMPT - STACK'].apply(
    lambda x: re.findall(r'[A-Z0-9]{9}', x) if isinstance(x, str) else []
)

# 5️⃣ Clean up junk: remove rows with no account numbers or “LAUNDERIN”
patterns_clean = patterns_df[patterns_df['Account_Numbers'].apply(lambda x: len(x) > 0)]
patterns_clean = patterns_clean[~patterns_clean['Account_Numbers'].apply(lambda x: 'LAUNDERIN' in x)]

# 6️⃣ Explode so each account gets its own row
patterns_exploded = patterns_clean.explode('Account_Numbers').reset_index(drop=True)

# ✅ Preview
patterns_exploded.head(20)




# In[49]:


patterns_exploded.tail(20)


# In[52]:


# See the first 20 raw lines from the Patterns file
for i, row in enumerate(patterns_df['BEGIN LAUNDERING ATTEMPT - STACK'].head(20)):
    print(i, row)


# In[58]:


print("Sample transaction accounts:", merged_full['Account'].head(20).tolist())
print("Sample pattern accounts:", patterns_exploded['Account_Numbers'].head(20).tolist())



# In[60]:


print("Sample Account Number:", merged_full['Account Number'].head(20).tolist())
print("Sample Account Number_to:", merged_full['Account Number_to'].head(20).tolist())


# In[59]:


print(merged_full.columns)


# In[61]:


# ✅ Merge on the sender's actual account number (Account Number)
master_with_from_pattern = merged_full.merge(
    patterns_exploded[['Account_Numbers', 'Pattern_Type']],
    left_on='Account Number',
    right_on='Account_Numbers',
    how='left'
).rename(columns={'Pattern_Type': 'From_Pattern_Type'})

# ✅ Merge on the receiver's actual account number (Account Number_to)
master_with_patterns = master_with_from_pattern.merge(
    patterns_exploded[['Account_Numbers', 'Pattern_Type']],
    left_on='Account Number_to',
    right_on='Account_Numbers',
    how='left',
    suffixes=('', '_to')
).rename(columns={'Pattern_Type': 'To_Pattern_Type'})

# ✅ Drop duplicate merge helper columns
master_with_patterns = master_with_patterns.drop(columns=['Account_Numbers', 'Account_Numbers_to'])

# ✅ Preview
master_with_patterns[['Account Number', 'Account Number_to', 'From_Pattern_Type', 'To_Pattern_Type']].head(20)


# In[63]:


print(master_with_patterns['From_Pattern_Type'].value_counts(dropna=False))
print(master_with_patterns['To_Pattern_Type'].value_counts(dropna=False))


# In[64]:


def classify_pattern(row):
    if row['From_Pattern_Type'] == 'STACK' or row['To_Pattern_Type'] == 'STACK':
        return 'STACK'
    elif row['From_Pattern_Type'] == 'CYCLE' or row['To_Pattern_Type'] == 'CYCLE':
        return 'CYCLE'
    else:
        return 'CLEAN'

master_with_patterns['Transaction_Pattern'] = master_with_patterns.apply(classify_pattern, axis=1)

# ✅ Count transactions by final pattern
print(master_with_patterns['Transaction_Pattern'].value_counts())


# In[65]:



master_with_patterns.to_csv("C:/Users/himal/Desktop/Project/BMO Project/HI_Medium_Enriched.csv", index=False)


# In[ ]:




