#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


# In[2]:


Input_closed_ticket = 'fwmp_closed.orc'
Input_opened_ticket = 'fwmp_open_unique.orc'


# In[3]:


opened_tickets = pd.read_orc(Input_opened_ticket)
closed_tickets = pd.read_orc(Input_closed_ticket)
opened_tickets = opened_tickets[['valuedatetime', 'complaintno', 'problemdesc', 'areaname']]
opened_tickets.dropna(inplace=True)
closed_tickets = closed_tickets[['valuedatetime', 'resolveddatetime','complaintno', 'problemdesc', 'areaname', 'first_time_assigned', 'closedby']]
closed_tickets = closed_tickets.drop_duplicates()
closed_tickets.dropna(inplace=True)
# opened_tickets = opened_tickets.drop(['assignedby'],axis = 1)


# In[4]:


merged_df = pd.merge(closed_tickets, opened_tickets, how='inner', on=['valuedatetime','complaintno','problemdesc', 'areaname'])


# In[5]:


merged_df = merged_df.drop_duplicates()
merged_df['valuedatetime'] = pd.to_datetime(merged_df['valuedatetime'])
merged_df['valuedatetime'] = merged_df['valuedatetime'].dt.strftime('%Y-%m-%d')
merged_df['resolveddatetime'] = pd.to_datetime(merged_df['resolveddatetime'])
merged_df['resolveddatetime'] = merged_df['resolveddatetime'].dt.strftime('%Y-%m-%d')


# In[6]:


result_data_1 = {'problemdesc': [], 'first_time_assigned': [], 'valuedatetime': [], 'first_time_count': [], 'closed_count': []}

for index, row in merged_df.iterrows():
    first_assigned = row['first_time_assigned']
    closed_by = row['closedby']
    problemdesc = row['problemdesc']
    
    # Check if resolveddatetime is the same as valuedatetime
    if row['resolveddatetime'] == row['valuedatetime']:
        # Increase first_time_count for first_time_assigned
        if first_assigned != closed_by:
            result_data_1['first_time_count'].append(result_data_1['first_time_count'].count(first_assigned) + 1)
            result_data_1['closed_count'].append(result_data_1['closed_count'].count(closed_by))
        else:
            result_data_1['first_time_count'].append(result_data_1['first_time_count'].count(first_assigned))
            # Increase closed_count for closedby
            result_data_1['closed_count'].append(result_data_1['closed_count'].count(closed_by) + 1)
    else:
        # Increase first_time_count for first_time_assigned
        result_data_1['first_time_count'].append(result_data_1['first_time_count'].count(first_assigned) + 1)
        
        # For tickets not resolved at the same time, closed_count is 0
        result_data_1['closed_count'].append(result_data_1['closed_count'].count(closed_by))
    
    # Store other columns in the result data
    result_data_1['first_time_assigned'].append(first_assigned)
    result_data_1['valuedatetime'].append(row['valuedatetime'])
    result_data_1['problemdesc'].append(problemdesc)

result_1 = pd.DataFrame(result_data_1)
prob_by_problemdesc = result_1.groupby(['problemdesc', 'first_time_assigned', 'valuedatetime']).agg({
    'first_time_count': 'sum',
    'closed_count': 'sum'
}).reset_index()


# In[7]:


#load factor k
k = 3

# Clip 'first_time_count' to a maximum value of k
prob_by_problemdesc['first_time_count'] = prob_by_problemdesc['first_time_count'].clip(upper=k)

# Calculate probability for each row
prob_by_problemdesc['probability'] = prob_by_problemdesc['closed_count'] / (prob_by_problemdesc['first_time_count'] + prob_by_problemdesc['closed_count'])


# In[8]:


result_data_2 = {'problemdesc': [], 'areaname': [], 'first_time_assigned': [], 'valuedatetime': [], 'first_time_count': [], 'closed_count': []}

for index, row in merged_df.iterrows():
    first_assigned = row['first_time_assigned']
    closed_by = row['closedby']
    problemdesc = row['problemdesc']
    areaname = row['areaname']
    
    # Check if resolveddatetime is the same as valuedatetime
    if row['resolveddatetime'] == row['valuedatetime']:
        # Increase first_time_count for first_time_assigned
        if first_assigned != closed_by:
            result_data_2['first_time_count'].append(result_data_2['first_time_count'].count(first_assigned) + 1)
            result_data_2['closed_count'].append(result_data_2['closed_count'].count(closed_by))
        else:
            result_data_2['first_time_count'].append(result_data_2['first_time_count'].count(first_assigned))
            # Increase closed_count for closedby
            result_data_2['closed_count'].append(result_data_2['closed_count'].count(closed_by) + 1)
    else:
        # Increase first_time_count for first_time_assigned
        result_data_2['first_time_count'].append(result_data_2['first_time_count'].count(first_assigned) + 1)
        
        # For tickets not resolved at the same time, closed_count is 0
        result_data_2['closed_count'].append(result_data_2['closed_count'].count(closed_by))
    
    # Store other columns in the result data
    result_data_2['first_time_assigned'].append(first_assigned)
    result_data_2['valuedatetime'].append(row['valuedatetime'])
    result_data_2['problemdesc'].append(problemdesc)
    result_data_2['areaname'].append(areaname)

result_2 = pd.DataFrame(result_data_2)


prob_by_problemdesc_area = result_2.groupby(['problemdesc', 'areaname', 'first_time_assigned', 'valuedatetime']).agg({
    'first_time_count': 'sum',
    'closed_count': 'sum'
}).reset_index()


# In[9]:


# Clip 'first_time_count' to a maximum value of k
prob_by_problemdesc_area['first_time_count'] = prob_by_problemdesc_area['first_time_count'].clip(upper=k)

# Calculate probability for each row
prob_by_problemdesc_area['probability'] = prob_by_problemdesc_area['closed_count'] / (prob_by_problemdesc_area['first_time_count'] + prob_by_problemdesc_area['closed_count'])


# In[10]:


prob_by_problemdesc_area['Difference'] = 0.0


for index, row in prob_by_problemdesc_area.iterrows():
    problemdesc = row['problemdesc']
    first_time_assigned = row['first_time_assigned']
    valuedatetime = row['valuedatetime']
    probability_area = row['probability']
    
    
    matching_rows = prob_by_problemdesc[(prob_by_problemdesc['problemdesc'] == problemdesc) & (prob_by_problemdesc['first_time_assigned'] == first_time_assigned) & (prob_by_problemdesc['valuedatetime'] == valuedatetime)]
    
    if not matching_rows.empty:
        probability_problemdesc = matching_rows.iloc[0]['probability']
        if(probability_problemdesc == 0) & (probability_area == 0):
            prob_by_problemdesc_area.at[index, 'Difference'] = 1
        else:
            difference =  probability_problemdesc - probability_area
            prob_by_problemdesc_area.at[index, 'Difference'] = difference

prob_by_problemdesc_area.drop('probability', axis = 1, inplace = True)


# In[11]:


from datetime import datetime, timedelta

prob_by_problemdesc_area['valuedatetime'] = pd.to_datetime(prob_by_problemdesc_area['valuedatetime'])

# Group by 'problemdesc', 'areaname', and Monthly intervals
prob_by_problemdesc_area = prob_by_problemdesc_area.groupby(['problemdesc', 'areaname','first_time_assigned', pd.Grouper(key='valuedatetime', freq='M')])

# Calculate the average of 'Difference' for each group
average_difference = prob_by_problemdesc_area['Difference'].mean().reset_index()


# In[12]:


average_difference['rank'] = average_difference.groupby(['problemdesc', 'areaname', 'valuedatetime'])['Difference'].rank(method='min')
average_difference = average_difference.sort_values(by='valuedatetime', ascending=True)


# In[13]:


# Find the maximum date (most recent date)
max_date = average_difference['valuedatetime'].max()

# Calculate the first day of the month for the most recent date
start_of_last_month = max_date.replace(day=1)

# Filter data for the last month
last_month_data = average_difference[(average_difference['valuedatetime'] >= start_of_last_month) & (average_difference['valuedatetime'] <= max_date)]
last_month_data = last_month_data.drop(columns=['valuedatetime', 'Difference'])
last_month_data.rename(columns={'first_time_assigned': 'NEs'}, inplace=True)
last_month_data = last_month_data.sort_values(by=['problemdesc', 'areaname'])
last_month_data.to_csv('Latest_Month_Rank_List.csv', index=False)


# In[14]:


last_month_data


# In[ ]:




