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


# Load Rank List
Rank_List = pd.read_csv("Latest_Month_Rank_List.csv")


# In[3]:


# Single input ticket
Input_opened_ticket = 'fwmp_open.orc'
opened_ticket = pd.read_orc(Input_opened_ticket)
opened_ticket = opened_ticket[['valuedatetime', 'complaintno', 'problemdesc', 'areaname']]
opened_ticket.dropna(inplace=True)


# In[6]:


# Merging the DataFrames on 'problemdesc' and 'areaname'
result = pd.merge(opened_ticket, Rank_List, on=['problemdesc', 'areaname'], how='inner')
result = result.sort_values(by='rank', ascending=True)
result


# In[ ]:




