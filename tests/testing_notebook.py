#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd 
import numpy as np
from bigcsv import BigCSV
import os 


# In[2]:


rand = np.random.rand(500, 1000)
rand = pd.DataFrame(rand)

rand.to_csv('test.csv', index=False)
df = pd.read_csv('test.csv')
df.columns = pd.RangeIndex(df.shape[1])


# In[3]:


df


# In[4]:


trans = BigCSV('test.csv', 'test_modified.csv')


# In[5]:


trans.transpose_csv()


# In[6]:


result = pd.read_csv('test_modified.csv')
result.head()


# In[7]:


res = pd.read_csv('test_modified.csv')

res = res.T


# In[8]:


res


# In[9]:


res.index = df.index
res


# In[10]:


df


# In[11]:


np.linalg.norm(res - df)


# In[12]:


trans.to_h5ad(outfile='result.h5ad', index=False)


# In[13]:


import anndata as an

res = an.read_h5ad('result.h5ad')


# In[14]:


res.to_df().reset_index(drop=True)


# In[15]:


resdf = res.to_df()

resdf.index = df.index 
resdf.columns = df.columns

(resdf - df).sum().sum()


# In[ ]:




