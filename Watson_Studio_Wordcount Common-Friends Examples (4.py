#!/usr/bin/env python
# coding: utf-8

# In[71]:


sc.version


# PART-1:  "WORDCOUNT" EXAMPLE USING MAPREDUCE

# In[1]:


# Fetch the text file for wordcount example
get_ipython().system('wget https://raw.githubusercontent.com/ibarabasi/wordcount/master/wordcount')
get_ipython().system('cat wordcount')


# In[2]:


#Simple example to read text file
rdd0 = sc.textFile("wordcount")
rdd0.take(20)


# In[3]:


word_counts = rdd0.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda a, b: (a + b)).map(lambda x:(x[1],x[0]))

word_counts.take(30)


# In[ ]:





# PART-2:  "COMMON FRIENDS" EXAMPLE USINg MAPREDUCE

# In[4]:


# Load data from github
get_ipython().system('wget "https://raw.githubusercontent.com/ibarabasi/wordcount/master/friends"')
rdd = sc.textFile("friends")
get_ipython().system('cat friends')


# In[5]:


get_ipython().system('cat wordcount/friends')
# Build the first pair RDD
rdd0 = rdd.map(lambda x: x.split())
rdd0.take(20)


# In[6]:


rdd1=rdd0.union(rdd.map(lambda x: x.split()[::-1]))
# Bring my friend list to local
lst = rdd1.filter(lambda x: x[0] == 'me').map(lambda x: x[1]).collect()
# Build the second pair RDD
rdd2 = rdd1.filter(lambda x: x[0] in lst).map(lambda x: x[1]).     filter(lambda x: x != 'me' and x not in lst).     map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b).     map(lambda x: (x[1], x[0])).sortByKey(ascending = False)
# Bring the result to local since the sample is small
for x, y in rdd2.collect():
    print ("The stranger {} has {} common friends with me".format(y, x))


# In[ ]:




