# Databricks notebook source
# Obtain the text data from URL
import urllib.request
stringInURL = "http://gutenberg.org/files/65119/65119-0.txt"
urllib.request.urlretrieve(stringInURL, "/tmp/mohanalavala.txt")

# COMMAND ----------

dbutils.fs.mv("file:/tmp/mohanalavala.txt", "dbfs:/data/mohanalavala.txt")

# COMMAND ----------

mohan_RDD = sc.textFile("dbfs:/data/mohanalavala.txt")

# COMMAND ----------

# flatmap each line to words
Words_RDD = mohan_RDD.flatMap(lambda line : line.lower().strip().split(" "))

# COMMAND ----------

import re
Clean_Tokens_RDD = Words_RDD.map(lambda w: re.sub(r'[^a-zA-Z]','',w))

# COMMAND ----------

#prepare to clean stopwords
from pyspark.ml.feature import StopWordsRemover
remove =StopWordsRemover()
stopwords = remove.getStopWords()
Clean_Words_RDD=Clean_Tokens_RDD.filter(lambda wrds: wrds not in stopwords)

# COMMAND ----------

IKVPairsRDD= Clean_Words_RDD.map(lambda word: (word,1))

# COMMAND ----------

Word_Count_RDD = IKVPairsRDD.reduceByKey(lambda acc, value: acc+value)

# COMMAND ----------

mohanresults = Word_Count_RDD.collect()

# COMMAND ----------

mohanresults = Word_Count_RDD.map(lambda x: (x[1], x[0])).sortByKey(False).take(15)
print(mohanresults)

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter

df = pd.DataFrame.from_records(mohanresults, columns =[plt.xlabel, plt.ylabel]) 
print(df)

mostCommon=mohanresults[5:10]
word,count = zip(*mostCommon)
import matplotlib.pyplot as plt
fig = plt.figure()
plt.bar(word,count,color="salmon")
plt.xlabel("Words")
plt.ylabel("Count")
plt.title("Most used words in Homecoming Horde")
plt.show()

# COMMAND ----------


