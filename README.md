# mohanalavala-finalproject-bigdata
### - **Author - Mohan Krishna Alavala**
## TextData Source Links: 
**Website**: https://www.gutenberg.org/
**Text Data URL**: http://gutenberg.org/files/65119/65119-0.txt 
## Tools and Languages used:
- Python
- Pyspark
- Databricks 
- Urllib
- Pandas 
- MatPlotLib

## Steps involved in Process:
1. To begin, we'll use the urllib.request library to request or pull data from the text data's url. The data is stored in a temporary file called 'mohanalavala.txt' which will get the text data from the website https://www.gutenberg.org/ and title is 'Homecoming Horde'
```
# Obtain the text data from URL
import urllib.request
stringInURL = "http://gutenberg.org/files/65119/65119-0.txt"
urllib.request.urlretrieve(stringInURL, "/tmp/mohanalavala.txt")
```
2.Now that the data has been saved, we'll use dbutils.fs.mv to transfer it from the temporary data to a new site called data.
```
dbutils.fs.mv("file:/tmp/mohanalavala.txt", "dbfs:/data/mohanalavala.txt")
```
3. We'll use sc.textfile in this step to import the data file into Spark's RDD (Resilient Distributed Systems), which is a collection of elements that can be worked on in parallel. RDDs can be made in one of two ways: by parallelizing an existing array in your driver program, or by referencing a dataset in an external storage system such as a shared filesystem, HDFS, HBase, or another data source which supports RDDs
```
mohan_RDD = sc.textFile("dbfs:/data/mohanalavala.txt")
```
### Clean Data
4. The above data file includes capitalized phrases, sentences, punctuation, and stopwords (words that don't add much context to a sentence). (Examples: the, have, etc). In the first step of cleaning the info, we'll use flatMap to break it down, changing any capitalized terms to lower case, removing any spaces, and breaking sentences into words.
```
# flatmap each line to words
Words_RDD = mohan_RDD.flatMap(lambda line : line.lower().strip().split(" "))
```
5.The punctuation is the next step. We'll use the re(regular expression) library for something that doesn't look like a file.
```
import re
Clean_Tokens_RDD = Words_RDD.map(lambda w: re.sub(r'[^a-zA-Z]','',w))
```
6. The next step is to use pyspark.ml.feature to delete any stopwords by importing stopwords remover.
```
#prepare to clean stopwords
from pyspark.ml.feature import StopWordsRemover
remove =StopWordsRemover()
stopwords = remove.getStopWords()
Clean_Words_RDD=Clean_Tokens_RDD.filter(lambda wrds: wrds not in stopwords)
```
### Processing the Data:
7. The next move is to process the data after it has been cleaned. Our terms will be transformed into key-value pairs in the middle. We'll make a pair of (", 1) pairs for each word variable in the RDD. By combining the map() transformation with the lambda() function, we can create a pair RDD. It will look like this once we've mapped it out: (word,1).
```
IKVPairsRDD= Clean_Words_RDD.map(lambda word: (word,1))
```
8. In this step, we'll perform the Reduce by key process. The response is the expression. Each time, we'll keep track of the first word count. If the same word appears more than once, the most recent one will be deleted and the first word count will be retained.
```
Word_Count_RDD = IKVPairsRDD.reduceByKey(lambda acc, value: acc+value)
```
9. We will retrieve the elements in this step. The collect() action function is used to return to the driver program all elements from the dataset(RDD/DataFrame/Dataset) as an Array(row).
 ``` 
mohanresults = Word_Count_RDD.collect()
 ```
10. Sorting a tuple list by the second value, which will be used to reorder the tuples in the list. The second values of Tuples are mentioned in ascending order. The top 15 words are printed below.
 ```
mohanresults = Word_Count_RDD.map(lambda x: (x[1], x[0])).sortByKey(False).take(15)
print(mohanresults)
```
11. The results will be graphed using the mathplotlib library. We can display any type of graph by plotting the x and y axis.
```
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
```
## Charting Results:

## References:



