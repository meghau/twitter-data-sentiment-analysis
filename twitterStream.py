from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt
import re

# AUTHOR: MEGHA UMESHA
# UNITY ID: mumesha

sc=None
pwords=None
nwords=None

def main():
    global sc, pwords, nwords
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
    counts = stream(ssc, pwords, nwords, 100)
    print counts
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    positive_values = []
    negative_values = []
    for i in counts:
        if i:
            positive_values.append(i[0][1])
            negative_values.append(i[1][1])
    x_values = range(1,len(positive_values)+1)
    plt.plot(x_values,positive_values, marker='o', linestyle='-', color='b', label='positive')
    plt.plot(x_values,negative_values, marker='o', linestyle='-', color='g', label='negative')
    plt.xlabel('Time step')
    plt.ylabel('Word count')
    plt.legend()
    plt.show()        


def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    wordlist = sc.textFile(filename).flatMap(lambda x: x.split("\n")).collect()
    return wordlist

def classify_word(word):
    if word in pwords:
        return ("positive", 1)
    elif word in nwords:
        return ("negative", 1)
    else:
        return ("none", 1)

def parse_tweet(tweet):
    return re.split("[^A-Za-z0-9]+", tweet.lower())
    
def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount)  # add the new values with the previous running count to get the new count

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))
    #tweets.pprint()
    
    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    parsed_words = tweets.flatMap(parse_tweet)

    classified_words = parsed_words.map(classify_word)

    filtered_words = classified_words.filter(lambda x: x[0] != "none")

    count = filtered_words.reduceByKey(lambda x, y: x + y)
        
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    count.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))
    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))

    #print counts
    runningCounts = count.updateStateByKey(updateFunction)
    runningCounts.pprint()
    
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
