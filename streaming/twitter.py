import json
import socket
import sys
import requests
import kafkaapi
from requests_oauthlib import OAuth1


TWITTER_API_KEY = '<YOUR_TWITTER_API_KEY_HERE>'
TWITTER_API_SECRET_KEY = '<YOUR_TWITTER_API_SECRET_KEY_HERE>'
TWITTER_ACCESS_TOKEN = 'TWITTER_ACCESS_TOKEN_HERE'
TWITTER_ACCESS_TOKEN_SECRET = 'TWITTER_ACCESS_TOKEN_SECRET_HERE'
TWITTER_OAUTH = OAuth1(TWITTER_API_KEY, TWITTER_API_SECRET_KEY, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)
TWITTER_STREAM_URL = 'https://stream.twitter.com/1.1/statuses/filter.json'
INDIA_LAT_LONG = '68.116667 ,8.066667,97.416667,37.100000'
TCP_IP = 'localhost'
TCP_PORT = 9008


def get_tweets(locations, filter_text):
    """
    This API stream tweets from the twitter stream. Here , we are passing a
    filter for tweet language and a geographical area from which we are
    interested in tweets.

    :param locations:
    :param filter_text:
    :return: response
    """
    query_data = [('language', 'en'), ('locations', locations), ('track', filter_text)]
    query_url = TWITTER_STREAM_URL + '?' + '&'.join([str(data[0]) + '=' + str(data[1]) for data in query_data])
    response = requests.get(query_url, auth=TWITTER_OAUTH, stream=True)
    return response


def send_tweets_to_socket(http_resp, tcp_connection):
    """
    This API will write the tweets to a socket which could be running on your
    local machine or some server.

    :param http_resp:
    :param tcp_connection:
    :return:
    """
    for line in http_resp.iter_lines():
        try:
            tweet = json.loads(line)
            tweet_text = tweet['text']
            tcp_connection.send((tweet_text + '\n').encode())
        except:
            exception = sys.exc_info()[0]
            print("Error: %s" % exception)


def send_tweets_to_kafka(broker_ids, topic_name, partitions, replication):
    """
    This API send tweets to a Kafka topic.


    # Start zookeeper server - bin\windows\zookeeper-server-start.bat config\zookeeper.properties
    # Start kafka broker - bin\windows\kafka-server-start.bat config\server.properties
    # Create topic - bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1
                    --partitions 1 --topic india-tweets
    # Below is the code for kafka producer.

    :param broker_id:
    :param topic_name:
    :param partitions:
    :param replication:
    :return:
    """
    tweets_stream = get_tweets(locations=INDIA_LAT_LONG, filter_text='#')
    kafka_producer = kafkaapi.create_producer(broker_ids)
    for tweet_dump in tweets_stream.iter_lines():
        try:
            tweet = json.loads(tweet_dump)
            tweet_text = tweet['text']
            kafka_producer.send(topic_name, tweet_text.encode())\
                            .add_callback(on_send_success)\
                            .add_errback(on_send_error)
        except:
            exception = sys.exc_info()[0]
            print("Error: %s" % exception)


def create_tweet_file(http_resp):
    """
    This API writes all the tweets to multiple files which is controlled by the number of tweets
    we want to have in a file..


    :param http_resp:
    :return:
    """
    tweet_counter = 0
    counter = 1
    for line in http_resp.iter_lines():
        file_path = '/home/saurabh/data/streaming/tweets-{counter}.txt'.format(counter=counter)
        with open(file_path,'a') as file_writer:
            try:
                tweet = json.loads(line)
                tweet_text = tweet['text']
                file_writer.write(tweet_text + '\n')
                tweet_counter = tweet_counter + 1
                if tweet_counter % 100000 == 0:
                    counter = counter + 1
                    file_writer.close()

            except:
                exception = sys.exc_info()[0]
                print("Error: %s" % exception)


def create_tcp_connection():
    """
    Creates a TCP connection and opens a socket to listen data from
    streaming sources.

    :return:
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(10)
    print("Waiting for TCP connection...")
    connection, address = s.accept()
    print("Connected... Starting getting tweets.")
    return connection


def on_send_success(record_metadata):
    pass
    #print(record_metadata.topic, record_metadata.partition, record_metadata.offset)


def on_send_error(excp):
    print('I am an errback', exc_info=excp)
    # Your error handling logic goes here


if __name__ == '__main__':
    response = get_tweets(locations=INDIA_LAT_LONG, filter_text='#')
    #create_tweet_file(response)
    #send_tweets_to_socket(response, create_tcp_connection())
    send_tweets_to_kafka(['localhost:9092'], 'india-tweet', 1, 1)



