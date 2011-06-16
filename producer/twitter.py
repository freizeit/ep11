# -*- coding: utf-8 -*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4


"""
Read tweets, pump them into the "tweets" topic exchange
"""


from collections import defaultdict
import os
import time

import kombu
import twython


def process_tweets(tweets, producer):
    for tweet in tweets["results"]:
        from_user = tweet["from_user"]
        to_user = tweet["to_user"]
        iso_language_code = tweet["iso_language_code"]
        text = tweet["text"]
        created_at = tweet["created_at"]
        if to_user:
            key = "msg.%s.%s.%s" % (iso_language_code, from_user, to_user)
        else:
            key = "twt.%s.%s" % (iso_language_code, from_user)

        producer.publish(dict(text=text, created_at=created_at),
                         routing_key=key)


def pump_tweets(twitter, producer, control_queue):
    """Read tweets and publish them using the given `producer`."""
    question = None
    since_ids = defaultdict(int)
    while True:
        if question:
            tweets = twitter.searchTwitter(q=question,
                                           since_id=since_ids[question])
            since_ids[question] = tweets["max_id_str"]
            process_tweets(tweets, producer)
            time.sleep(5)
        if control_queue.qsize():
            message = control_queue.get(block=False)
            if message:
                message.ack()
                if message.payload == "quit":
                    break
                else:
                    question = message.payload


def get_twitter_config():
    return dict(
        twitter_token=os.environ.get("CHR_CONSUMER_KEY"),
        twitter_secret=os.environ.get("CHR_CONSUMER_SECRET"),
        oauth_token=os.environ.get("CHR_ACCESS_TOKEN_KEY"),
        oauth_token_secret=os.environ.get("CHR_ACCESS_TOKEN_SECRET"))


def main(config):
    connection = kombu.BrokerConnection(**config)
    channel = connection.channel()
    # By default messages sent to exchanges are persistent (delivery_mode=2),
    # and queues and exchanges are durable.
    twitter_exchange = kombu.Exchange("twitter", type="topic")
    producer = kombu.Producer(channel, twitter_exchange)
    control_queue = connection.SimpleQueue("twitter_pump")
    try:
        twitter = twython.Twython(**get_twitter_config())
        pump_tweets(twitter, producer, control_queue)
    finally:
        channel.close()
        connection.close()


if __name__ == '__main__':
    config = dict(hostname="localhost", userid="guest", password="guest",
                  virtual_host="/")
    main(config)
