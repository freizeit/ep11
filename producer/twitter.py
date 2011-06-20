#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4


"""
Read tweets, pump them into the "tweets" topic exchange
"""


from collections import defaultdict
import time

import kombu
import twython

from utils import config


def pump_tweets(tweets, producer):
    for tweet in tweets["results"]:
        from_user = tweet.get("from_user")
        to_user = tweet.get("to_user")
        iso_language_code = tweet.get("iso_language_code")
        text = tweet.get("text")
        created_at = tweet.get("created_at")
        if to_user:
            key = "msg.%s.%s.%s" % (iso_language_code, from_user,
                                            to_user)
        else:
            key = "twt.%s.%s" % (iso_language_code, from_user)

        producer.publish(dict(created_at=created_at, from_user=from_user,
                              iso_language_code=iso_language_code, text=text,
                              to_user=to_user, source="twitter"),
                         routing_key=key)


def handle_tweets(twitter, producer, control_queue):
    """Read tweets and publish them using the given `producer`."""
    question = None
    since_ids = defaultdict(int)
    while True:
        if question:
            tweets = twitter.searchTwitter(q=question,
                                           since_id=str(since_ids[question]))
            since_ids[question] = tweets["max_id"]
            print(".%s=%s." % (question, len(tweets["results"])))
            pump_tweets(tweets, producer)
            time.sleep(1)
        if control_queue.qsize():
            message = control_queue.get(block=False)
            if message:
                if message.payload == "quit":
                    break
                else:
                    question = message.payload


def main():
    connection = kombu.BrokerConnection(**config.get_rabbitmq_config())
    channel = connection.channel()
    twitter_exchange = kombu.Exchange("twitter", type="topic")
    producer = kombu.Producer(channel, twitter_exchange)
    control_queue = connection.SimpleBuffer("twitter_pump")
    try:
        twitter = twython.Twython(**config.get_twitter_config())
        handle_tweets(twitter, producer, control_queue)
    finally:
        channel.close()
        connection.close()


if __name__ == '__main__':
    main()
