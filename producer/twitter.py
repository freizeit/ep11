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
        from_user = tweet["from_user"]
        to_user = tweet["to_user"]
        iso_language_code = tweet["iso_language_code"]
        text = tweet["text"]
        created_at = tweet["created_at"]
        if to_user:
            key = "twitter.msg.%s.%s.%s" % (iso_language_code, from_user,
                                            to_user)
        else:
            key = "twitter.twt.%s.%s" % (iso_language_code, from_user)

        producer.publish(dict(created_at=created_at, from_user=from_user,
                              iso_language_code=iso_language_code, text=text,
                              source="twitter"),
                         routing_key=key)


def handle_tweets(twitter, producer, control_queue):
    """Read tweets and publish them using the given `producer`."""
    question = None
    since_ids = defaultdict(int)
    while True:
        if question:
            tweets = twitter.searchTwitter(q=question,
                                           since_id=since_ids[question])
            since_ids[question] = tweets["max_id_str"]
            pump_tweets(tweets, producer)
            time.sleep(5)
        if control_queue.qsize():
            message = control_queue.get(block=False)
            if message:
                message.ack()
                if message.payload == "quit":
                    break
                else:
                    question = message.payload


def main():
    connection = kombu.BrokerConnection(**config.get_rabbitmq_config())
    channel = connection.channel()
    # By default messages sent to exchanges are persistent (delivery_mode=2),
    # and queues and exchanges are durable.
    mblog_exchange = kombu.Exchange("mblog", type="topic")
    producer = kombu.Producer(channel, mblog_exchange)
    control_queue = connection.SimpleQueue("twitter_pump")
    try:
        twitter = twython.Twython(**config.get_twitter_config())
        handle_tweets(twitter, producer, control_queue)
    finally:
        channel.close()
        connection.close()


if __name__ == '__main__':
    main()