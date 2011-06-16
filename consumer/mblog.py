# -*- coding: utf-8 -*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4


"""
Read tweets, pump them into the "tweets" topic exchange
"""

import pprint

import kombu

from utils import config


def handle_tweets(body, message):
    pprint.pprint(body)
    pprint.pprint(message)
    message.ack()


def main():
    connection = kombu.BrokerConnection(**config.get_rabbitmq_config())
    channel = connection.channel()
    # By default messages sent to exchanges are persistent (delivery_mode=2),
    # and queues and exchanges are durable.
    try:
        mblog_exchange = kombu.Exchange("mblog", type="topic")
        queue = kombu.Queue("twitter_posts", mblog_exchange,
                            routing_key="twitter.#")
        consumer = kombu.Consumer(channel, queue, [handle_tweets])
        consumer.consume()
    finally:
        connection.drain_events()
        channel.close()
        connection.close()


if __name__ == '__main__':
    main()
