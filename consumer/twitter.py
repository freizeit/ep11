#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4


"""
Consumes tweets matching a given pattern, prints them to stdout.
"""

from optparse import OptionParser
import random
import textwrap

import kombu

from utils import config


def handle_tweets(body, message):
    print("\n")
    print("=> %s (%s)" % (body["from_user"],
                          message.delivery_info["routing_key"]))
    for line in textwrap.wrap(body["text"], 70):
        print("   %s" % line)
    message.ack()


def main(options):
    connection = kombu.BrokerConnection(**config.get_rabbitmq_config())
    channel = connection.channel()
    # By default messages sent to exchanges are persistent (delivery_mode=2),
    # and queues and exchanges are durable.
    try:
        twitter_exchange = kombu.Exchange("twitter", type="topic")
        queue = kombu.Queue(name=str(random.random()),
                            exchange=twitter_exchange,
                            routing_key=options.pattern, durable=False,
                            auto_delete=True)
        consumer = kombu.Consumer(channel, queue, callbacks=[handle_tweets])
        consumer.consume()
        while True:
            connection.drain_events()
    finally:
        channel.close()
        connection.close()


if __name__ == '__main__':
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-p", "--pattern", dest="pattern",
                      help="the topic pattern to look for")
    (options, _) = parser.parse_args()
    main(options)
