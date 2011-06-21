#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4


"""
Simple order processor server, adapted from
    http://pika.github.com/connecting.html#continuation-passing-style
"""


import pika
from utils import config


def on_connected(connection):
    """Called when we are fully connected to RabbitMQ"""
    # Open a channel
    connection.channel(on_channel_open)


def on_channel_open(channel):
    """Called when our channel has opened"""
    config.set("channel", channel)

    # Fanout exchange + queues for validated orders.
    channel.exchange_declare(exchange="orders", durable=False, type="fanout",
                             auto_delete=True)
    channel.queue_declare(queue="validated", durable=False,
                          auto_delete=True, callback=on_orders_queue_declared)


def on_orders_queue_declared(frame):
    """Called when a queue has been declared on the `orders` exchange."""
    channel = config.get("channel")
    callbacks = dict(validated=process)
    queue_name  = frame.method.queue
    assert queue_name in callbacks, "Unknown queue: %s" % queue_name

    channel.basic_consume(callbacks[queue_name], queue=queue_name)
    channel.queue_bind(exchange="orders", queue=queue_name)


def process(channel, method, header, body):
    """Called with a validated order."""
    print "|R| orders.validated: %s" % body
    channel.basic_ack(delivery_tag=method.delivery_tag)


connection = pika.adapters.SelectConnection(config.pika_params(), on_connected)


try:
    # Loop so we can communicate with RabbitMQ
    connection.ioloop.start()
finally:
    # Gracefully close the connection
    connection.close()
    # Loop until we're fully closed, will stop on its own
    connection.ioloop.start()
