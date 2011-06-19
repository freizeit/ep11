#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4


"""
Simple RPC server, adapted from
    http://pika.github.com/connecting.html#continuation-passing-style
"""


import simplejson

import pika

from utils import config


channel = None


def on_connected(connection):
    """Called when we are fully connected to RabbitMQ"""
    # Open a channel
    connection.channel(on_channel_open)


def on_channel_open(new_channel):
    """Called when our channel has opened"""
    global channel
    channel = new_channel

    channel.exchange_declare(exchange="rpc", durable=False, auto_delete=True)
    channel.queue_declare(queue="jobs", durable=False, auto_delete=True,
                          callback=on_job_queue_declared)


def on_job_queue_declared(frame):
    """Called when the RPC job queue has been declared."""
    channel.basic_consume(handle_job, queue="jobs")
    channel.queue_bind(exchange="rpc", queue="jobs", routing_key="jobs")


def handle_job(channel, method, header, body):
    """Called when we receive an RPC job message."""
    print "> RPC job: %s" % body
    channel.basic_ack(delivery_tag=method.delivery_tag)

    ints = simplejson.loads(body)
    channel.basic_publish(exchange="rpc", routing_key=header.reply_to,
        body=simplejson.dumps(sum(ints)),
        properties=pika.BasicProperties(delivery_mode=1))


connection = pika.adapters.SelectConnection(config.pika_params(), on_connected)


try:
    # Loop so we can communicate with RabbitMQ
    connection.ioloop.start()
finally:
    # Gracefully close the connection
    connection.close()
    # Loop until we're fully closed, will stop on its own
    connection.ioloop.start()
