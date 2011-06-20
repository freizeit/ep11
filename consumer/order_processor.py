#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4


"""
Simple order processor server, adapted from
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

    # Preprocessing exchange + queues for non-validated orders.
    channel.exchange_declare(exchange="incoming", durable=False, type="direct",
                             auto_delete=True)
    channel.queue_declare(queue="incoming", durable=False, auto_delete=True,
                          callback=on_preproc_queue_declared)
    channel.queue_declare(queue="decrypted", durable=False, auto_delete=True,
                          callback=on_preproc_queue_declared)
    # Fanout exchange + queues for validated orders.
    channel.exchange_declare(exchange="orders", durable=False, type="fanout",
                             auto_delete=True)
    channel.queue_declare(queue="validated", durable=False,
                          auto_delete=True, callback=on_orders_queue_declared)


def on_preproc_queue_declared(frame):
    """Called when a queue has been declared on the `incoming` exchange."""
    callbacks = dict(incoming=decrypt, decrypted=authenticate)
    queue_name  = frame.method.queue
    assert queue_name in callbacks, "Unknown queue: %s" % queue_name

    channel.basic_consume(callbacks[queue_name], queue=queue_name)
    channel.queue_bind(exchange="incoming", queue=queue_name,
                       routing_key="rk-%s" % queue_name)


def on_orders_queue_declared(frame):
    """Called when a queue has been declared on the `orders` exchange."""
    callbacks = dict(validated=process)
    queue_name  = frame.method.queue
    assert queue_name in callbacks, "Unknown queue: %s" % queue_name

    channel.basic_consume(callbacks[queue_name], queue=queue_name)
    channel.queue_bind(exchange="orders", queue=queue_name)


def decrypt(channel, method, header, body):
    """Called when we receive an encrypted order."""
    print "\n* Encrypted order: %s" % body
    body = body.decode("rot13")
    if body == "quit":
        connection.close()
        connection.ioloop.start()
        return

    channel.basic_ack(delivery_tag=method.delivery_tag)
    order = simplejson.loads(body)
    assert isinstance(order, list) and len(order) == 2, \
        "Malformed order %s" % order
    channel.basic_publish(exchange="incoming", routing_key="rk-decrypted",
        body=body, properties=pika.BasicProperties(delivery_mode=1))
    print "> Decrypted: %s" % body


def authenticate(channel, method, header, body):
    """Called with a decrypted order."""
    print "* Decrypted: %s" % body
    channel.basic_ack(delivery_tag=method.delivery_tag)

    customer, order = simplejson.loads(body)
    if customer != "al_maisan":
        print "! Unknown customer: %s" % customer
    else:
        channel.basic_publish(exchange="orders", routing_key="validated",
                              body=order)
        print "> Authenticated: %s" % order


def process(channel, method, header, body):
    """Called with a validated order."""
    print "* Validated: %s" % body
    channel.basic_ack(delivery_tag=method.delivery_tag)
    print "> Processed: %s" % body


connection = pika.adapters.SelectConnection(config.pika_params(), on_connected)


try:
    # Loop so we can communicate with RabbitMQ
    connection.ioloop.start()
finally:
    # Gracefully close the connection
    connection.close()
    # Loop until we're fully closed, will stop on its own
    connection.ioloop.start()
