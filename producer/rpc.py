#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4


"""
Simple RPC example producer.
"""


import os

import pika


connection = None
channel = None


# Step #2
def on_connected(connection):
    """Called when we are fully connected to RabbitMQ"""
    # Open a channel
    connection.channel(on_channel_open)


# Step #3
def on_channel_open(new_channel):
    """Called when our channel has opened"""
    global channel
    channel = new_channel

    channel.exchange_declare(exchange="rpc_ctl", durable=False,
                             auto_delete=True)
    channel.queue_declare(queue="rpc_ctl", durable=False, auto_delete=True,
                          callback=on_ctl_queue_declared)

    channel.exchange_declare(exchange="rpc", durable=False, auto_delete=True)
    channel.queue_declare(durable=False, auto_delete=True,
                          exclusive=True, callback=on_queue_declared)


# Step #4
def on_ctl_queue_declared(frame):
    """
    Called when RabbitMQ has told us our Queue has been declared, frame is
    the response from RabbitMQ.
    """
    channel.basic_consume(handle_ctl_msg, queue='rpc_ctl')

def on_queue_declared(frame):
    """
    Called when RabbitMQ has told us our Queue has been declared, frame is
    the response from RabbitMQ.
    """
    channel.basic_consume(handle_rpc_msg, queue=frame.method.queue)


# Step #5
def handle_ctl_msg(channel, method, header, body):
    """Called when we receive a message from RabbitMQ"""
    body = body.strip()
    print body
    if body == "quit":
        connection.close()
        connection.ioloop.start()


def handle_rpc_msg(channel, method, header, body):
    """Called when we receive a message from RabbitMQ"""
    print body


# Step #1: Connect to RabbitMQ
user = os.environ.get("EP11_RABBITMQ_USERID", "guest")
passwd = os.environ.get("EP11_RABBITMQ_PASSWORD", "guest")
credentials = pika.PlainCredentials(user, passwd)
parameters = pika.ConnectionParameters(credentials=credentials)
connection = pika.adapters.SelectConnection(parameters, on_connected)


try:
    # Loop so we can communicate with RabbitMQ
    connection.ioloop.start()
finally:
    # Gracefully close the connection
    connection.close()
    # Loop until we're fully closed, will stop on its own
    connection.ioloop.start()
