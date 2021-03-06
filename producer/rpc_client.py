#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4


"""
Simple RPC client, adapted from
    http://pika.github.com/connecting.html#continuation-passing-style
"""


import pika

from utils import config


def on_connected(connection):
    """Called when we are fully connected to RabbitMQ"""
    config.set("connection", connection)
    # Open a channel
    connection.channel(on_channel_open)


def on_channel_open(channel):
    """Called when our channel has opened"""
    config.set("channel", channel)

    channel.exchange_declare(exchange="rpc_ctl", durable=False,
                             auto_delete=True)
    channel.queue_declare(queue="rpc_ctl", durable=False, auto_delete=True,
                          callback=on_ctl_queue_declared)

    channel.exchange_declare(exchange="rpc", durable=False, auto_delete=True)
    channel.queue_declare(queue="jobs", durable=False, auto_delete=True)
    channel.queue_declare(durable=False, auto_delete=True,
                          exclusive=True, callback=on_result_queue_declared)


def on_ctl_queue_declared(_):
    """Called when the control queue has been declared."""
    channel = config.get("channel")
    channel.basic_consume(handle_ctl_msg, queue='rpc_ctl')


def on_result_queue_declared(frame):
    """
    Called when the RPC results queue has been declared, the generated
    name is in the frame (response from RabbitMQ).
    """
    result_queue = frame.method.queue
    config.set("result_queue", result_queue)
    channel = config.get("channel")
    channel.basic_consume(handle_result, queue=result_queue)
    channel.queue_bind(exchange="rpc", queue=result_queue,
                       routing_key=result_queue)


def handle_ctl_msg(channel, method, header, body):
    """Called when we receive a control message from the shell."""
    body = body.strip()
    print "* Rcvd ctrl msg: %s" % body
    channel.basic_ack(delivery_tag=method.delivery_tag)

    connection = config.get("connection")
    result_queue = config.get("result_queue")

    if body == "quit":
        channel.basic_publish(exchange="rpc", routing_key="jobs", body=body,
            properties=pika.BasicProperties(delivery_mode=1))
        connection.close()
        connection.ioloop.start()
    else:
        channel.basic_publish(exchange="rpc", routing_key="jobs", body=body,
            properties=pika.BasicProperties(
                delivery_mode=1, reply_to=result_queue))


def handle_result(channel, method, header, body):
    """Called when we receive an RPC result message."""
    print "> RPC result: %s" % body
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
