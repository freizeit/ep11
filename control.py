#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4


"""
Control the various amqp producers and consumers.
"""


from optparse import OptionParser

import kombu

from utils import config


def main(options):
    connection = kombu.BrokerConnection(**config.get_rabbitmq_config())
    channel = connection.channel()
    # By default messages sent to exchanges are persistent,
    # and queues and exchanges are durable.
    control_queue = connection.SimpleQueue(options.queue,
                                           queue_opts=dict(durable=True))
    try:
        while True:
            cmd = raw_input(options.prompt)
            control_queue.put(
                cmd.strip(),
                delivery_mode=kombu.entity.TRANSIENT_DELIVERY_MODE)
    finally:
        channel.close()
        connection.close()


if __name__ == '__main__':
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-p", "--prompt", dest="prompt",
                      help="the prompt to display to user")
    parser.add_option("-q", "--queue", dest="queue",
                      help="the name of the control queue")
    (options, args) = parser.parse_args()
    main(options)
