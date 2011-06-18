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
    control_queue = connection.SimpleBuffer(options.queue)
    try:
        while True:
            cmd = raw_input(options.prompt)
            cmd = cmd.strip()
            control_queue.put(
                cmd, delivery_mode=kombu.entity.TRANSIENT_DELIVERY_MODE)
            if cmd == "quit":
                break
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
