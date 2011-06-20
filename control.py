#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4


"""
Control the various amqp producers and consumers.
"""


from optparse import OptionParser
import re
import simplejson

import kombu

from utils import config


def main(options):
    list_of_ints_re = re.compile("^\s*(\d+)(\s*,+\s*\d+)*\s*$")
    delimiter_re = re.compile("\s*,\s*")
    connection = kombu.BrokerConnection(**config.get_rabbitmq_config())
    channel = connection.channel()
    control_queue = connection.SimpleBuffer(options.queue)
    try:
        while True:
            cmd = raw_input(options.prompt)
            cmd = cmd.strip()
            if list_of_ints_re.match(cmd):
                cmd = simplejson.dumps(
                    [int(i) for i in delimiter_re.split(cmd) if i])
            if options.encrypt:
                cmd = cmd.encode("rot13")
            control_queue.put(
                cmd, delivery_mode=kombu.entity.TRANSIENT_DELIVERY_MODE)
            do_quit = (cmd == "quit" and not options.encode or
                       options.encrypt and cmd.decode("rot13") == "quit")
            if do_quit:
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
    parser.add_option("-e", "--encrypt", action="store_true", dest="encrypt",
                      help="Encrypt payloads using rot13")
    (options, args) = parser.parse_args()
    main(options)
