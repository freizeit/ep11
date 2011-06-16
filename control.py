# -*- coding: utf-8 -*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4


"""
Control the twitter pump.
"""


import kombu


def main(config):
    connection = kombu.BrokerConnection(**config)
    channel = connection.channel()
    # By default messages sent to exchanges are persistent (delivery_mode=2),
    # and queues and exchanges are durable.
    control_queue = connection.SimpleQueue("twitter_pump")
    try:
        while True:
            cmd = raw_input("quit|question for the twitter pump: ")
            control_queue.put(cmd.strip())
    finally:
        channel.close()
        connection.close()


if __name__ == '__main__':
    config = dict(hostname="localhost", userid="guest", password="guest",
                  virtual_host="/")
    main(config)
