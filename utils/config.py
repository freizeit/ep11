# -*- coding: utf-8 -*-
# vim: tabstop=4 shiftwidth=4 softtabstop=4


"""
Various utility functions concerned with configuration.
"""


def get_rabbitmq_config():
    return dict(
        hostname=os.environ.get("EP11_RABBITMQ_HOST"),
        userid=os.environ.get("EP11_RABBITMQ_USERID"),
        password=os.environ.get("EP11_RABBITMQ_PASSWORD"),
        virtual_host=os.environ.get("EP11_RABBITMQ_VIRTUAL_HOST"))


import os
def get_twitter_config():
    return dict(
        twitter_token=os.environ.get("EP11_CONSUMER_KEY"),
        twitter_secret=os.environ.get("EP11_CONSUMER_SECRET"),
        oauth_token=os.environ.get("EP11_ACCESS_TOKEN_KEY"),
        oauth_token_secret=os.environ.get("EP11_ACCESS_TOKEN_SECRET"))


import pika
def pika_params():
    user = os.environ.get("EP11_RABBITMQ_USERID", "guest")
    passwd = os.environ.get("EP11_RABBITMQ_PASSWORD", "guest")
    credentials = pika.PlainCredentials(user, passwd)
    return pika.ConnectionParameters(credentials=credentials)
