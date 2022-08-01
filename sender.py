#!/usr/bin/env python
import pika
import config

def publish_msg(id,status):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=config.credentials['MSG_QUEUE_HOST']))
    channel = connection.channel()

    channel.queue_declare(queue='msg-queue', durable=True)
    msg = "Task info => id: {} status: {}".format(id,status)

    #print(msg)
    channel.basic_publish(
        exchange='',
        routing_key='msg-queue',
        body=msg,
        properties=pika.BasicProperties(
            delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
    ))

    connection.close()

