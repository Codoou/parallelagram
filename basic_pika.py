import pika

credentials = pika.PlainCredentials('','')
connection = pika.BlockingConnection(pika.ConnectionParameters('','', '/', credentials))
channel = connection.channel()

channel.queue_declare(queue='parallelagram')

channel.basic_publish(exchange="", routing_key="parallelagram", body="Producing a message!")