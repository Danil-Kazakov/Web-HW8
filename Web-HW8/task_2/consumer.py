import pika
import json
import time
from mongoengine import connect
from models import Contact

connect('contacts', host='localhost', port=27017)

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='email_queue')

def send_email(contact_id):
    print(f"Sent email to contact {contact_id}")

def callback(ch, method, properties, body):
    data = json.loads(body)
    contact_id = data['contact_id']
    contact = Contact.objects(id=contact_id).first()
    if contact:
        send_email(contact_id)
        contact.email_sent = True
        contact.save()
    else:
        print(f"Contact with ID {contact_id} not found")

channel.basic_consume(queue='email_queue', on_message_callback=callback, auto_ack=True)

print('Waiting for messages. To exit press CTRL+C')
channel.start_consuming()