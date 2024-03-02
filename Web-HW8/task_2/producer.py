import pika
import json
import uuid
from faker import Faker
from mongoengine import connect
from models import Contact

connect('contacts', host='localhost', port=27017)

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='email_queue')

fake = Faker()

def generate_contacts(num_contacts):
    contacts = []
    for _ in range(num_contacts):
        name = fake.name()
        email = fake.email()
        contact = Contact(full_name=name, email=email)
        contact.save()
        contacts.append(contact)
    return contacts

def send_message_to_queue(contact_id):
    channel.basic_publish(exchange='',
                          routing_key='email_queue',
                          body=json.dumps({'contact_id': str(contact_id)}))
    print(f"Sent message for contact {contact_id}")

if __name__ == '__main__':
    num_contacts = 10
    contacts = generate_contacts(num_contacts)
    for contact in contacts:
        send_message_to_queue(contact.id)

connection.close()