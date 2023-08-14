from confluent_kafka import Producer
import time
import logging
import json 

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

####################
p = Producer({'bootstrap.servers': 'localhost:9092'})
print('Kafka Producer has been initiated...')
#####################
def receipt(err, msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)
        
#####################
def main():
    while True:  # Infinite loop to keep sending messages
        user_input = input("Enter a message: ")  # Get user input
        data = {'message': user_input}
        m = json.dumps(data)
        p.poll(1)
        p.produce('user-tracker', m.encode('utf-8'), callback=receipt)
        p.flush()
        time.sleep(3)
        
if __name__ == '__main__':
    main()
