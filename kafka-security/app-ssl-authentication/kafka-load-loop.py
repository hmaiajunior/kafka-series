import threading
import time
import random
import string
from confluent_kafka import Producer
import json
import sys

# Configurações de conexão com o Kafka via SSL/TLS com mTLS
producer_config = {
    'bootstrap.servers': 'localhost:9091,localhost:9092,localhost:9093',
    'security.protocol': 'SSL',
    'ssl.ca.location': '/home/henrique/Documentos/treinamentos/kafka-series/kafka-security/app-cert-ssl/ca-cert',
    'ssl.certificate.location': '/home/henrique/Documentos/treinamentos/kafka-series/kafka-security/app-cert-ssl/client-cert.pem',
    'ssl.key.location': '/home/henrique/Documentos/treinamentos/kafka-series/kafka-security/app-cert-ssl/client-key.pem',
    'linger.ms': 5,
    'acks': '0',
}

KAFKA_TOPIC = 'topic05'
MESSAGES_PER_BATCH = 10
NUM_THREADS = 5

def delivery_report(err, msg):
    if err is not None:
        print(f'❌ Falha ao enviar mensagem: {err}')

def generate_random_string(length=50):
    letters = string.ascii_letters + string.digits
    return ''.join(random.choice(letters) for _ in range(length))

def produce_loop(producer_id):
    p = Producer(producer_config)
    counter = 0

    try:
        while True:
            for i in range(MESSAGES_PER_BATCH):
                payload = generate_random_string()
                p.produce(
                    topic=KAFKA_TOPIC,
                    key=f'thread-{producer_id}-{counter}',
                    value=payload,
                    callback=delivery_report
                )
                p.poll(0)
                counter += 1

            p.flush()
            print(f'🧵 Thread {producer_id}: {MESSAGES_PER_BATCH} mensagens enviadas. Total: {counter}')
            time.sleep(2)

    except KeyboardInterrupt:
        print(f'\n🛑 Thread {producer_id} interrompida pelo usuário. Total enviado: {counter}')
        sys.exit(0)

def main():
    threads = []

    print(f'🚀 Iniciando envio contínuo com {NUM_THREADS} thread(s)... Pressione Ctrl+C para parar.\n')

    for i in range(NUM_THREADS):
        t = threading.Thread(target=produce_loop, args=(i,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

if __name__ == '__main__':
    main()
