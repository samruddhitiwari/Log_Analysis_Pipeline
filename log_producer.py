from kafka import KafkaProducer
import time, random

producer = KafkaProducer(bootstrap_servers='localhost:9092')

ips = ['192.168.1.1', '10.0.0.2', '172.16.0.3']
status_codes = [200, 401, 404, 500]

while True:
    ip = random.choice(ips)
    status = random.choice(status_codes)
    log = f"{ip} - - [01/Aug/2025:12:34:56 +0000] \"GET /index.html HTTP/1.1\" {status}"
    producer.send('logs', value=log.encode('utf-8'))
    time.sleep(0.5)
