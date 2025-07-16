import threading
import time
from typing import Final
import redis
import serial
import json
import paho.mqtt.client as mqtt

# Configuration
SERIAL_PORT:Final[str] = '/dev/ttyUSB0'  # update as needed\ nBAUDRATE = 115200
REDIS_HOST:Final[str] = 'localhost'
REDIS_PORT:Final[int] = 6379
REDIS_QUEUE:Final[str] = 'sms_queue'
MQTT_BROKER:Final[str] = 'localhost'
MQTT_PORT:Final[int] = 1883
MQTT_TOPIC:Final[str] = 'modem/sms'
BAUDRATE:Final[int] = 115200

# Initialize Redis client
redis_client:redis.Redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

# Initialize MQTT client
mqtt_client:mqtt.Client = mqtt.Client()


def init_modem(ser) -> None:
    """
    Initialize modem in text mode
    """
    ser.write(b'AT+CMGF=1\r')  # set SMS text mode
    time.sleep(0.5)
    ser.flush()


def read_sms_thread() -> None:
    """
    Thread to read SMS from modem and push to Redis queue
    """
    ser = serial.Serial(SERIAL_PORT, BAUDRATE, timeout=5)
    init_modem(ser)
    while True:
        try:
            # list all received SMS
            ser.write(b'AT+CMGL=\"ALL\"\r')
            time.sleep(1)
            lines = ser.read(ser.in_waiting or 1000).decode(errors='ignore').splitlines()
            messages = []
            msg = {}
            for line in lines:
                if line.startswith('+CMGL:'):
                    parts = line.split(',')
                    index = parts[0].split(':')[1].strip()
                    status = parts[1].strip().strip('"')
                    sender = parts[2].strip().strip('"')
                    date = parts[4].strip().strip('"') + ' ' + parts[5].strip().strip('"')
                    msg = {'index': index, 'status': status, 'sender': sender, 'date': date, 'body': ''}
                elif msg is not None and line:
                    # body text
                    msg['body'] += line + '\n'
                if msg and msg.get('body') and (line == '' or line == lines[-1]):
                    messages.append(msg)
                    msg = {}

            # enqueue and delete
            for m in messages:
                payload = json.dumps(m)
                redis_client.rpush(REDIS_QUEUE, payload)
                # delete SMS from modem
                cmd = f'AT+CMGD={m["index"]}\r'.encode()
                ser.write(cmd)
                time.sleep(0.2)

        except Exception as e:
            print(f"Error reading SMS: {e}")

        time.sleep(10)


def mqtt_publish_thread() -> None:
    """
    Thread to pop messages from Redis and publish over MQTT
    """
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
    mqtt_client.loop_start()
    while True:
        try:
            # block until a message is available
            item = redis_client.blpop(REDIS_QUEUE, timeout=5)
            if item:
                _, payload = item
                message = json.loads(payload)
                # publish JSON message
                mqtt_client.publish(MQTT_TOPIC, json.dumps(message))
                print(f"Published SMS from {message['sender']} to MQTT.")
        except Exception as e:
            print(f"Error publishing MQTT: {e}")
            time.sleep(5)


def main() -> None:
    t1 = threading.Thread(target=read_sms_thread, daemon=True)
    t2 = threading.Thread(target=mqtt_publish_thread, daemon=True)

    t1.start()
    t2.start()

    print("SMS-to-MQTT service started.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
        mqtt_client.loop_stop()


if __name__ == '__main__':
    main()
