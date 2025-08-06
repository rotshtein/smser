from ensurepip import version
import threading
import time
from typing import Final
import logging
import json
import click
from MQTTPublisher import MQTTPublisher
from RedisClient import RedisClient
from SMSReader import SMSReader, CheckModemPort

VERSION: Final[str] = "1.0.0"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
SERIAL_PORT: Final[str] = ''#'/dev/ttyUSB3'  # update as needed
BAUDRATE: Final[int] = 115200
REDIS_HOST: Final[str] = 'localhost'
REDIS_PORT: Final[int] = 6379
REDIS_QUEUE: Final[str] = 'sms_queue'
MQTT_BROKER: Final[str] = 'localhost'
MQTT_PORT: Final[int] = 1883
MQTT_TOPIC: Final[str] = 'modem/sms'


def read_sms_thread(sms_reader: SMSReader, redis_client:RedisClient, queue_name: str) -> None:
    """
    Continuously reads SMS messages from the modem using sms_reader,
    pushes them to the specified Redis queue using redis_client,
    and deletes them from the modem after successful enqueue.
    Runs as a background thread.
    :param sms_reader: SMSReader instance for reading SMS
    :param redis_client: RedisClient instance for Redis operations
    :param queue_name: Name of the Redis queue to push messages to
    """
    logger.info("SMS reading thread started")
    
    while True:
        try:
            messages = sms_reader.read_sms()
            
            if messages:
                # Batch operations for better performance
                pipeline = redis_client.pipeline()
                for msg in messages:
                    pipeline.rpush(queue_name, json.dumps(msg))
                pipeline.execute()
                
                # Delete messages after successful enqueue
                for msg in messages:
                    sms_reader.delete_sms(msg['index'])
                    
                logger.info(f"Processed {len(messages)} new SMS messages")
            
            time.sleep(2)  # Reduced polling frequency
            
        except Exception as e:
            logger.error(f"Error in SMS reading thread: {e}")
            time.sleep(5)


def mqtt_publish_thread(mqtt_publisher: MQTTPublisher, redis_client: RedisClient, queue_name: str) -> None:
    """
    Continuously pops messages from the specified Redis queue and publishes
    them to the MQTT broker using mqtt_publisher. Runs as a background thread.
    :param mqtt_publisher: MQTTPublisher instance for publishing to MQTT
    :param redis_client: RedisClient instance for Redis operations
    :param queue_name: Name of the Redis queue to consume messages from
    """
    logger.info("MQTT publishing thread started")
    
    while True:
        try:
            # Block until a message is available
            item = redis_client.blpop(queue_name,0)
            if item:
                _, payload = item
                message = json.loads(payload)
                mqtt_publisher.publish(message)
                
        except Exception as e:
            logger.error(f"Error in MQTT publishing thread: {e}")
            import time
            time.sleep(5)


@click.command()
@click.option('-l', '--log-level', default='WARNING', help='Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
@click.option('-p','--serial-port', default=SERIAL_PORT, show_default=True, help='Serial port for the modem')
@click.option('-b','--baudrate', default=BAUDRATE, show_default=True, type=int, help='Baudrate for the serial port')
@click.option('-r','--redis-host', default=REDIS_HOST, show_default=True, help='Redis server host')
@click.option('-v', '--version', is_flag=True, help='Show the version and exit')
@click.option('-rp','--redis-port', default=REDIS_PORT, show_default=True, type=int, help='Redis server port')
@click.option('-rq','--redis-queue', default=REDIS_QUEUE, show_default=True, help='Redis queue name for SMS')
@click.option('-mq','--mqtt-broker', default=MQTT_BROKER, show_default=True, help='MQTT broker address')
@click.option('-mp','--mqtt-port', default=MQTT_PORT, show_default=True, type=int, help='MQTT broker port')
@click.option('-mt','--mqtt-topic', default=MQTT_TOPIC, show_default=True, help='MQTT topic for SMS messages')
def main(log_level, serial_port, baudrate, redis_host, redis_port, redis_queue, mqtt_broker, mqtt_port, mqtt_topic, version):
    """
    Main entry point for the SMS-to-MQTT service. Initializes the SMS reader,
    MQTT publisher, and Redis client, then starts background threads for reading
    SMS and publishing to MQTT. Handles graceful shutdown and resource cleanup.
    :param log_level: Logging level as a string (e.g., 'INFO')
    :param serial_port: Serial port for the modem
    :param baudrate: Baudrate for the serial port
    :param redis_host: Redis server host
    :param redis_port: Redis server port
    :param redis_queue: Redis queue name for SMS
    :param mqtt_broker: MQTT broker address
    :param mqtt_port: MQTT broker port
    :param mqtt_topic: MQTT topic for SMS messages
    """
    # Set up logging level
    if version is True:
        click.echo(f"SMS-to-MQTT Service Version: {VERSION}")
        return
    
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    logging.getLogger().setLevel(numeric_level)

    sms_reader = None
    mqtt_publisher = None
    try:
        if serial_port is None or serial_port == '':
            try:
                serial_port = CheckModemPort().find_port()
                logger.info(f"Using serial port: {serial_port}")
            except Exception as e:
                logger.error(f"Failed to find modem port: {e}")
                return

        # Initialize components
        sms_reader = SMSReader(serial_port, baudrate)
        mqtt_publisher = MQTTPublisher(mqtt_broker, mqtt_port, mqtt_topic)
        
        # Start threads
        t1 = threading.Thread(
            target=read_sms_thread, 
            args=(sms_reader, 
                  RedisClient(host=redis_host, port=redis_port), 
                  redis_queue), 
            daemon=True
        )
        
        t2 = threading.Thread(
            target=mqtt_publish_thread, 
            args=(mqtt_publisher, 
                  RedisClient(host=redis_host, port=redis_port),
                  redis_queue, ), 
            daemon=True
        )
        
        t1.start()
        t2.start()
        
        logger.info("SMS-to-MQTT service started successfully")
        
        # Keep main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Cleanup resources
        if sms_reader:
            sms_reader.close()
        if mqtt_publisher:
            mqtt_publisher.close()
        logger.info("Service stopped")


if __name__ == '__main__':
    # import serial.tools.list_ports

    # ports = serial.tools.list_ports.comports()
    # for port in ports:
    #     print(f"{port.device} - {port.description}")
    #test()
    main()
