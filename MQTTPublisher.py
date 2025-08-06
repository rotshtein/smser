import time
import json
import paho.mqtt.client as mqtt
import logging
from typing import Dict

logger = logging.getLogger(__name__)

class MQTTPublisher:
    """Efficient MQTT publisher with connection management"""
    
    def __init__(self, broker: str, port: int, topic: str):
        """
        Initialize the MQTTPublisher instance and connect to the MQTT broker.
        :param broker: MQTT broker address
        :param port: MQTT broker port
        :param topic: Default MQTT topic (not used in publish, kept for compatibility)
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._logger.debug('MQTTPublisher initialize')
        self._broker = broker
        self._port = port
        self._client: mqtt.Client = mqtt.Client(protocol=mqtt.MQTTv311)
        self._client.on_disconnect = self.on_disconnect
        self._connect()
    
    def _connect(self):
        """
        Connect to the MQTT broker and start the network loop.
        Raises an exception if connection fails.
        """
        try:
            self._client.connect(self._broker, self._port)
            self._client.loop_start()
            logger.info("Connected to MQTT broker")
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            raise
        
    def on_disconnect(self, client, userdata, rc):
        """
        Callback for MQTT client disconnect event. Attempts to reconnect until successful.
        :param client: The MQTT client instance
        :param userdata: The private user data
        :param rc: The disconnection result code
        """
        self._logger.warning("Disconnected from MQTT broker. Attempting to reconnect...")
        while True:
            try:
                client.reconnect()
                self._logger.info("Reconnected to MQTT broker")
                break
            except Exception as e:
                self._logger.error(f"Reconnect failed: {e}")
                time.sleep(5)  # Wait before retrying
                
    def publish(self, message: Dict) -> bool:
        """
        Publish an SMS message to the MQTT broker.
        The topic is constructed as modem/{sender}/{timestamp}.
        :param message: Dictionary containing SMS data (expects 'body', 'sender', 'timestamp')
        :return: True if published successfully, False otherwise
        """
        try:
            payload = json.dumps(message['body'])
            message['sender'] = ''.join(filter(str.isdigit, message['sender']))
            result = self._client.publish(f"modem/{message['sender']}/{message['timestamp']}", payload)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                logger.info(f"Published SMS from {message.get('sender', 'unknown')} to MQTT")
                return True
            else:
                logger.error(f"Failed to publish to MQTT: {result.rc}")
                return False
        except Exception as e:
            logger.error(f"Error publishing to MQTT: {e}")
            return False
    
    def close(self):
        """
        Close the MQTT connection and stop the network loop.
        """
        self._client.loop_stop()
        self._client.disconnect()