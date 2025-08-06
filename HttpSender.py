import logging
import requests
from MQTTPublisher import MQTTPublisher

class HttpSmsSender:
    """Send SMS via HTTP POST to a configured endpoint."""
    def __init__(self, url, auth_token=None, timeout=10):
        """
        Initialize the HttpSmsSender instance.
        :param url: The HTTP endpoint URL for sending SMS
        :param auth_token: Optional authentication token for the endpoint
        :param timeout: Timeout for HTTP requests (in seconds)
        """
        self.url = url
        self.auth_token = auth_token
        self.timeout = timeout
        self._logger = logging.getLogger(self.__class__.__name__)
        self.mqtt_publisher = None

    def send_sms(self, recipient, message, **kwargs):
        """
        Send an SMS via HTTP POST to the configured endpoint.
        :param recipient: The recipient phone number
        :param message: The SMS message body
        :param kwargs: Additional fields to include in the POST data
        :return: Response object if successful, None otherwise
        """
        headers = {'Content-Type': 'application/json'}
        if self.auth_token:
            headers['Authorization'] = f'Bearer {self.auth_token}'
        data = {
            'recipient': recipient,
            'message': message
        }
        data.update(kwargs)
        try:
            resp = requests.post(self.url, json=data, headers=headers, timeout=self.timeout)
            resp.raise_for_status()
            self._logger.info(f"SMS sent to {recipient} via HTTP POST.")
            return resp
        except requests.RequestException as e:
            self._logger.error(f"Failed to send SMS to {recipient}: {e}")
            return None

    def setup_mqtt(self, broker_url, broker_port, topic, username=None, password=None):
        """
        Initialize the MQTT publisher for forwarding received SMS.
        :param broker_url: MQTT broker address
        :param broker_port: MQTT broker port
        :param topic: MQTT topic (not used directly in publish)
        :param username: Optional MQTT username
        :param password: Optional MQTT password
        """
        self.mqtt_publisher = MQTTPublisher(broker_url, broker_port, topic, username, password)

    def receive_sms_and_forward(self, sender, message, **kwargs):
        """
        Receive an SMS and forward it to the configured MQTT broker.
        :param sender: The sender phone number
        :param message: The SMS message body
        :param kwargs: Additional fields to include in the MQTT payload
        :return: True if forwarded successfully, False otherwise
        """
        if not self.mqtt_publisher:
            self._logger.error("MQTT publisher not initialized. Call setup_mqtt first.")
            return False
        payload = {
            'sender': sender,
            'message': message
        }
        payload.update(kwargs)
        try:
            self.mqtt_publisher.publish(payload)
            self._logger.info(f"SMS from {sender} forwarded to MQTT.")
            return True
        except Exception as e:
            self._logger.error(f"Failed to forward SMS to MQTT: {e}")
            return False

# Example usage:
# if __name__ == "__main__":
#     logging.basicConfig(level=logging.INFO)
#     sender = HttpSmsSender('https://api.example.com/send_sms', auth_token='your_token')
#     response = sender.send_sms('+1234567890', 'Hello from HTTP!')
#     if response:
#         print('SMS sent successfully:', response.json())
#     else:
#         print('Failed to send SMS') 