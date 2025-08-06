import time
import serial
import serial.tools.list_ports
import logging
from datetime import datetime
from typing import Generator, Optional, Dict, List
from contextlib import contextmanager

class CheckModemPort:
    def __init__(self):
        self.at_port: str = None
        self._logger = logging.getLogger(self.__class__.__name__)

    def find_port(self) -> str:
        # List all available serial ports
        ports = serial.tools.list_ports.comports()
        self._logger.debug("Scanning serial ports...\n")
        for port in ports:
            self._logger.debug(f"Checking {port.device} ({port.description})...")
            success = self.check_at_command(port.device)
            if success:
                self._logger.info(f"✅ Found AT-compatible modem on {port.device}")
                self.at_port = port.device
                break
            else:
                self._logger.debug("❌ No response or unexpected reply")

        else:
            self._logger.warning("\n❗ No modem responded to AT command on available ports.")
        return self.at_port
    
    def check_at_command(self, port, baudrate=115200) -> bool:
        try:
            ser = serial.Serial(port=port, baudrate=baudrate, timeout=1)
            ser.write(b'AT\r')
            time.sleep(0.5)
            response = ser.read_all().decode(errors='ignore').strip()
            ser.close()

            if "OK" in response:
                return True
            else:
                return False
        except serial.SerialException:
            return False


class SMSReader:
    """Efficient SMS reader with connection management"""

    def __init__(self, port: str, baudrate: int):
        """
        Initialize the SMSReader instance and set up serial connection parameters.
        :param port: Serial port for the modem
        :param baudrate: Baudrate for the serial connection
        """
        self._port = port
        self._baudrate = baudrate
        self._serial_conn: Optional[serial.Serial] = None
        self._logger = logging.getLogger(self.__class__.__name__)
        
    @contextmanager
    def get_connection(self) -> Generator[serial.Serial, None, None]:
        """
        Context manager for serial connection. Opens the connection if not already open,
        initializes the modem, and yields the serial object. Handles errors and cleanup.
        :yield: Open serial.Serial connection
        """
        if self._serial_conn is None or not self._serial_conn.is_open:
            self._serial_conn = serial.Serial(self._port, self._baudrate, timeout=5)
            self._init_modem()
        try:
            yield self._serial_conn
        except Exception as e:
            self._logger.error(f"Serial connection error: {e}")
            if self._serial_conn and self._serial_conn.is_open:
                self._serial_conn.close()
            self._serial_conn = None
            raise Exception(f"Serial connection error: {e}")
    
    def _init_modem(self) -> None:
        """
        Initialize the modem in text mode (AT+CMGF=1).
        """
        self._serial_conn.write(b'AT+CMGF=1\r')
        time.sleep(0.5)
        ret = self._serial_conn.read(self._serial_conn.in_waiting or 100).decode(errors='ignore')
        self._serial_conn.flush()
        self._logger.info(f"Modem initialized [{ret}]")
    
    def _send_command(self, command: str) -> str:
        """
        Send an AT command to the modem and return the response as a string.
        :param command: The AT command to send
        :return: Response from the modem
        """
        self._logger.debug(f"Sending command: {command}")
        cmd_bytes = f'{command}\r'.encode()
        with self.get_connection() as ser:
            ser.flush()
            ser.write(cmd_bytes)
            time.sleep(0.5)  # Reduced wait time
            return ser.read(ser.in_waiting or 1024).decode(errors='ignore')

    def _wait_for_ok(self, timeout: float = 1.0) -> bool:
        """
        Wait for an 'OK' response from the modem within the specified timeout.
        :param timeout: Timeout in seconds
        :return: True if 'OK' received, False otherwise
        """
        end_time = time.time() + timeout
        buffer = ""
        with self.get_connection() as ser:
            while time.time() < end_time:
                if ser.in_waiting:
                    buffer += ser.read(ser.in_waiting).decode(errors='ignore')
                    if 'OK' in buffer:
                        return True
                    if 'ERROR' in buffer:
                        self._logger.error("Received ERROR from modem")
                        return False
                time.sleep(0.1)
        self._logger.warning("Timeout waiting for OK from modem")
        return False
    
    def _parse_sms_message(self, lines: List[str]) -> List[Dict]:
        """
        Efficiently parse SMS messages from a list of modem response lines.
        Only messages with status 'REC UNREAD' are collected and deleted after parsing.
        :param lines: List of response lines from the modem
        :return: List of parsed SMS message dictionaries
        """
        messages = []
        current_msg = None
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            if line.startswith('+CMGL:'):
                # Save previous message if it's unread
                if current_msg and current_msg.get('body'): # and current_msg['status'] == 'REC UNREAD':
                    messages.append(current_msg)
                
                # Parse new message header
                try:
                    parts = line.split(',')
                    if len(parts) >= 6:
                        index = parts[0].split(':')[1].strip()
                        status = parts[1].strip().strip('"')
                        sender = parts[2].strip().strip('"')
                        try:
                            date = parts[4].strip().strip('"') + ' ' + parts[5].strip().split('+')[0]
                        except IndexError:
                            date = "1/1/1970 00:00:00"
                        
                        dt = datetime.strptime(date, "%y/%m/%d %H:%M:%S")
                        
                        current_msg = {
                            'index': index,
                            'status': status,
                            'sender': sender,
                            'timestamp': int(dt.timestamp()),
                            'body': ''
                        }
                except (IndexError, ValueError) as e:
                    self._logger.warning(f"Failed to parse SMS header: {line}, error: {e}")
                    current_msg = None
                    
            elif current_msg is not None and line and not line.startswith('OK') and not line.startswith('ERROR'):
                current_msg['body'] += line + '\n'
        
        # Don't forget the last message
        if current_msg and current_msg.get('body') and current_msg['status'] == 'REC UNREAD':
            self._logger.debug(f"Adding unread message: {current_msg}")
            messages.append(current_msg)

        for msg in messages:
            self.delete_sms(msg['index'])
        return messages
    
    def read_sms(self) -> List[Dict]:
        """
        Read all unread SMS messages from the modem.
        Uses AT+CMGL="REC UNREAD" to fetch only unread messages.
        :return: List of unread SMS message dictionaries
        """
        try:
            response = self._send_command('AT+CMGL="REC UNREAD"')
            lines = response.splitlines()
            return self._parse_sms_message(lines)
        except Exception as e:
            self._logger.error(f"Error reading SMS: {e}")
            return []
    
    
    def delete_sms(self, index: str) -> bool:
        """
        Delete an SMS message from the modem by its index.
        :param index: Index of the SMS message to delete
        :return: True if deletion was successful, False otherwise
        """
        try:
            response = self._send_command(f'AT+CMGD={index}')
            return 'OK' in response
        except Exception as e:
            self._logger.error(f"Error deleting SMS {index}: {e}")
            return False
    
    def close(self):
        """
        Close the serial connection to the modem if it is open.
        """
        if self._serial_conn and self._serial_conn.is_open:
            self._serial_conn.close()


# if __name__ == '__main__':
#     cp = CheckModemPort()
#     x = cp.find_port()
#     print(x)