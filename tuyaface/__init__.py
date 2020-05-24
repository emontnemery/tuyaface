import time
import socket
import json
from bitstring import BitArray
import binascii
from hashlib import md5
import logging

from . import aescipher
from . import const as tf
from .helper import *

logger = logging.getLogger(__name__)


def _generate_json_data(device_id: str, command: int, data: dict):

    """
    Fill the data structure for the command with the given values
    return: json str
    """

    payload_dict = {

        tf.CONTROL: {"devId": "", "uid": "", "t": ""},
        tf.STATUS: {"gwId": "", "devId": ""},
        tf.HEART_BEAT: {},
        tf.DP_QUERY: {"gwId": "", "devId": "", "uid": "", "t": ""},
        tf.CONTROL_NEW: {"devId": "", "uid": "", "t": ""},
        tf.DP_QUERY_NEW: {"devId": "", "uid": "", "t": ""},
    }

    json_data = {}
    if command in payload_dict:
        json_data = payload_dict[command]

    if 'gwId' in json_data:
        json_data['gwId'] = device_id
    if 'devId' in json_data:
        json_data['devId'] = device_id
    if 'uid' in json_data:
        json_data['uid'] = device_id  # still use id, no seperate uid
    if 't' in json_data:
        json_data['t'] = str(int(time.time()))

    if command == tf.CONTROL_NEW:
        json_data['dps'] = {"1": None, "2": None, "3": None}
    if data is not None:
        json_data['dps'] = data

    return json.dumps(json_data)


def _generate_payload(device: dict, command: int, data: dict=None, request_cnt: int=0):
    """
    Generate the payload to send.

    Args:
        device: Device attributes
        request_cnt: request sequence number
        command: The type of command.
            This is one of the entries from payload_dict
        data: The data to be send.
            This is what will be passed via the 'dps' entry
    """

    #TODO: don't overwrite variables
    payload_json = _generate_json_data(
        device['deviceid'],
        command,
        data
    ).replace(' ', '').encode('utf-8')

    header_payload_hb = b''
    payload_hb = payload_json

    if device['protocol'] == '3.1':

        if command == tf.CONTROL:
            payload_crypt = aescipher.encrypt(device['localkey'], payload_json)
            preMd5String = b'data=' + payload_crypt + b'||lpv=' +  b'3.1||' + device['localkey']
            m = md5()
            m.update(preMd5String)
            hexdigest = m.hexdigest()

            header_payload_hb = b'3.1' + hexdigest[8:][:16].encode('latin1')
            payload_hb =  header_payload_hb + payload_crypt

    elif device['protocol'] == '3.3':

        if command != tf.DP_QUERY:
            # add the 3.3 header
            header_payload_hb = b'3.3' +  b"\0\0\0\0\0\0\0\0\0\0\0\0"

        payload_crypt = aescipher.encrypt(device['localkey'], payload_json, False)
        payload_hb = header_payload_hb + payload_crypt
    else:
        raise Exception('Unknown protocol %s.' % (device['protocol']))

    return _stitch_payload(payload_hb, request_cnt, command)


def _stitch_payload(payload_hb: bytes, request_cnt: int, command: int):
    """
    Joins the payload request parts together
    """

    command_hb = command.to_bytes(4, byteorder='big')
    request_cnt_hb = request_cnt.to_bytes(4, byteorder='big')

    payload_hb = payload_hb + hex2bytes("000000000000aa55")

    payload_hb_len_hs = len(payload_hb).to_bytes(4, byteorder='big')
    
    header_hb = hex2bytes('000055aa') + request_cnt_hb + command_hb + payload_hb_len_hs
    buffer_hb = header_hb + payload_hb

    # calc the CRC of everything except where the CRC goes and the suffix
    hex_crc = format(binascii.crc32(buffer_hb[:-8]) & 0xffffffff, '08X')
    return buffer_hb[:-8] + hex2bytes(hex_crc) + buffer_hb[-4:]


def _process_raw_reply(device: dict, raw_reply: bytes):
    """
    Splits the raw reply(s) into chuncks and decrypts it.
    returns json str or str (error)
    """

    a = BitArray(raw_reply)

    #TODO: don't overwrite variables
    for s in a.split('0x000055aa', bytealigned=True):
        sbytes = s.tobytes()
        payload = None

        # Skip invalid messages
        if len(sbytes) < 28 or not s.endswith('0x0000aa55'):
            continue

        # Parse header
        seq = int.from_bytes(sbytes[4:8], byteorder='big')
        cmd = int.from_bytes(sbytes[8:12], byteorder='big')
        sz = int.from_bytes(sbytes[12:16], byteorder='big')
        rc = int.from_bytes(sbytes[16:20], byteorder='big')
        has_return_code = (rc & 0xFFFFFF00) == 0
        crc = int.from_bytes(sbytes[-8:-4], byteorder='big')

        # Check CRC
        if crc != binascii.crc32(sbytes[:-8]):
            continue

        if device['protocol'] == '3.1':
            
            data = sbytes[20:-8]
            if sbytes[20:21] == b'{':
                if not isinstance(data, str):
                    payload = data.decode()
            elif sbytes[20:23] == b'3.1':
                logger.info('we\'ve got a 3.1 reply, code untested')
                data = data[3:]  # remove version header
                data = data[16:]  # remove (what I'm guessing, but not confirmed is) 16-bytes of MD5 hexdigest of payload
                payload = aescipher.decrypt(device['localkey'], data)

        elif device['protocol'] == '3.3':
            if sz > 12:
                data = sbytes[20:8+sz]
                if cmd == tf.STATUS:
                    data = data[15:]
                payload = aescipher.decrypt(device['localkey'], data, False)

        msg = {"cmd": cmd, "seq": seq, "data": payload}
        if has_return_code:
            msg['rc'] = rc
        yield msg


def _select_status_reply(replies: list):
    """
    Find the first valid status reply
    returns dict
    """

    filtered_replies = list(filter(lambda x: x["data"] and x["cmd"] == tf.STATUS, replies))
    if len(filtered_replies) == 0:
        return None
    return filtered_replies[0]


def _select_command_reply(replies: list, command: int, seq: int=None):
    """
    Find the last valid status reply
    returns dict
    """

    filtered_replies = list(filter(lambda x: x["cmd"] == command, replies))
    if seq is not None:
        filtered_replies = list(filter(lambda x: x["seq"] == seq, filtered_replies))
    if len(filtered_replies) == 0:
        return None
    if len(filtered_replies) > 1:
        logger.info("Got multiple replies %s for request [%x:%s]", filtered_replies, command, tf.cmd_to_string.get(command, f'UNKNOWN'))
    return filtered_replies[0]


def _set_properties(device: dict):
    """
    Set default tuyaface properties
    """

    device.setdefault('tuyaface', {
        'sequence_nr': 0,
        'connection': None,
        'availability': False,
        'pref_status_cmd': tf.DP_QUERY
    })


def _status(device: dict, expect_reply: int = 1, recurse_cnt: int = 0):
    """
    Sends current status request to the tuya device and waits for status update
    returns (dict, list(dict))
    """

    _set_properties(device)
    cmd = device['tuyaface']['pref_status_cmd']

    request_cnt = _send_request(device, cmd, None)

    replies = []
    new_replies = [None]
    request_reply = None
    status_reply = None

    # There might already be data waiting in the socket, e.g. a heartbeat reply, continue reading until
    # the expected response has been received or there is a timeout
    # If status is triggered by DP_QUERY, the status is in a DP_QUERY message
    # If status is triggered by CONTROL_NEW, the status is a STATUS message
    while new_replies and (not request_reply or (cmd == tf.CONTROL_NEW and not status_reply)):
        new_replies = list(reply for reply in _receive_replies(device, expect_reply))
        replies = replies + new_replies
        request_reply = _select_command_reply(replies, cmd, request_cnt)
        status_reply = _select_status_reply(replies)

    # If there is valid reply to tf.DP_QUERY, use it as status reply
    if cmd == tf.DP_QUERY and request_reply["data"] and request_reply["data"] != 'json obj data unvalid':
        status_reply = request_reply

    if not status_reply and recurse_cnt < 3 and device['tuyaface']['availability']:
        if request_reply and request_reply["data"] == 'json obj data unvalid':
            # some devices (ie LSC Bulbs) only offer partial status with CONTROL_NEW instead of DP_QUERY
            device['tuyaface']['pref_status_cmd'] = tf.CONTROL_NEW
        status_reply, new_replies = _status(device, 2, recurse_cnt + 1)
        replies = replies + new_replies

    return (status_reply, replies)


def status(device: dict):
    """
    Requests status of the tuya device
    returns dict
    """
    
    #TODO: validate/sanitize request
    reply, _ = _status(device)
    if not reply:
        reply = {'data':'{}'}
    logger.debug("reply: '%s'", reply)
    return json.loads(reply["data"])


def _set_status(device: dict, dps: dict):
    """
    Sends status update request to the tuya device and waits for status update
    returns (dict, list(dict))
    """
    _set_properties(device)

    #TODO: validate/sanitize request
    tmp = { str(k):v for k,v in dps.items() }
    request_cnt = _send_request(device, tf.CONTROL, tmp)

    replies = []
    new_replies = [None]
    request_reply = None
    status_reply = None

    # There might already be data waiting in the socket, e.g. a heartbeat reply, continue reading until
    # the expected response has been received or there is a timeout
    while new_replies and (not request_reply or not status_reply):
        new_replies = list(reply for reply in _receive_replies(device, 2))
        replies = replies + new_replies
        request_reply = _select_command_reply(replies, tf.CONTROL, request_cnt)
        status_reply = _select_status_reply(replies)

    return (status_reply, replies)


def set_status(device: dict, dps: dict):
    """
    Sends status update request to the tuya device and waits for status update
    returns dict
    """

    status_reply, _ = _set_status(device, dps)

    if not status_reply:
        reply = {'data':'{}'}
    logger.debug("status_reply: %s", status_reply)
    return json.loads(status_reply["data"])


def set_state(device: dict, value: bool, idx: int = 1):
    """
    Sends status update request for one dps value to the tuya device
    returns dict
    """

    # turn a device on / off
    return set_status(device,{idx: value})


def _connect(device: dict, timeout:int = 2):

    """
    connects to the tuya device
    returns connection object
    """

    connection = None

    logger.info('Connecting to %s' % device['ip'])
    try:
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        connection.settimeout(timeout)
        connection.connect((device['ip'], 6668))
        device['tuyaface']['connection'] = connection
        device['tuyaface']['availability'] = True
        return connection
    except Exception as e:
        logger.warning('Failed to connect to %s. Retry in %d seconds' % (device['ip'], 1))
        raise e


def _receive_replies(device: dict, max_receive_cnt):
    if max_receive_cnt <= 0:
        return

    connection = device['tuyaface']['connection']

    try:
        data = connection.recv(4096)

        for reply in _process_raw_reply(device, data):
            if reply:
                logger.debug("received msg (seq %s): [%x:%s] '%s'", reply['seq'], reply['cmd'], tf.cmd_to_string.get(reply['cmd'], f'UNKNOWN'), reply.get('data', ''))
            yield reply
    except socket.timeout as e:
        device['tuyaface']['availability'] = False
        pass
    except Exception as e:
        raise e

    yield from _receive_replies(device, max_receive_cnt-1)


def _send_request(device: dict, command: int = tf.DP_QUERY, payload: dict = None):
    """
    Connects to the tuya device and sends the request
    returns request counter of the sent request
    """

    connection = device['tuyaface']['connection']
    if not connection:
        _connect(device)
        connection = device['tuyaface']['connection']

    request_cnt = device['tuyaface'].get('sequence_nr', 0)
    if 'sequence_nr' in device['tuyaface']:
        device['tuyaface']['sequence_nr'] = request_cnt + 1

    request = _generate_payload(device, command, payload, request_cnt)
    logger.debug("sending command: [%x:%s] payload: [%s]", command, tf.cmd_to_string.get(command, f'UNKNOWN'), payload)
    try:
        connection.send(request)
    except Exception as e:
        raise e

    return request_cnt
