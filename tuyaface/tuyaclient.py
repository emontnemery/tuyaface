import json
import logging
import queue
import select
import socket
import threading
import time

from . import (_connect, _process_raw_reply, send_request, set_state, status, tf)

logger = logging.getLogger(__name__)


HEART_BEAT_PING_TIME = 5
HEART_BEAT_PONG_TIME = 5

class TuyaClient(threading.Thread):

    """ Helper class to maintain a connection to and serialize access to a Tuya device. """
    def __init__(self, device: dict, on_status: callable=None, on_connection: callable=None):

        super().__init__()
        self.connection = None
        self.device = device
        self.force_reconnect = False
        self.last_ping = 0
        self.last_pong = time.time()
        self.on_connection = on_connection
        self.on_status = on_status
        self.command_queue = queue.Queue()
        self.seq=0
        # socketpair used to interrupt the worker thread
        self.socketpair = socket.socketpair()
        self.stop = threading.Event()


    def _ping(self):
        """ Send a ping message. """

        self.last_ping = time.time()
        try:
            logger.debug("TuyaClient: PING")
            replies = list(reply for reply in send_request(self.device, tf.HEART_BEAT, connection=self.connection, seq=self.seq))
            self.seq += 1
            if replies:
                logger.debug("TuyaClient: PONG %s", replies)
                self._reset_pong()
        except socket.error:
            self.force_reconnect = True


    def _reset_pong(self):
        """ Reset expired counter. """

        self.last_pong = time.time()


    def _is_connection_stale(self):
        """ Indicates if connection has expired. """

        if time.time() - self.last_ping > HEART_BEAT_PING_TIME:
            self._ping()

        return (time.time() - self.last_pong) > HEART_BEAT_PING_TIME + HEART_BEAT_PONG_TIME


    def _connect(self):

        self.connection = _connect(self.device)
        if self.on_connection:
            self.on_connection(True)
        self._reset_pong()

    def _interrupt(self):

        try:
            # Write to the socket to interrupt the worker thread
            self.socketpair[1].send(b"x")
        except socket.error:
            # The socketpair may already be closed during shutdown, ignore it
            pass


    def run(self):

        while not self.stop.is_set():
            try:
                force_sleep = False
                if self.force_reconnect:
                    self.force_reconnect = False
                    logger.warning("TuyaClient: reconnecting")
                    if self.connection:
                        try:
                            self.connection.close()
                        except Exception:
                            logger.exception("TuyaClient: exception when closing socket", exc_info=False)
                        if self.on_connection:
                            self.on_connection(False)
                        self.connection = None
                        continue

                if self.connection == None:
                    try:
                        logger.debug("TuyaClient: connecting")
                        self._connect()
                        logger.info("TuyaClient: connected")
                        continue
                    except Exception:
                        logger.exception("TuyaClient: exception when opening socket", exc_info=False)

                if self.connection:
                    # poll the socket, as well as the socketpair to allow us to be interrupted
                    rlist = [self.connection, self.socketpair[0]]
                    logger.debug("TuyaClient: ---------> Waiting for sockets")
                    can_read, _, _ = select.select(rlist, [], [], HEART_BEAT_PING_TIME/2)
                    logger.debug("TuyaClient: <--------- Data in sockets: %s", can_read)
                    if self.connection in can_read:
                        try:
                            data = self.connection.recv(4096)
                            logger.debug("TuyaClient: read from socket '%s' (%s)", data, len(data))
                            if data:
                                for reply in _process_raw_reply(self.device, data):
                                    logger.debug("TuyaClient: Got msg %s", reply)
                                    json_reply = None
                                    if reply["data"]:
                                        json_reply = json.loads(reply["data"])
                                    if self.on_status:
                                        self.on_status(json_reply)
                            else:
                                # If the socket is in the read list, but no data, sleep
                                force_sleep = True
                        except socket.error:
                            logger.exception("TuyaClient: exception when reading from socket", exc_info=False)
                            self.force_reconnect = True

                    if self.socketpair[0] in can_read:
                        # Clear the socket's buffer
                        logger.debug("TuyaClient: Interrupted")
                        self.socketpair[0].recv(128)

                    if self._is_connection_stale():
                        self.force_reconnect = True

                while not self.command_queue.empty():
                    command, args, reply_queue = self.command_queue.get()
                    logger.debug("TuyaClient: got command %s %s %s", command, args, reply_queue)
                    result = command(*args)
                    reply_queue.put(result)

                if not self.connection or force_sleep:
                    logger.debug("TuyaClient: sleeping..")
                    time.sleep(HEART_BEAT_PING_TIME/2)
            except Exception:
                logger.exception("TuyaClient: Unexpected exception")


    def stop_client(self):
        """Close the connection and stop the worker thread"""

        self.stop.set()
        self._interrupt()
        self.join()


    def _status(self, _):

        if self.connection == None:
            self._connect()
        try:
            logger.debug("TuyaClient: ---------------> _status waiting for reply")
            data = status(self.device, connection=self.connection, seq=self.seq)
            logger.debug("TuyaClient: ---------------> _status reply %s", data)
            self.seq += 1
            return data
        except socket.error:
            self.force_reconnect = True


    def status(self):

        reply_queue = queue.Queue(1)
        self.command_queue.put((self._status, (None,), reply_queue))
        self._interrupt()
        reply = None
        try:
            logger.debug("TuyaClient: ---------------> status waiting for reply")
            reply = reply_queue.get(timeout=2)
            logger.debug("TuyaClient: ---------------> status reply %s", reply)
            return reply
        except queue.Empty:
            logger.warning("TuyaClient: No reply to status")
            logger.debug("TuyaClient: ---------------> No reply to status")


    def _set_state(self, value: bool, idx: int = 1):

        if self.connection == None:
            self._connect()
        try:
            logger.debug("TuyaClient: ---------------> _set_state waiting for reply")
            data = set_state(self.device, value, idx, connection=self.connection, seq=self.seq)
            logger.debug("TuyaClient: ---------------> _set_state reply %s", data)
            self.seq += 1
            return data
        except socket.error:
            self.force_reconnect = True


    def set_state(self, value: bool, idx: int = 1):

        reply_queue = queue.Queue(1)
        self.command_queue.put((self._set_state, (value, idx), reply_queue))
        self._interrupt()
        reply = None
        try:
            logger.debug("TuyaClient: ---------------> set_state waiting for reply")
            reply = reply_queue.get(timeout=2)
            logger.debug("TuyaClient: ---------------> set_state reply %s", reply)
            return reply
        except queue.Empty:
            logger.warning("TuyaClient: No reply to set_state")
            logger.debug("TuyaClient: ---------------> No reply to set_state")
