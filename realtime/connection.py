import asyncio
import json
import logging
from collections import defaultdict
from functools import wraps
from typing import Callable, List, Dict, TypeVar, DefaultDict

import websockets
from typing_extensions import ParamSpec
from websockets.exceptions import ConnectionClosed

from realtime.channel import Channel
from realtime.exceptions import NotConnectedError
from realtime.message import HEARTBEAT_PAYLOAD, PHOENIX_CHANNEL, ChannelEvents, Message
from realtime.types import Callback

T_Retval = TypeVar("T_Retval")
T_ParamSpec = ParamSpec("T_ParamSpec")

logging.basicConfig(
    format="%(asctime)s:%(levelname)s - %(message)s", level=logging.INFO
)


def ensure_connection(func: Callable[T_ParamSpec, T_Retval]):
    @wraps(func)
    def wrapper(*args: T_ParamSpec.args, **kwargs: T_ParamSpec.kwargs) -> T_Retval:
        if not args[0].connected:
            raise NotConnectedError(func.__name__)

        return func(*args, **kwargs)

    return wrapper


class CallbackError(Exception):
    pass


class Socket:
    def __init__(
            self,
            url: str,
            auto_reconnect: bool = False,
            params=None,
            hb_interval: int = 30,
            version: int = 2,
            ping_timeout: int = 20,
    ) -> None:
        """
        `Socket` is the abstraction for an actual socket connection that receives and 'reroutes' `Message` according to its `topic` and `event`.
        Socket-Channel has a 1-many relationship.
        Socket-Topic has a 1-many relationship.
        :param url: Websocket URL of the Realtime server. starts with `ws://` or `wss://`
        :param params: Optional parameters for connection.
        :param hb_interval: WS connection is kept alive by sending a heartbeat message. Optional, defaults to 30.
        :param version: phoenix JSON serializer version.
        """
        self.ws_connection = None
        if params is None:
            params = {}
        self.url = url
        self.connected = False
        self.params = params
        self.hb_interval = hb_interval
        self.kept_alive = set()
        self.auto_reconnect = auto_reconnect
        self.version = version
        self.ping_timeout = ping_timeout

        self.listen_task = None
        self.keep_alive_task = None
        self.connection_lock = asyncio.Lock()
        self.reconnect_lock = asyncio.Lock()
        self.shutdown_lock = asyncio.Lock()
        self.receive_lock = asyncio.Lock()
        self.keep_alive_lock = asyncio.Lock()

        self.channels: DefaultDict[str, List[Channel]] = defaultdict(list)

    async def _run_callback_safe(self, callback: Callback, payload: Dict) -> None:
        try:
            if asyncio.iscoroutinefunction(callback):
                asyncio.create_task(callback(payload))
            else:
                callback(payload)
        except Exception as e:
            raise CallbackError("Error in callback") from e

    @ensure_connection
    async def _listen(self) -> None:
        """
        An infinite loop that keeps listening.
        :return: None
        """
        async def listen_to_messages():
            while self.connected:
                if self.ws_connection is None:
                    await asyncio.sleep(1)  # Wait a bit before trying to reconnect or handle the lack of connection
                    continue
                try:
                    async with self.receive_lock:
                        msg = await self.ws_connection.recv()
                        logging.debug(f"Realtime - received: {msg}")
                    msg_array = json.loads(msg)
                    msg = Message(
                        join_ref=msg_array[0],
                        ref=msg_array[1],
                        topic=msg_array[2],
                        event=msg_array[3],
                        payload=msg_array[4],
                    )
                    if msg.event == ChannelEvents.reply:
                        for channel in self.channels.get(msg.topic, []):
                            if msg.ref == channel.control_msg_ref:
                                if msg.payload["status"] == "error":
                                    logging.info(
                                        f"Error joining channel: {msg.topic} - {msg.payload['response']['reason']}"
                                    )
                                    break
                                elif msg.payload["status"] == "ok":
                                    logging.info(f"Successfully joined {msg.topic}")
                                    continue
                            else:
                                for cl in channel.listeners:
                                    if cl.ref in ["*", msg.ref]:
                                        await self._run_callback_safe(cl.callback, msg.payload)

                    if msg.event == ChannelEvents.close:
                        for channel in self.channels.get(msg.topic, []):
                            if msg.join_ref == channel.join_ref:
                                logging.info(f"Successfully left {msg.topic}")
                                continue

                    for channel in self.channels.get(msg.topic, []):
                        for cl in channel.listeners:
                            if cl.event in ["*", msg.event]:
                                await self._run_callback_safe(cl.callback, msg.payload)
                except (TimeoutError, ConnectionClosed) as e:
                    logging.error(f"Connection error: {e}")
                    await self.close()
                    if self.auto_reconnect:
                        logging.info("Connection with server closed, trying to reconnect...")
                        await asyncio.sleep(2)
                        await self.connect()
                    else:
                        logging.exception("Connection with the server closed.")
                        break
                except CallbackError:
                    logging.info("Error in callback")
        await listen_to_messages()

    async def connect(self) -> None:
        async with self.connection_lock:
            if self.connected:
                logging.warning("Ran connect() but self.connected is True. returning")
                return  # Already connected, no need to connect again

            # Close any existing session and connection before reconnecting
            await self.shutdown()

            try:
                self.ws_connection = await websockets.connect(self.url, ping_timeout=self.ping_timeout)
                self.connected = True
                logging.info('Realtime reconnected')
                if self.listen_task is None or self.listen_task.done():
                    self.listen_task = asyncio.create_task(self._listen())
                if self.keep_alive_task is None or self.keep_alive_task.done():
                    self.keep_alive_task = asyncio.create_task(self._keep_alive())
                if self.channels:
                    for channels in self.channels.values():
                        for channel in channels:
                            await channel.join()
            except Exception as e:
                logging.error(f"Error connecting to WebSocket: {e}")
                await self.shutdown()
                raise

    async def shutdown(self) -> None:
        async with self.shutdown_lock:
            if self.listen_task and not self.listen_task.done():
                self.listen_task.cancel()
                try:
                    await self.listen_task
                except asyncio.CancelledError:
                    logging.info("Listen task cancelled.")
            self.listen_task = None

            if self.keep_alive_task and not self.keep_alive_task.done():
                self.keep_alive_task.cancel()
                try:
                    await self.keep_alive_task
                except asyncio.CancelledError:
                    logging.info("Keep-alive task cancelled.")
            self.keep_alive_task = None

        # Close the WebSocket connection
        if hasattr(self.ws_connection, 'close'):
            await self.ws_connection.close()
            logging.info("WebSocket connection closed.")

        self.connected = False

    async def _handle_reconnection(self) -> None:
        for task in self.kept_alive:
            task.cancel()
        if self.auto_reconnect:
            logging.info("Connection with server closed, trying to reconnect...")
            await self.connect()
            for topic, channels in self.channels.items():
                for channel in channels:
                    await channel.join()
        else:
            logging.exception("Connection with the server closed.")

    async def leave_all(self) -> None:
        for channel in self.channels:
            for chan in self.channels.get(channel, []):
                await chan.leave()

    async def _keep_alive(self) -> None:
        """
        Sending heartbeat to server
        Ping - pong messages to verify connection is alive
        """
        while True:
            async with self.keep_alive_lock:
                # self.logger.debug('Realtime sending heartbeat...')
                try:
                    if self.ws_connection:
                        data = [
                            None,
                            None,
                            PHOENIX_CHANNEL,
                            ChannelEvents.heartbeat,
                            HEARTBEAT_PAYLOAD,
                        ]

                        await self.ws_connection.send(json.dumps(data))
                    # self.logger.debug('Realtime sent heartbeat')
                    await asyncio.sleep(self.hb_interval)
                except Exception as e:
                    logging.error(f"Error sending heartbeat: {e}")
                    await self.close()
                    if self.auto_reconnect:
                        logging.warning("Connection with server closed, trying to reconnect...")
                        await self.connect()
                    else:
                        logging.exception("Connection with the server closed.")
                        break

    @ensure_connection
    async def set_channel(self, topic: str) -> Channel:
        """
        :param topic: Initializes a channel and creates a two-way association with the socket
        :return: Channel
        """
        chan = Channel(self, topic, self.params)
        self.channels[topic].append(chan)
        await chan.join()
        return chan

    async def close(self) -> None:
        if self.ws_connection and not self.ws_connection.closed:
            await self.ws_connection.close()
            self.ws_connection = None
        self.connected = False

    def remove_channel(self, topic: str) -> None:
        """
        :param topic: Removes a channel from the socket
        :return: None
        """
        self.channels.pop(topic, None)

    def summary(self) -> None:
        """
        Prints a list of topics and event, and reference that the socket is listening to
        :return: None
        """
        for topic, chans in self.channels.items():
            for chan in chans:
                print(
                    f"Topic: {topic} | Events: {[e for e, _, _ in chan.listeners]} | References: {[r for _, r, _ in chan.listeners]}]"
                )
