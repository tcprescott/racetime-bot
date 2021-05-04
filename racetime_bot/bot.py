import asyncio
import json
from functools import partial

import aiohttp
from tenacity import RetryError, AsyncRetrying, stop_after_attempt, retry_if_exception_type, wait_exponential

from .handler import RaceHandler


class TaskHandler:
    pass


class Bot:
    """
    The racetime.gg bot class.

    This bot uses event loops to connect to multiple race rooms at once. Each
    room is assigned a handler object, which you can determine with the
    `get_handler_class`/`get_handler_kwargs` methods.

    When implementing your own bot, you will need to specify what handler you
    wish to use, as the default `RaceHandler` will not actually do anything
    other than connect to the room and log the messages.
    """
    racetime_host = 'racetime.gg'
    racetime_port = None
    racetime_secure = True
    scan_races_every = 30
    reauthorize_every = 36000

    continue_on = [
        # Exception types that will not cause the bot to shut down.
        # websockets.ConnectionClosed,
    ]

    def __init__(self, category_slug, client_id, client_secret, logger,
                 ssl_context=None):
        """
        Bot constructor.
        """
        self.logger = logger
        self.category_slug = category_slug
        self.ssl_context = ssl_context

        self.loop = asyncio.get_event_loop()
        self.last_scan = None
        self.handlers = {}
        self.races = {}
        self.state = {}

        self.client_id = client_id
        self.client_secret = client_secret

        self.http = aiohttp.ClientSession(raise_for_status=True)

        self.join_lock = asyncio.Lock()

    def get_handler_class(self):
        """
        Returns the handler class for races. Each race the bot finds will have
        its own handler object of this class.
        """
        return RaceHandler

    def get_handler_kwargs(self, ws_conn, state):
        """
        Returns a dict of keyword arguments to be passed into the handler class
        when instantiated.

        Override this method if you need to pass additional kwargs to your race
        handler.
        """
        return {
            'conn': ws_conn,
            'logger': self.logger,
            'state': state,
            'command_prefix': '!',
        }

    def should_handle(self, race_data):
        """
        Determine if a race we've found should be handled by this bot or not.

        Returns True if the race should have a handler created for it, False
        otherwise.
        """
        status = race_data.get('status', {}).get('value')
        return status not in self.get_handler_class().stop_at

    async def authorize(self):
        """
        Get an OAuth2 token from the authentication server.
        """
        try:
            async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(5),
                    retry=retry_if_exception_type(aiohttp.ClientResponseError)):
                with attempt:
                    async with self.http.post(self.http_uri('/o/token'), data={
                        'client_id': self.client_id,
                        'client_secret': self.client_secret,
                        'grant_type': 'client_credentials',
                    }, ssl=self.ssl_context) as resp:
                        data = await resp.json()
                        if not data.get('access_token'):
                            raise Exception('Unable to retrieve access token.')
                        return data.get('access_token'), data.get('expires_in', 36000)
        except RetryError as e:
            raise e.last_attempt._exception from e

    async def create_handler(self, race_data):
        """
        Create a new WebSocket connection and set up a handler object to manage
        it.
        """
        ws_conn = await self.http.ws_connect(
            self.ws_uri(race_data.get('websocket_bot_url')),
            headers={
                'Authorization': 'Bearer ' + self.access_token,
            },
            ssl=self.ssl_context if self.ssl_context is not None else self.racetime_secure,
        )

        race_name = race_data.get('name')
        if race_name not in self.state:
            self.state[race_name] = {}

        cls = self.get_handler_class()
        kwargs = self.get_handler_kwargs(ws_conn, self.state[race_name])

        handler = cls(bot=self, **kwargs)
        handler.data = race_data

        self.logger.info(
            'Created handler for %(race)s'
            % {'race': race_data.get('name')}
        )

        return handler

    async def reauthorize(self):
        """
        Reauthorize with the token endpoint, to generate a new access token
        before the current one expires.

        This method runs in a constant loop, creating a new access token when
        needed.
        """
        while True:
            self.logger.info('Get new access token')
            self.access_token, self.reauthorize_every = await self.authorize()

            # close existing websocket connections so a new one can be established
            # with a new bot token
            for name in self.handlers:
                try:
                    await asyncio.wait_for(self.handlers[name].handler.ws.close(), timeout=30)
                    print(self.handlers[name].handler.ws.closed)
                except asyncio.TimeoutError:
                    self.logger.exception("Timed out waiting for websocket to close to allow for reconnection.")

            delay = self.reauthorize_every
            if delay > 600:
                # Get a token a bit earlier so that we don't get caught out by
                # expiry.
                delay -= 600
            await asyncio.sleep(delay)

    async def refresh_races(self):
        """
        Retrieve current race information from the category detail API
        endpoint, retrieving the current race list. Creates a handler and task
        for any race that should be handled but currently isn't.

        This method runs in a constant loop, checking for new races every few
        seconds.
        """
        def done(task_name, *args):
            del self.handlers[task_name]

        while True:
            self.logger.debug('Refresh races')
            try:
                async with self.http.get(
                        self.http_uri(f'/{self.category_slug}/data'),
                        ssl=self.ssl_context,
                ) as resp:
                    data = json.loads(await resp.read())
            except Exception:
                self.logger.error('Fatal error when attempting to retrieve race data.', exc_info=True)
                await asyncio.sleep(self.scan_races_every)
                continue
            self.races = {}
            for race in data.get('current_races', []):
                self.races[race.get('name')] = race

            for name, summary_data in self.races.items():
                async with self.join_lock:
                    if name not in self.handlers:
                        try:
                            async with self.http.get(
                                self.http_uri(summary_data.get('data_url')),
                                ssl=self.ssl_context,
                            ) as resp:
                                race_data = json.loads(await resp.read())
                        except Exception:
                            self.logger.error('Fatal error when attempting to retrieve summary data.', exc_info=True)
                            await asyncio.sleep(self.scan_races_every)
                            continue
                        if self.should_handle(race_data):
                            try:
                                handler = await self.create_handler(race_data)
                            except Exception as e:
                                self.logger.exception("Failed to create handler.")
                                continue
                            self.handlers[name] = TaskHandler()
                            self.handlers[name].task = self.loop.create_task(handler.handle())
                            self.handlers[name].task.add_done_callback(partial(done, name))
                            self.handlers[name].handler = handler
                        else:
                            if name in self.state:
                                del self.state[name]
                            self.logger.info(
                                'Ignoring %(race)s by configuration.'
                                % {'race': race_data.get('name')}
                            )

            await asyncio.sleep(self.scan_races_every)

    async def join_race_room(self, race_name, force=False):
        """
        Retrieve current race information for the requested race_name and
        joins that room, if the room isn't already being handled.  If it's
        already handled, no action is taken.

        This is useful if the room is unlisted, and as a result refresh_races
        would be unable to find and join the race room.

        `race_name` is in the format of categoryslug/adjective-verb-0123
        """
        def done(task_name, *args):
            del self.handlers[task_name]

        self.logger.info(f'Attempting to join {race_name}')

        if not race_name.split('/')[0] == self.category_slug:
            # TODO create a real exception class for this
            raise Exception('Race is not for the bot\'s category category.')

        try:
            async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(5),
                    retry=retry_if_exception_type(aiohttp.ClientResponseError),
                    wait=wait_exponential(multiplier=1, min=4, max=10)):
                with attempt:
                    async with self.http.get(
                            self.http_uri(f'/{race_name}/data'),
                            ssl=self.ssl_context,
                    ) as resp:
                        data = json.loads(await resp.read())
        except RetryError as e:
            raise e.last_attempt._exception from e

        name = data['name']

        async with self.join_lock:
            if name in self.handlers:
                self.logger.info(f'Returning existing handler for {name}')
                return self.handlers[name]

            if self.should_handle(data) or force:
                handler = await self.create_handler(data)
                self.handlers[name] = TaskHandler()
                self.handlers[name].task = self.loop.create_task(handler.handle())
                self.handlers[name].task.add_done_callback(partial(done, name))
                self.handlers[name].handler = handler

                return handler
            else:
                if name in self.state:
                    del self.state[name]
                self.logger.info(
                    'Ignoring %(race)s by configuration.'
                    % {'race': data.get('name')}
                )

    async def startrace(self, **kwargs):
        """
        Create a race.

        Creates a handler for the room and returns a handler object.
        """
        if kwargs.get('goal') and kwargs.get('custom_goal'):
            # TODO use a specific error class
            raise Exception('Either a goal or custom_goal can be specified, but not both.')

        try:
            async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(5),
                    retry=retry_if_exception_type(aiohttp.ClientResponseError),
                    wait=wait_exponential(multiplier=1, min=4, max=10)):
                with attempt:
                    async with self.http.post(
                        url=self.http_uri(f'/o/{self.category_slug}/startrace'),
                        data=kwargs,
                        ssl=self.ssl_context,
                        headers={
                            'Authorization': 'Bearer ' + self.access_token,
                        }
                    ) as resp:
                        headers = resp.headers
        except RetryError as e:
            raise e.last_attempt._exception from e

        if 'Location' in headers:
            race_name = headers['Location'][1:]
            return await self.join_race_room(race_name)

        raise Exception('Received an unexpected response when creating a race.')

    def handle_exception(self, loop, context):
        """
        Handle exceptions that occur during the event loop.
        """
        self.logger.error(context)
        exception = context.get('exception')
        if exception:
            self.logger.exception(context)

        if not exception or exception.__class__ not in self.continue_on:
            loop.stop()

    def run(self):
        """
        Run the bot. Creates an event loop then iterates over it forever.
        """
        self.loop.create_task(self.reauthorize())
        self.loop.create_task(self.refresh_races())
        self.loop.set_exception_handler(self.handle_exception)
        self.loop.run_forever()

    def http_uri(self, url):
        """
        Generate a HTTP/HTTPS URI from the given URL path fragment.
        """
        return self.uri(
            proto='https' if self.racetime_secure else 'http',
            url=url,
            port=self.racetime_port
        )

    def ws_uri(self, url):
        """
        Generate a WS/WSS URI from the given URL path fragment.
        """
        return self.uri(
            proto='wss' if self.racetime_secure else 'ws',
            url=url,
            port=self.racetime_port
        )

    def uri(self, proto, url, port=None):
        """
        Generate a URI from the given protocol and URL path fragment.
        """
        if port:
            return '%(proto)s://%(host)s:%(port)s%(url)s' % {
                'proto': proto,
                'host': self.racetime_host,
                'url': url,
                'port': port,
            }

        return '%(proto)s://%(host)s%(url)s' % {
            'proto': proto,
            'host': self.racetime_host,
            'url': url,
        }
