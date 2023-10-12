import asyncio
import uuid

import aiohttp
import isodate
from aiohttp import ClientWebSocketResponse, ClientResponseError
from tenacity import RetryError, AsyncRetrying, stop_after_attempt, retry_if_exception_type, wait_exponential

from racetime_bot.utils import arg_parser

class RaceHandler:
    """
    Standard race handler.

    You should use this class as a basis for creating your own handler that
    can consume incoming messages, react to race data changes, and send stuff
    back to the race room.
    """
    # This is used by `should_stop` to determine when the handler should quit.
    stop_at = ['cancelled', 'finished']

    def __init__(self, logger, conn: ClientWebSocketResponse, bot, state, command_prefix='!'):
        """
        Base handler constructor.

        Sets up the following attributes:
        * conn - WebSocket connection, used internally.
        * data - Race data dict, as retrieved from race detail API endpoint.
        * logger - The logger instance bot was instantiated with.
        * state - A dict of stateful data for this race
        * ws - The open WebSocket, used internally.
        * bot - The parent bot object, useful for creating new race rooms.

        About data vs state - data is the race information retrieved from the
        server and can be read by your handler, but should not be written to.
        The state on the other hand can be used by your handler to preserve
        information about the race. It is preserved even if the handler is
        recreated (e.g. due to disconnect). Use it for any information you
        want.
        """
        self.ws = conn
        self.data = {}
        self.logger = logger
        self.state = state
        self.command_prefix = command_prefix
        self.bot = bot

    async def should_stop(self):
        """
        Determine if the handler should be terminated. This is checked after
        every receieved message.

        By default, checks if the race state matches one of the values in
        `stop_at`.
        """
        return self.data.get('status', {}).get('value') in self.stop_at

    async def begin(self):
        """
        Bot actions to perform when first connecting to a race room.

        Override this method to add an intro for when your bot first appears.
        """
        pass

    async def consume(self, data):
        """
        Standard message consumer. This is called for every message we receive
        from the site.

        This implementation will attempt to find an appropriate method to call
        to handle the incoming data, based on its type. For example if we have
        a "race.data" type message, it will call `self.race_data(data)`.
        """
        msg_type = data.get('type')

        self.logger.debug('[%(race)s] Received %(msg_type)s' % {
            'race': self.data.get('name'),
            'msg_type': msg_type,
        })

        method = msg_type.replace('.', '_')
        if msg_type and hasattr(self, method):
            await getattr(self, method)(data)
        else:
            self.logger.debug(f'No handler for {msg_type}, ignoring.')

    async def end(self):
        """
        Bot actions to perform just before disconnecting from a race room.

        Override this method to add an outro for when your bot leaves.
        """
        pass

    async def error(self, data):
        """
        Consume an incoming "error" type message.

        By default, just raises the message as an exception.
        """
        raise Exception(data.get('errors'))

    async def chat_message(self, data):
        """
        Consume an incoming "chat.message" type message.

        This method assumes a standard bot operation. It checks the first word
        in the message, and if it looks like an exclaimation command like
        "!seed", then it will call the relevant method, i.e.
        `self.ex_seed(args, message, positional_args, keyword_args)` (where `args` is the remainder of the
        message split up by words, and message is the original message blob).
        """
        message = data.get('message', {})

        if message.get('is_bot') or message.get('is_system'):
            self.logger.debug('Ignoring bot/system message.')
            return

        words = message.get('message', '').split(' ')
        if words and words[0].startswith(self.command_prefix):
            # look to see if there's a command handler for this
            method = 'ex_' + words[0][len(self.command_prefix):]
            if hasattr(self, method):
                args = words[1:]
                self.logger.debug('[%(race)s] Calling handler for %(word)s' % {
                    'race': self.data.get('name'),
                    'word': words[0],
                })
                try:
                    await getattr(self, method)(args, message)
                except Exception as e:
                    self.logger.error('Command raised exception.', exc_info=True)
                    await self.send_message(f'Command raised exception: {str(e)}')

            # if not, look to see if there's a new command handler for this
            method = 'ex2_' + words[0][len(self.command_prefix):]
            if hasattr(self, method):
                self.logger.debug('[%(race)s] Calling new handler for %(word)s' % {
                    'race': self.data.get('name'),
                    'word': words[0],
                })
                try:
                    positional_args, keyword_args = arg_parser(message.get('message', ''))
                    await getattr(self, method)(message, positional_args, keyword_args)
                except Exception as e:
                    self.logger.error('Command raised exception.', exc_info=True)
                    await self.send_message(f'Command raised exception: {str(e)}')


    async def race_data(self, data):
        """
        Consume an incoming "race.data" message.

        By default just updates the `data` attribute on the object. If you
        want to react to race changes, you can override this method to add
        further functionality.
        """
        self.data = data.get('race')

    async def send_message(self, message, actions=None, pinned=False, direct_to=None):
        """
        Send a chat message to the race room.

        `message` should be the message string you want to send.
        `actions` should be a list of Action objects (or raw dict data, if you're bold).
        `pinned` will pin the message at the top of the chat window.
        `direct_to` will send message as DM to the specified user ID.

        Note: for more info on setting up race actions, see `msg_actions.py`
        """
        if actions and not isinstance(actions, dict):
            # Assume actions is a list of Action objects
            actions = {
                action.label: action.data for action in actions
            }
        if direct_to and (actions or pinned):
            raise Exception('Cannot DM a message with actions or pin')
        await self.ws.send_json({
            'action': 'message',
            'data': {
                'message': message,
                'direct_to': direct_to,
                'actions': actions,
                'pinned': pinned,
                'guid': str(uuid.uuid4()),
            }
        })
        self.logger.debug('[%(race)s] Message: "%(message)s"' % {
            'race': self.data.get('name'),
            'message': message,
        })



    async def edit(self, **kwargs):
        """
        Edits the race.  For valid options, see
        https://github.com/racetimeGG/racetime-app/wiki/Category-bots#start-and-edit-races

        This method allows you to only pass what you actually want to change.
        Anything not specified will be pulled from the race room's data.
        """

        name = self.data.get('name')
        status = self.data.get('status', {}).get('value')

        if status in ['finished', 'cancelled']:
            # TODO raise a better exception
            raise Exception('Cannot edit a race that has finished or been cancelled.')

        if 'invitational' in kwargs:
            # TODO use a specific error class
            raise Exception('Cannot set invitational status.  Use make_open or make_invitational instead.')

        if kwargs.get('goal') and kwargs.get('custom_goal'):
            # TODO use a specific error class
            raise Exception('Either a goal or custom_goal can be specified, but not both.')

        settings = {}

        if self.data['goal'].get('custom'):
            settings['custom_goal'] = self.data['goal']['name']
        else:
            settings['goal'] = self.data['goal']['name']

        settings['unlisted'] = self.data['unlisted']
        settings['info_user'] = self.data['info_user']
        settings['start_delay'] = round(isodate.parse_duration(self.data['start_delay']).total_seconds())
        settings['time_limit'] = round(isodate.parse_duration(self.data['time_limit']).total_seconds()/3600)
        settings['streaming_required'] = self.data['streaming_required']
        settings['auto_start'] = self.data['auto_start']
        settings['allow_comments'] = self.data['allow_comments']
        settings['hide_comments'] = self.data.get('hide_comments', False)
        settings['allow_prerace_chat'] = self.data.get('allow_prerace_chat', True)
        settings['allow_midrace_chat'] = self.data['allow_midrace_chat']
        settings['allow_non_entrant_chat'] = self.data['allow_non_entrant_chat']
        settings['chat_message_delay'] = round(isodate.parse_duration(self.data['chat_message_delay']).total_seconds())

        for keyword in kwargs:
            if keyword == 'goal':
                settings[keyword] = kwargs[keyword]
                try:
                    del settings['custom_goal']
                except KeyError:
                    pass
            elif keyword == 'custom_goal':
                settings['custom_goal'] = kwargs['custom_goal']
                try:
                    del settings['goal']
                except KeyError:
                    pass
            else:
                settings[keyword] = kwargs[keyword]

        if not status in ['open', 'invitational']:
            for k in ['goal', 'custom_goal', 'start_delay', 'time_limit', 'streaming_required', 'auto_start', 'allow_prerace_chat']:
                try:
                    del settings[k]
                except KeyError:
                    continue

        try:
            async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(5),
                    retry=retry_if_exception_type(ClientResponseError),
                    wait=wait_exponential(multiplier=1, min=4, max=10)):
                with attempt:
                    async with self.bot.http.post(
                        url=self.bot.http_uri(f'/o/{name}/edit'),
                        data=settings,
                        ssl=self.bot.ssl_context,
                        headers={
                            'Authorization': 'Bearer ' + self.bot.access_token,
                        }
                    ) as resp:
                        if resp.status == 200:
                            return True
        except RetryError as e:
            raise e.last_attempt._exception from e

        raise Exception('Received an unexpected response while editing a race.')

    async def set_bot_raceinfo(self, info):
        """
        Set the `info_bot` field on the race room's data.
        """
        await self.ws.send_json({
            'action': 'setinfo',
            'data': {'info_bot': info}
        })

        self.logger.info('[%(race)s] Set info: "%(info)s"' % {
            'race': self.data.get('name'),
            'info': info,
        })

    async def set_raceinfo(self, info, overwrite=False, prefix=True):
        """
        Set the `info_user` field on the race room's data.

        This method is deprecated, please switch to using `set_bot_raceinfo`.

        `info_user` should be the information you wish to set. By default, this
        method will prefix your information with the existing info, if needed.
        You can change this to suffix with `prefix=False`, or disable this
        behaviour entirely with `overwrite=True`.
        """
        if self.data.get('info_user') and not overwrite:
            if prefix:
                info = info + ' | ' + self.data.get('info_user')
            else:
                info = self.data.get('info_user') + ' | ' + info

        await self.ws.send_json({
            'action': 'setinfo',
            'data': {'info_user': info}
        })
        self.logger.info('[%(race)s] [Deprecated] Set info: "%(info)s"' % {
            'race': self.data.get('name'),
            'info': info,
        })

    async def set_open(self):
        """
        Set the room in an open state.
        """
        await self.ws.send_json({
            'action': 'make_open'
        })
        self.logger.info('[%(race)s] Make open' % {
            'race': self.data.get('name')
        })

    async def set_invitational(self):
        """
        Set the room in an invite-only state.
        """
        await self.ws.send_json({
            'action': 'make_invitational'
        })
        self.logger.info('[%(race)s] Make invitational' % {
            'race': self.data.get('name')
        })

    async def force_start(self):
        """
        Forces a start of the race.
        """
        await self.ws.send_json({
            'action': 'begin'
        })
        self.logger.info('[%(race)s] Forced start' % {
            'race': self.data.get('name')
        })

    async def cancel_race(self):
        """
        Forcibly cancels a race.
        """
        await self.ws.send_json({
            'action': 'cancel'
        })
        self.logger.info('[%(race)s] cancelled' % {
            'race': self.data.get('name')
        })

    async def invite_user(self, user):
        """
        Invites a user to the race.

        `user` should be the hashid of the user.
        """
        await self.ws.send_json({
            'action': 'invite',
            'data': {
                'user': user
            }
        })
        self.logger.info('[%(race)s] invited %(user)s' % {
            'race': self.data.get('name'),
            'user': user
        })

    async def accept_request(self, user):
        """
        Accepts a request to join the race room.

        `user` should be the hashid of the user.
        """
        await self.ws.send_json({
            'action': 'accept_request',
            'data': {
                'user': user
            }
        })
        self.logger.info('[%(race)s] accept join request %(user)s' % {
            'race': self.data.get('name'),
            'user': user
        })

    async def force_unready(self, user):
        """
        Forcibly unreadies an entrant.

        `user` should be the hashid of the user.
        """
        await self.ws.send_json({
            'action': 'force_unready',
            'data': {
                'user': user
            }
        })
        self.logger.info('[%(race)s] force unready %(user)s' % {
            'race': self.data.get('name'),
            'user': user
        })

    async def remove_entrant(self, user):
        """
        Forcibly removes an entrant from the race.

        `user` should be the hashid of the user.
        """
        await self.ws.send_json({
            'action': 'remove_entrant',
            'data': {
                'user': user
            }
        })
        self.logger.info('[%(race)s] removed entrant %(user)s' % {
            'race': self.data.get('name'),
            'user': user
        })

    async def add_monitor(self, user):
        """
        Adds a user as a race monitor.

        `user` should be the hashid of the user.
        """
        await self.ws.send_json({
            'action': 'add_monitor',
            'data': {
                'user': user
            }
        })
        self.logger.info('[%(race)s] added race monitor %(user)s' % {
            'race': self.data.get('name'),
            'user': user
        })

    async def remove_monitor(self, user):
        """
        Removes a user as a race monitor.

        `user` should be the hashid of the user.
        """
        await self.ws.send_json({
            'action': 'remove_monitor',
            'data': {
                'user': user
            }
        })
        self.logger.info('[%(race)s] added race monitor %(user)s' % {
            'race': self.data.get('name'),
            'user': user
        })

    async def pin_message(self, message):
        """
        Pin a chat message.

        `message` should be the hashid of the message.
        """
        await self.ws.send_json({
            'action': 'pin_message',
            'data': {
                'message': message,
            }
        })
        self.logger.info('[%(race)s] pinned chat message %(message)s' % {
            'race': self.data.get('name'),
            'message': message
        })

    async def unpin_message(self, message):
        """
        Unpin a chat message.

        `message` should be the hashid of the message.
        """
        await self.ws.send_json({
            'action': 'unpin_message',
            'data': {
                'message': message,
            }
        })
        self.logger.info('[%(race)s] unpinned chat message %(message)s' % {
            'race': self.data.get('name'),
            'message': message
        })

    async def handle(self):
        """
        Low-level handler for the race room. This will loop over the websocket,
        processing any messages that come in.
        """
        self.logger.info('[%(race)s] Handler started' % {
            'race': self.data.get('name'),
        })
        await self.begin()
        error_count = 0
        loop_count = 0
        while True and error_count < 10:
            loop_count += 1
            try:
                data = await self.ws.receive_json()
                asyncio.create_task(self.consume(data))
                error_count = 0
            except TypeError:
                error_count += 1
                message = await self.ws.receive()
                if message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
                    self.logger.info(f"Websocket for {self.data.get('name')} was closed.")
                    self.ws = await self.bot.http.ws_connect(
                        self.bot.ws_uri(self.data.get('websocket_bot_url')),
                        headers={
                            'Authorization': 'Bearer ' + self.bot.access_token,
                        },
                        ssl=self.bot.ssl_context if self.bot.ssl_context is not None else self.bot.racetime_secure,
                    )
                    if self.ws.closed:
                        self.logger.warning(f"Websocket for {self.data.get('name')} is still closed.")
                    else:
                        self.logger.info(f"Websocket for {self.data.get('name')} re-established.")
                else:
                    self.logger.exception(f"Received unknown data of type {message.type}. Recovering handler for {self.data.get('name')}...")
            except ValueError:
                error_count += 1
                message = await self.ws.receive()
                self.logger.warning(f"Ignored message that was invalid json of type {message.type}.")
            if await self.should_stop():
                await self.end()
                break
