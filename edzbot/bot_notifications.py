"""Small text notifications via official HTTPS Bot API, never file downloads."""
from __future__ import annotations
import asyncio
import json
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


def _send(token: str, chat_id: str, text: str) -> int:
    payload = urlencode({'chat_id': chat_id, 'text': text[:3900]}).encode()
    req = Request('https://api.telegram.org/bot' + token + '/sendMessage',
                  data=payload, method='POST')
    try:
        with urlopen(req, timeout=20) as res:
            body = json.load(res)
    except HTTPError as ex:
        # urllib HTTPError includes the token-bearing API URL. Never leak it.
        raise RuntimeError(f'Bot API trả HTTP {ex.code}') from None
    except URLError:
        raise RuntimeError('Không kết nối được Telegram Bot API') from None
    if body.get('ok') is not True:
        raise RuntimeError('Telegram Bot API refused notification')
    return int(body['result']['message_id'])


async def send(token: str, chat_id: str, text: str) -> int:
    if not token or not chat_id:
        raise RuntimeError('Notification bot not configured')
    return await asyncio.to_thread(_send, token, chat_id, text)
