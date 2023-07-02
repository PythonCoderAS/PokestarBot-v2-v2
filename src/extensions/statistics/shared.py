from datetime import date
from typing import Union, Optional
from zoneinfo import ZoneInfo

from discord import (
    CategoryChannel,
    Interaction,
    Member,
    Embed,
    File,
    Message,
    Thread,
    TextChannel,
    ForumChannel,
    VoiceChannel,
    StageChannel,
    DMChannel,
    PartialMessageable,
    User,
)


NewYork = ZoneInfo("America/New_York")

def get_month_bucket_from_message(message: Message) -> date:
    return message.created_at.astimezone(NewYork).replace(day=1).date()


SUPPORTED_COMMAND_CHANNEL_TYPES = Union[VoiceChannel, ForumChannel, TextChannel, Thread, StageChannel]
SUPPORTED_CHANNEL_TYPES = Union[SUPPORTED_COMMAND_CHANNEL_TYPES, DMChannel]

def is_private_thread(channel: SUPPORTED_CHANNEL_TYPES) -> Optional[bool]:
    """Returns if the thread is private.

    Use to set `is_private` on Statistic.
    """
    if isinstance(channel, Thread):
        return channel.is_private()
    else:
        return None
