from tortoise import fields
from tortoise.models import Model
from typing import Optional
from discord import (
    Thread,
    TextChannel,
    CategoryChannel,
    VoiceChannel,
    ForumChannel,
    StageChannel,
    DMChannel,
)
from .mixins import AuthorIdMixin, ChannelIdMixin
from typing import Union


class Statistic(AuthorIdMixin, ChannelIdMixin, Model):
    id = fields.IntField(pk=True)
    guild_id = fields.BigIntField(null=True, index=True)
    channel_id = fields.BigIntField(index=True, null=False)
    thread_id = fields.BigIntField(null=True, index=True)
    author_id = fields.BigIntField(null=False, index=True)
    messages = fields.IntField(null=False, default=0)
    month = fields.DateField(null=False, index=True)
    num_words = fields.BigIntField(null=False, default=0)
    num_characters = fields.BigIntField(null=False, default=0)
    """Number of characters in all the messages for this statistics unit. For bots, this sums up their messages and embed byte counts. For non-bots, this counts their messages only."""
    num_attachments = fields.IntField(null=False, default=0)
    num_links = fields.IntField(null=False, default=0)
    """Number of links in all the messages for this statistics unit. For bots, this sums up their messages and embed links. For non-bots, this counts their messages only."""
    is_bot = fields.BooleanField(null=False, default=False, index=True)
    is_private = fields.BooleanField(null=True, default=None, index=True)
    """is_private is only used for threads. It is None for non-threads."""
    last_updated = fields.DatetimeField(null=False, auto_now=True)

    class Meta:
        unique_together = ("channel_id", "thread_id", "author_id", "month")

    @property
    def thread(self) -> Optional[Thread]:
        if self.thread_id:
            return self.bot.get_channel(self.thread_id)
        return None

    @property
    def target_channel(
        self,
    ) -> Union[TextChannel, CategoryChannel, VoiceChannel, ForumChannel, StageChannel, DMChannel, Thread,]:
        if self.thread_id:
            return self.thread
        return self.channel

    @property
    def target_channel_id(self) -> int:
        if self.thread_id:
            return self.thread_id
        return self.channel_id
