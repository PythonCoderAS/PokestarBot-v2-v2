from tortoise import fields
from tortoise.models import Model
from typing import Optional
from discord import Thread, TextChannel, CategoryChannel, VoiceChannel, ForumChannel, StageChannel, DMChannel
from .mixins import AuthorIdMixin, ChannelIdMixin
from typing import Union


class Statistic(AuthorIdMixin, ChannelIdMixin, Model):
    id = fields.IntField(pk=True)
    guild_id = fields.BigIntField(null=True, index=True)
    channel_id = fields.BigIntField(index=True, null=False)
    thread_id = fields.BigIntField(null=True, index=True)
    author_id = fields.BigIntField(null=False, index=True)
    messages = fields.IntField(null=False, default=0)
    created_at = fields.DatetimeField(auto_now_add=True)
    updated_at = fields.DatetimeField(auto_now=True)

    class Meta:
        unique_together = ("channel_id", "thread_id", "author_id")

    @property
    def thread(self) -> Optional[Thread]:
        if self.thread_id:
            return self.bot.get_channel(self.thread_id)
        return None
    
    @property
    def target_channel(self) -> Union[TextChannel, CategoryChannel, VoiceChannel, ForumChannel, StageChannel, DMChannel, Thread]:
        if self.thread_id:
            return self.thread
        return self.channel
    
    @property
    def target_channel_id(self) -> int:
        if self.thread_id:
            return self.thread_id
        return self.channel_id
