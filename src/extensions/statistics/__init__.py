from discord.ext.commands import GroupCog, Bot

from discord import (
    Message,
    Thread,
)
from collections import defaultdict
from asyncio import Lock

from .recalculate import StatisticsRecalculate
from .shared import get_month_bucket_from_message
from .view import StatisticsView
from .view.shared import is_private
from ...models.statistic import Statistic


class Statistics(GroupCog, group_name="statistics", description="View statistics information."):
    view = StatisticsView()
    recalculate = StatisticsRecalculate()

    def __init__(self, bot: Bot, **kwargs):
        super().__init__(**kwargs)
        self.bot = bot
        # We lock per (channel, author) to ensure that the same object isn't modified twice
        self.locks: defaultdict[int, Lock] = defaultdict(Lock)
        self.recalculate.statistics = self

    @GroupCog.listener()
    async def on_message(self, message: Message):
        async with self.locks[message.channel.id]:
            words = len(message.content.split())
            characters = len(message.content)
            attachments = len(message.attachments)
            links = len(StatisticsRecalculate.URL_REGEX.findall(message.content))
            if message.author.bot:
                # We count bytes of all embeds of all messages of all bots
                characters += sum(len(embed) for embed in message.embeds)
                # We count all embeds with links
                for embed in message.embeds:
                    if embed.url:
                        links += 1
            month = get_month_bucket_from_message(message)
            kwargs = {
                "author_id": message.author.id,
                "month": month,
                "guild_id": message.guild.id if message.guild else None,
            }
            if isinstance(message.channel, Thread):
                existing = await Statistic.filter(thread_id=message.channel.id, **kwargs).first()
                if existing is None:
                    await Statistic.create(
                        thread_id=message.channel.id,
                        channel_id=message.channel.parent.id,
                        messages=1,
                        num_words=words,
                        num_characters=characters,
                        num_attachments=attachments,
                        num_links=links,
                        is_bot=message.author.bot,
                        is_private=is_private(message.channel),
                        **kwargs,
                    )
                else:
                    existing.messages += 1
                    existing.num_words += words
                    existing.num_characters += characters
                    existing.num_attachments += attachments
                    existing.num_links += links
                    await existing.save()
            else:
                existing = await Statistic.filter(channel_id=message.channel.id, thread_id=None, **kwargs).first()
                if existing is None:
                    await Statistic.create(
                        channel_id=message.channel.id,
                        messages=1,
                        num_words=words,
                        num_characters=characters,
                        num_attachments=attachments,
                        num_links=links,
                        is_bot=message.author.bot,
                        is_private=None,
                        **kwargs,
                    )
                else:
                    existing.messages += 1
                    existing.num_words += words
                    existing.num_characters += characters
                    existing.num_attachments += attachments
                    existing.num_links += links
                    await existing.save()

    async def cog_load(self):
        await self.recalculate.start_workers()

    async def cog_unload(self):
        await self.recalculate.stop_workers()


async def setup(bot: Bot):
    await bot.add_cog(Statistics(bot))
