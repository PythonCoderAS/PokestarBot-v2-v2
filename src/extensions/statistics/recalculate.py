import re
from dataclasses import dataclass
from time import monotonic
from traceback import print_exception
from asyncio import Queue, create_task, Task, wait_for
from discord.ext.commands import Bot
from discord.app_commands import Group, command
from discord import (
    CategoryChannel,
    Interaction,
    Member,
    Thread,
    TextChannel,
    ForumChannel,
    VoiceChannel,
    PartialMessageable,
    User,
)
from collections import defaultdict
from ...models.statistic import Statistic
from typing import Optional

from .shared import (
    SUPPORTED_CHANNEL_TYPES,
    get_month_bucket_from_message,
    NewYork,
    is_private_thread,
    SUPPORTED_COMMAND_CHANNEL_TYPES,
)


@dataclass
class StatisticsUnit:
    messages: int = 0
    words: int = 0
    characters: int = 0
    attachments: int = 0
    links: int = 0


@dataclass(frozen=True)
class RecalculateTask:
    channel: SUPPORTED_CHANNEL_TYPES
    since_last: bool = False


class StatisticsRecalculate(Group, name="recalculate", description="Recalculate statistics information."):
    RECALCULATION_LOG_CHANNEL = 1102585915840397363
    URL_REGEX = re.compile(r"https?://\S{2,}")
    statistics: Optional["Statistics"] = None
    queue: Queue[RecalculateTask] = Queue()
    workers: list[Task] = []
    message_tasks: Queue[str] = Queue()

    def send_start_message(self, channel: SUPPORTED_CHANNEL_TYPES):
        log_channel = self.statistics.bot.get_channel(self.RECALCULATION_LOG_CHANNEL)
        if log_channel is None:
            return
        self.message_tasks.put_nowait(f"Starting statistics recalculation on {channel.mention}.")

    def send_end_message(self, channel: SUPPORTED_CHANNEL_TYPES, message_count: int):
        log_channel = self.statistics.bot.get_channel(self.RECALCULATION_LOG_CHANNEL)
        if log_channel is None:
            return
        self.message_tasks.put_nowait(
            f"Finished statistics recalculation on {channel.mention}. {message_count} messages were processed."
        )

    async def recalculate_channel(self, task: RecalculateTask):
        channel = task.channel
        kwargs = {}
        kwargs["guild_id"] = channel.guild.id if channel.guild else None
        kwargs["month"] = None
        stat_count: defaultdict[User | Member, StatisticsUnit] = defaultdict(StatisticsUnit)
        total_msgs = 0
        if isinstance(channel, Thread):
            kwargs["thread_id"] = channel.id
            kwargs["channel_id"] = channel.parent.id
        else:
            kwargs["channel_id"] = channel.id
        after = None
        if task.since_last:
            last_stat = await Statistic.filter(**kwargs).order_by("-last_updated").first()
            if last_stat:
                after = last_stat.last_updated.astimezone(NewYork).replace(
                    tzinfo=NewYork, day=1, hour=0, minute=0, second=0, microsecond=0
                )
        is_private = is_private_thread(channel)
        async with self.statistics.locks[channel.id]:
            async for message in channel.history(limit=None, after=after):
                if kwargs["month"] is None:
                    kwargs["month"] = get_month_bucket_from_message(message)
                elif kwargs["month"] != get_month_bucket_from_message(message):
                    await Statistic.filter(**kwargs).delete()
                    await Statistic.bulk_create(
                        [
                            Statistic(
                                author_id=author.id,
                                messages=unit.messages,
                                num_words=unit.words,
                                num_characters=unit.characters,
                                num_attachments=unit.attachments,
                                num_links=unit.links,
                                is_bot=author.bot,
                                is_private=is_private,
                                **kwargs,
                            )
                            for author, unit in stat_count.items()
                        ]
                    )
                    stat_count.clear()
                    kwargs["month"] = get_month_bucket_from_message(message)
                unit = stat_count[message.author]
                unit.messages += 1
                unit.words += len(message.content.split())
                unit.characters += len(message.content)
                unit.attachments += len(message.attachments)
                unit.links = len(self.URL_REGEX.findall(message.content))
                if message.author.bot:
                    # We count bytes of all embeds of all messages of all bots
                    unit.characters += sum(len(embed) for embed in message.embeds)
                    # For every embed with a link, we count that for bots.
                    for embed in message.embeds:
                        if embed.url:
                            unit.links += 1
                total_msgs += 1
            if stat_count:
                await Statistic.filter(**kwargs).delete()
                await Statistic.bulk_create(
                    [
                        Statistic(
                            author_id=author.id,
                            messages=unit.messages,
                            num_words=unit.words,
                            num_characters=unit.characters,
                            num_attachments=unit.attachments,
                            num_links=unit.links,
                            is_bot=author.bot,
                            is_private=is_private,
                            **kwargs,
                        )
                        for author, unit in stat_count.items()
                    ]
                )
        return total_msgs

    async def queue_channel_and_threads(
        self,
        interaction: Interaction,
        channel: SUPPORTED_CHANNEL_TYPES,
        include_threads: bool = False,
        only_threads: bool = False,
        since_last: bool = False,
    ):
        perms = channel.permissions_for(channel.guild.me)
        if not (perms.view_channel and perms.read_message_history):
            return
        if isinstance(channel, ForumChannel):
            include_threads = True
        else:
            if isinstance(channel, VoiceChannel):
                include_threads = False
                if only_threads:
                    return
            if not only_threads:
                self.queue.put_nowait(RecalculateTask(channel, since_last=since_last))
        if include_threads and isinstance(channel, (TextChannel, ForumChannel)):
            for thread in channel.threads:
                self.queue.put_nowait(RecalculateTask(thread, since_last=since_last))
            if isinstance(channel, TextChannel):
                async for thread in channel.archived_threads(limit=None, private=False):
                    self.queue.put_nowait(RecalculateTask(thread, since_last=since_last))
                async for thread in channel.archived_threads(
                    limit=None, private=True, joined=not channel.permissions_for(channel.guild.me).manage_threads
                ):
                    self.queue.put_nowait(RecalculateTask(thread, since_last=since_last))
            else:
                async for thread in channel.archived_threads(limit=None):
                    self.queue.put_nowait(RecalculateTask(thread, since_last=since_last))

    @command(name="channel", description="Recalculate channel statistics.")
    async def channel(
        self,
        interaction: Interaction,
        channel: Optional[SUPPORTED_COMMAND_CHANNEL_TYPES] = None,
        include_threads: bool = True,
        only_threads: bool = False,
        since_last: bool = False,
    ):
        bot: Bot = interaction.client
        if only_threads:
            include_threads = True
        if not await bot.is_owner(interaction.user):
            return await interaction.response.send_message("Only the bot owner can use this command.", ephemeral=True)
        if channel is None:
            channel = interaction.channel
        if isinstance(channel, PartialMessageable):
            channel = await self.statistics.bot.fetch_channel(channel.id)
        await interaction.response.send_message(f"Queued recalculation for <#{channel.id}>.", ephemeral=True)
        await self.queue_channel_and_threads(interaction, channel, include_threads, only_threads, since_last)

    @command(name="guild", description="Recalculate guild statistics.")
    async def guild(
        self,
        interaction: Interaction,
        guild_id: Optional[int] = None,
        include_threads: bool = True,
        only_threads: bool = False,
        since_last: bool = False,
    ):
        bot: Bot = interaction.client
        if not await bot.is_owner(interaction.user):
            return await interaction.response.send_message("Only the bot owner can use this command.", ephemeral=True)
        if guild_id is None:
            guild = interaction.guild
        else:
            guild = bot.get_guild(guild_id)
        if guild is None:
            return await interaction.response.send_message("Guild not found.", ephemeral=True)
        await interaction.response.send_message(f"Queued recalculation for {guild.name}.", ephemeral=True)
        for channel in guild.channels:
            if isinstance(channel, CategoryChannel):
                continue
            await self.queue_channel_and_threads(interaction, channel, include_threads, only_threads, since_last)

    @command(name="global", description="Recalculate global statistics.")
    async def global_(
        self,
        interaction: Interaction,
        include_threads: bool = True,
        only_threads: bool = False,
        since_last: bool = False,
    ):
        bot: Bot = interaction.client
        if not await bot.is_owner(interaction.user):
            return await interaction.response.send_message("Only the bot owner can use this command.", ephemeral=True)
        await interaction.response.send_message(
            "Queued global recalculation. This will take a *long* while!",
            ephemeral=True,
        )
        for guild in bot.guilds:
            for channel in guild.channels:
                if isinstance(channel, CategoryChannel):
                    continue
                await self.queue_channel_and_threads(interaction, channel, include_threads, only_threads, since_last)

    async def worker(self):
        while True:
            try:
                task = await self.queue.get()
                channel = task.channel
                self.send_start_message(channel)
                count = await self.recalculate_channel(task)
                self.send_end_message(channel, count)
                self.queue.task_done()
            except Exception as e:
                print_exception(e)

    async def log_worker(self):
        while True:
            msg = await self.message_tasks.get()
            cur_time = monotonic()
            while monotonic() - cur_time < 1 and len(msg) < 1700:
                try:
                    msg += "\n" + await wait_for(self.message_tasks.get(), 1)
                    self.message_tasks.task_done()
                except TimeoutError:
                    break
            await self.statistics.bot.get_channel(self.RECALCULATION_LOG_CHANNEL).send(msg)
            self.message_tasks.task_done()

    async def start_workers(self):
        for _ in range(5):
            self.workers.append(create_task(self.worker()))
        self.workers.append(create_task(self.log_worker()))

    async def stop_workers(self):
        for worker in self.workers:
            try:
                worker.cancel()
            except Exception:
                pass
