from traceback import print_exception
from tortoise.functions import Sum
from discord.ext.commands import GroupCog, Bot
from discord.app_commands import Group, command, Range
from discord import (
    CategoryChannel,
    Guild,
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
)
from collections import defaultdict
from asyncio import Lock, Queue, create_task, Task
from ..models.statistic import Statistic
from typing import Literal, Optional, Union, cast
from enum import Enum, auto
from matplotlib.figure import Figure
from io import BytesIO
from datetime import date
from zoneinfo import ZoneInfo

NewYork = ZoneInfo("America/New_York")


# Months = Literal["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
# MonthNamesToNum: dict[str, int] = {month: i for i, month in enumerate(Months.__args__, start=1)}
class Months(Enum):
    January = 1
    February = 2
    March = 3
    April = 4
    May = 5
    June = 6
    July = 7
    August = 8
    September = 9
    October = 10
    November = 11
    December = 12


class PrivateMode(Enum):
    exclude = auto()
    hide = auto()  # This hides the private channels from all listings but not the total
    hide_name = auto()
    show = auto()
    ephemeral = auto()
    ephemeral_only_if_private = auto()


class LimitedPrivateMode(Enum):
    ephemeral = auto()
    ephemeral_only_if_private = auto()
    show = auto()


def get_month_bucket_from_message(message: Message) -> date:
    return message.created_at.astimezone(NewYork).replace(day=1).date()


SUPPORTED_COMMAND_CHANNEL_TYPES = Union[VoiceChannel, ForumChannel, TextChannel, Thread, StageChannel]
SUPPORTED_CHANNEL_TYPES = Union[SUPPORTED_COMMAND_CHANNEL_TYPES, DMChannel]


class StatisticsView(Group, name="view", description="View statistics information."):
    @staticmethod
    async def validate_guild_only(interaction: Interaction, error_message: str) -> bool:
        if not interaction.guild_id:
            await interaction.response.send_message(error_message, ephemeral=True)
        return interaction.guild_id is not None

    @staticmethod
    async def validate_before_and_after_dates(
        interaction: Interaction,
        before_month: Optional[Months] = None,
        before_year: Optional[int] = None,
        after_month: Optional[Months] = None,
        after_year: Optional[int] = None,
    ) -> bool:
        if (before_year, before_month).count(None) == 1:
            await interaction.response.send_message(
                "The `before_month` and `before_year` options must be used together.",
                ephemeral=True,
            )
            return False
        elif (after_year, after_month).count(None) == 2:
            before_date = None
        else:
            assert before_month is not None and before_year is not None
            before_date = date(before_year, cast(int, before_month.value), 1)
        if (after_year, after_month).count(None) == 1:
            await interaction.response.send_message(
                "The `after_month` and `after_year` options must be used together.",
                ephemeral=True,
            )
            return False
        elif (after_year, after_month).count(None) == 2:
            after_date = None
        else:
            assert after_month is not None and after_year is not None
            after_date = date(after_year, cast(int, after_month.value), 1)
        if after_date and after_date > date.today():
            await interaction.response.send_message("The `after_date` option cannot be in the future.", ephemeral=True)
            return False
        if before_date and after_date:
            if before_date < after_date:
                await interaction.response.send_message(
                    "The `before_date` option must be after the `after_date` option.",
                    ephemeral=True,
                )
                return False
        return True

    @classmethod
    async def validate_graph_options(
        cls, interaction: Interaction, graph_only: bool, top_items: int, param_name: str
    ) -> bool:
        if not graph_only:
            return True
        if graph_only and top_items <= 0:
            await interaction.response.send_message(
                f"The `{param_name}` option must be greater than 0 when using the `graph_only` option.",
                ephemeral=True,
            )
            return False
        return await cls.validate_guild_only(interaction, "The `graph_only` option can only be used in a server.")

    @staticmethod
    def is_private(channel: SUPPORTED_CHANNEL_TYPES) -> bool:
        if isinstance(channel, DMChannel):
            return True  # DMs are always private
        elif isinstance(channel, Thread):
            return channel.is_private()
        else:
            at_everyone_perms = channel.permissions_for(channel.guild.default_role)
            return not at_everyone_perms.view_channel

    @command(name="user", description="View user statistics.")
    async def user(
        self,
        interaction: Interaction,
        member: Optional[Member],
        top_channels: Range[int, 0, 25] = 10,
        graph_only: bool = False,
        include_threads: bool = True,
        private_mode: PrivateMode = PrivateMode.hide_name,
        before_month: Optional[Months] = None,
        before_year: Optional[Range[int, 2015]] = None,
        after_month: Optional[Months] = None,
        after_year: Optional[Range[int, 2015]] = None,
    ):
        if member is None:
            member = interaction.user
            user_id = interaction.user.id
        else:
            if not await self.validate_guild_only(interaction, "The `member` option can only be used in a server."):
                return
            user_id = member.id
        if include_threads and not await self.validate_guild_only(
            interaction, "The `include_threads` options can only be used in a server."
        ):
            return
        if not await self.validate_before_and_after_dates(
            interaction, before_month, before_year, after_month, after_year
        ):
            return
        if before_month:
            # We only need to check the month since the validator will check to make sure the year exists as well
            before_date = date(before_year, cast(int, before_month.value), 1)
        else:
            before_date = None
        if after_month:
            after_date = date(after_year, cast(int, after_month.value), 1)
        else:
            after_date = None
        if not await self.validate_graph_options(interaction, graph_only, top_channels, "top_channels"):
            return
        await interaction.response.defer(thinking=True)
        base = (
            Statistic.filter(author_id=user_id, guild_id=interaction.guild_id)
            .order_by("-messages")
            .annotate(total_messages=Sum("messages"))
            .group_by("guild_id", "channel_id", "thread_id", "author_id")
        )
        if before_date:
            base = base.filter(month__lte=before_date)
        if after_date:
            base = base.filter(month__gte=after_date)
        if not include_threads:
            base = base.filter(thread_id=None)
        base = base.group_by("guild_id", "channel_id", "thread_id", "author_id")
        values = base.values(
            "guild_id",
            "channel_id",
            "thread_id",
            "author_id",
            messages="total_messages",
        )
        stats = [Statistic(**value) async for value in values]
        if private_mode == PrivateMode.exclude:
            stats = [stat for stat in stats if not self.is_private(stat.channel)]
        total = sum([stat.messages for stat in stats])
        buf = BytesIO()
        made_graph = False
        if not (top_channels <= 0 or not interaction.guild_id):  # Skip graph if set to 0
            if private_mode == PrivateMode.hide:
                top_channels_stats_pool = [stat for stat in stats if not self.is_private(stat.channel)]
            else:
                top_channels_stats_pool = stats
            graph_stats = top_channels_stats_pool[:top_channels]
            names = [
                stat.target_channel.name
                if (not self.is_private(stat.target_channel) or private_mode != PrivateMode.hide_name)
                else "**Private Channel**"
                for stat in graph_stats
            ]
            counts = [stat.messages for stat in graph_stats]
            fig = Figure()
            ax = fig.subplots()
            ax.barh(names, counts)
            ax.set_xlabel("# of Messages")
            ax.set_ylabel("Channel/Thread")
            ax.set_title(f"Messages sent by {member.display_name}")
            fig.tight_layout()
            fig.savefig(buf, format="png", bbox_inches="tight")
            buf.seek(0)
            made_graph = True
        assert (
            not graph_only
            or made_graph
            # Represents "if graph only, then a graph must be made" (p->q)
        ), "Somehow requested graph only but no graph was made."
        has_at_least_one_private_channel = next((self.is_private(stat.channel) for stat in stats), None) != None
        if graph_only:
            return await interaction.followup.send(
                files=[
                    File(
                        buf,
                        filename="graph.png",
                        description="A graph of the top channels the user commented in",
                    )
                ],
                ephemeral=True
                if private_mode == PrivateMode.ephemeral
                or (has_at_least_one_private_channel and private_mode == PrivateMode.ephemeral_only_if_private)
                else False,
            )
        embed = Embed(
            title=f"Statistics for {member.display_name}",
            description=f"Total messages: **{total:,}**\n",
        )
        for stat in stats[:50]:  # Limit to 50 channels listed so we do not go over the embed character limit
            embed.description += f"**<#{stat.target_channel_id}>**: {stat.messages:,}\n"
        if interaction.guild_id:
            embed.description += f"\nTotal channels: **{len([item for item in stats if not item.thread_id])}** channels, **{sum([item.messages for item in stats if not item.thread_id]):,} messages**"
            embed.description += f"\nTotal threads: **{len([item for item in stats if item.thread_id])}** threads, **{sum([item.messages for item in stats if item.thread_id]):,} messages**"
            embed.description += f"\nTotal channels and threads: **{len(stats)}** distinct channels & threads"
        embed.description = embed.description.strip()
        if made_graph:
            embed.set_image(url="attachment://graph.png")
        await interaction.followup.send(
            embed=embed,
            files=[
                File(
                    buf,
                    filename="graph.png",
                    description="A graph of the top channels the user commented in",
                )
            ]
            if made_graph
            else [],
            ephemeral=True
            if private_mode == PrivateMode.ephemeral
            or (has_at_least_one_private_channel and private_mode == PrivateMode.ephemeral_only_if_private)
            else False,
        )

    @command(name="channel", description="View channel statistics.")
    async def channel(
        self,
        interaction: Interaction,
        channel: Optional[SUPPORTED_COMMAND_CHANNEL_TYPES],
        top_users: Range[int, 0, 25] = 10,
        graph_only: bool = False,
        include_threads: bool = False,
        private_mode: LimitedPrivateMode = LimitedPrivateMode.show,
        before_month: Optional[Months] = None,
        before_year: Optional[Range[int, 2015]] = None,
        after_month: Optional[Months] = None,
        after_year: Optional[Range[int, 2015]] = None,
    ):
        if channel is None:
            channel = interaction.channel
            channel_id = interaction.channel.id
        else:
            channel_id = channel.id
        if include_threads and not await self.validate_guild_only(
            interaction, "The `include_threads` options can only be used in a server."
        ):
            return
        if not await self.validate_before_and_after_dates(
            interaction, before_month, before_year, after_month, after_year
        ):
            return
        if before_month:
            # We only need to check the month since the validator will check to make sure the year exists as well
            before_date = date(before_year, cast(int, before_month.value), 1)
        else:
            before_date = None
        if after_month:
            after_date = date(after_year, cast(int, after_month.value), 1)
        else:
            after_date = None
        if not await self.validate_graph_options(interaction, graph_only, top_users, "top_users"):
            return
        await interaction.response.defer(thinking=True)
        if isinstance(channel, Thread):
            include_threads = False
            base = Statistic.filter(channel_id=channel.parent, guild_id=interaction.guild_id)
        base = (
            Statistic.filter(channel_id=channel_id, guild_id=interaction.guild_id)
            .order_by("-messages")
            .annotate(total_messages=Sum("messages"))
        )
        if before_date:
            base = base.filter(month__lte=before_date)
        if after_date:
            base = base.filter(month__gte=after_date)
        if not include_threads:
            base = base.filter(thread_id=None)
        base = base.group_by("guild_id", "channel_id", "thread_id", "author_id")
        values = base.values(
            "guild_id",
            "channel_id",
            "thread_id",
            "author_id",
            messages="total_messages",
        )
        stats = [Statistic(**value) async for value in values]
        total = sum([stat.messages for stat in stats])
        buf = BytesIO()
        made_graph = False
        if not (top_users <= 0 or not interaction.guild_id):
            graph_stats = stats[:top_users]
            names = []
            for stat in graph_stats:
                user = interaction.guild.get_member(stat.author_id)
                names.append(user.display_name if user else "Unknown User")
            counts = [stat.messages for stat in graph_stats]
            fig = Figure()
            ax = fig.subplots()
            ax.barh(names, counts)
            ax.set_xlabel("# of Messages")
            ax.set_ylabel("User")
            ax.set_title(f"Messages sent in {channel.name}")
            fig.tight_layout()
            fig.savefig(buf, format="png", bbox_inches="tight")
            buf.seek(0)
            made_graph = True
        assert (
            not graph_only
            or made_graph
            # Represents "if graph only, then a graph must be made" (p->q)
        ), "Somehow requested graph only but no graph was made."
        has_at_least_one_private_channel = next((self.is_private(stat.channel) for stat in stats), None) != None
        if graph_only:
            return await interaction.followup.send(
                files=[
                    File(
                        buf,
                        filename="graph.png",
                        description="A graph of the top users in the channel",
                    )
                ],
                ephemeral=True
                if private_mode == LimitedPrivateMode.ephemeral
                or (has_at_least_one_private_channel and private_mode == LimitedPrivateMode.ephemeral_only_if_private)
                else False,
            )
        embed = Embed(
            title=f"Statistics for {channel.name}",
            description=f"Total messages: **{total:,}**\n",
        )
        for stat in stats[:50]:  # Limit to 50 users listed so we do not go over the embed character limit
            user = interaction.guild.get_member(stat.author_id)
            embed.description += f"**{user.mention if user else 'Unknown User'}**: {stat.messages:,}\n"
        if made_graph:
            embed.set_image(url="attachment://graph.png")
        await interaction.followup.send(
            embed=embed,
            files=[
                File(
                    buf,
                    filename="graph.png",
                    description="A graph of the top channels the user commented in",
                )
            ]
            if made_graph
            else [],
            ephemeral=True
            if private_mode == LimitedPrivateMode.ephemeral
            or (has_at_least_one_private_channel and private_mode == LimitedPrivateMode.ephemeral_only_if_private)
            else False,
        )


class StatisticsRecalculate(Group, name="recalculate", description="Recalculate statistics information."):
    RECALCULATION_LOG_CHANNEL = 1102585915840397363
    statistics: Optional["Statistics"] = None
    queue: Queue[SUPPORTED_CHANNEL_TYPES] = Queue()
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

    async def recalculate_channel(self, channel: SUPPORTED_CHANNEL_TYPES):
        kwargs = {}
        kwargs["guild_id"] = channel.guild.id if channel.guild else None
        kwargs["month"] = None
        message_count: defaultdict[int, int] = defaultdict(int)
        total_msgs = 0
        if isinstance(channel, Thread):
            kwargs["thread_id"] = channel.id
            kwargs["channel_id"] = channel.parent.id
        else:
            kwargs["channel_id"] = channel.id
        async with self.statistics.locks[channel.id]:
            async for message in channel.history(limit=None):
                if kwargs["month"] is None:
                    kwargs["month"] = get_month_bucket_from_message(message)
                elif kwargs["month"] != get_month_bucket_from_message(message):
                    await Statistic.filter(**kwargs).delete()
                    await Statistic.bulk_create(
                        [
                            Statistic(author_id=author_id, messages=count, **kwargs)
                            for author_id, count in message_count.items()
                        ]
                    )
                    message_count.clear()
                    kwargs["month"] = get_month_bucket_from_message(message)
                message_count[message.author.id] += 1
                total_msgs += 1
            if message_count:
                await Statistic.filter(**kwargs).delete()
                await Statistic.bulk_create(
                    [
                        Statistic(author_id=author_id, messages=count, **kwargs)
                        for author_id, count in message_count.items()
                    ]
                )
        return total_msgs

    async def queue_channel_and_threads(
        self,
        interaction: Interaction,
        channel: SUPPORTED_CHANNEL_TYPES,
        include_threads: bool = False,
        only_threads: bool = False,
    ):
        if isinstance(channel, ForumChannel):
            include_threads = True
        else:
            if isinstance(channel, VoiceChannel):
                include_threads = False
                if only_threads:
                    return
            if not only_threads:
                self.queue.put_nowait(channel)
        if include_threads and isinstance(channel, (TextChannel, ForumChannel)):
            for thread in channel.threads:
                self.queue.put_nowait(thread)
            if isinstance(channel, TextChannel):
                async for thread in channel.archived_threads(limit=None, private=False):
                    self.queue.put_nowait(thread)
                if interaction.app_permissions.manage_threads:
                    async for thread in channel.archived_threads(limit=None, private=True):
                        self.queue.put_nowait(thread)
                else:
                    async for thread in channel.archived_threads(limit=None, private=True, joined=True):
                        self.queue.put_nowait(thread)
            else:
                async for thread in channel.archived_threads(limit=None):
                    self.queue.put_nowait(thread)

    @command(name="channel", description="Recalculate channel statistics.")
    async def channel(
        self,
        interaction: Interaction,
        channel: Optional[SUPPORTED_COMMAND_CHANNEL_TYPES] = None,
        include_threads: bool = True,
        only_threads: bool = False,
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
        await self.queue_channel_and_threads(interaction, channel, include_threads, only_threads)

    @command(name="guild", description="Recalculate guild statistics.")
    async def guild(
        self,
        interaction: Interaction,
        guild_id: Optional[int] = None,
        include_threads: bool = True,
        only_threads: bool = False,
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
            await self.queue_channel_and_threads(interaction, channel, include_threads, only_threads)

    @command(name="global", description="Recalculate global statistics.")
    async def global_(
        self,
        interaction: Interaction,
        include_threads: bool = True,
        only_threads: bool = False,
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
                await self.queue_channel_and_threads(interaction, channel, include_threads, only_threads)

    async def worker(self):
        while True:
            try:
                channel = await self.queue.get()
                self.send_start_message(channel)
                count = await self.recalculate_channel(channel)
                self.send_end_message(channel, count)
                self.queue.task_done()
            except Exception as e:
                print_exception(e)

    async def log_worker(self):
        while True:
            msg = await self.message_tasks.get()
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
            except:
                pass


class Statistics(GroupCog, group_name="statistics", description="View statistics information."):
    view = StatisticsView()
    recalculate = StatisticsRecalculate()

    def __init__(self, bot: Bot, **kwargs):
        super().__init__(**kwargs)
        self.bot = bot
        # We lock per (channel, author) to ensure that the same object isn't modified twice
        self.locks: defaultdict[tuple[int, int], Lock] = defaultdict(Lock)
        self.recalculate.statistics = self

    @GroupCog.listener()
    async def on_message(self, message: Message):
        async with self.locks[message.channel.id]:
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
                        **kwargs,
                    )
                else:
                    existing.messages += 1
                    await existing.save()
            else:
                existing = await Statistic.filter(channel_id=message.channel.id, thread_id=None, **kwargs).first()
                if existing is None:
                    await Statistic.create(channel_id=message.channel.id, messages=1, **kwargs)
                else:
                    existing.messages += 1
                    await existing.save()

    async def cog_load(self):
        await self.recalculate.start_workers()

    async def cog_unload(self):
        await self.recalculate.stop_workers()


async def setup(bot: Bot):
    await bot.add_cog(Statistics(bot))
