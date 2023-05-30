import re
from dataclasses import dataclass
from time import monotonic
from traceback import print_exception

from tortoise.expressions import Q
from tortoise.functions import Sum
from discord.ext.commands import GroupCog, Bot
from discord.app_commands import Group, command, Range
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
from collections import defaultdict
from asyncio import Lock, Queue, create_task, Task, wait_for
from ..models.statistic import Statistic
from typing import Optional, Union, cast
from enum import Enum, auto
from matplotlib.figure import Figure
from io import BytesIO
from datetime import date
from zoneinfo import ZoneInfo

NewYork = ZoneInfo("America/New_York")


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


class BotModes(Enum):
    exclude = auto()
    include = auto()
    only = auto()


class StatisticMode(Enum):
    messages = auto()
    words = auto()
    characters = auto()
    attachments = auto()
    links = auto()

    def label(self) -> str:
        return StatisticModeLabels[self]

    def field_name(self) -> str:
        return StatisticModeFieldNames[self]

    def title_word(self) -> str:
        return StatisticModeTitleWord[self]


StatisticModeLabels = {
    StatisticMode.messages: "# of Messages",
    StatisticMode.words: "# of Words",
    StatisticMode.characters: "# of Characters",
    StatisticMode.attachments: "# of Attachments",
    StatisticMode.links: "# of Links",
}

StatisticModeTitleWord = {
    StatisticMode.messages: "Messages",
    StatisticMode.words: "Words",
    StatisticMode.characters: "Characters",
    StatisticMode.attachments: "Attachments",
    StatisticMode.links: "Links",
}

StatisticModeFieldNames = {
    StatisticMode.messages: "messages",
    StatisticMode.words: "num_words",
    StatisticMode.characters: "num_characters",
    StatisticMode.attachments: "num_attachments",
    StatisticMode.links: "num_links",
}


def get_month_bucket_from_message(message: Message) -> date:
    return message.created_at.astimezone(NewYork).replace(day=1).date()


SUPPORTED_COMMAND_CHANNEL_TYPES = Union[VoiceChannel, ForumChannel, TextChannel, Thread, StageChannel]
SUPPORTED_CHANNEL_TYPES = Union[SUPPORTED_COMMAND_CHANNEL_TYPES, DMChannel]


class StatisticsView(Group, name="view", description="View statistics information."):
    @staticmethod
    async def validate_guild_only(interaction: Interaction, error_message: str) -> bool:
        """Returns if the interaction is in a guild. If not, sends an ephemeral error message."""
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
        """Returns if the before and after dates are valid. If not, sends an ephemeral error message."""
        if (before_year, before_month).count(None) == 1:
            await interaction.response.send_message(
                "The `before_month` and `before_year` options must be used together.",
                ephemeral=True,
            )
            return False
        elif (before_year, before_month).count(None) == 2:
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
        """Returns if the graph options are valid. If not, sends an ephemeral error message."""
        if not graph_only:
            return True
        if graph_only and top_items <= 0:
            await interaction.response.send_message(
                f"The `{param_name}` option must be greater than 0 when using the `graph_only` option.",
                ephemeral=True,
            )
            return False
        return await cls.validate_guild_only(interaction, "The `graph_only` option can only be used in a server.")

    @classmethod
    def is_private(cls, channel: SUPPORTED_CHANNEL_TYPES) -> bool:
        """Returns if the channel or thread is private."""
        if isinstance(channel, DMChannel):
            return True  # DMs are always private
        elif isinstance(channel, Thread):
            return channel.is_private() or cls.is_private(channel.parent)
        elif channel is None:
            return False
        else:
            at_everyone_perms = channel.permissions_for(channel.guild.default_role)
            return not at_everyone_perms.view_channel

    @staticmethod
    def is_private_thread(channel: SUPPORTED_CHANNEL_TYPES) -> Optional[bool]:
        """Returns if the thread is private.

        Use to set `is_private` on Statistic.
        """
        if isinstance(channel, Thread):
            return channel.is_private()
        else:
            return None

    @classmethod
    def is_private_stat(cls, stat: Statistic) -> bool:
        """Returns if the statistic is private."""
        return stat.is_private if stat.is_private is not None else cls.is_private(stat.target_channel)

    @classmethod
    async def parse_date_options(
        cls,
        interaction: Interaction,
        before_month: Optional[Months] = None,
        before_year: Optional[int] = None,
        after_month: Optional[Months] = None,
        after_year: Optional[int] = None,
    ) -> tuple[bool, tuple[Optional[date], Optional[date]]]:
        """Returns the parsed date options.

        Return format: (valid, (before_date, after_date))
        """
        if not await cls.validate_before_and_after_dates(
            interaction, before_month, before_year, after_month, after_year
        ):
            return False, (None, None)
        if before_month:
            # We only need to check the month since the validator will check to make sure the year exists as well
            before_date = date(before_year, cast(int, before_month.value), 1)
        else:
            before_date = None
        if after_month:
            after_date = date(after_year, cast(int, after_month.value), 1)
        else:
            after_date = None
        return True, (before_date, after_date)

    @staticmethod
    def make_bar_graph(bar_names, bar_values, title, x_label, y_label) -> BytesIO:
        buf = BytesIO()
        fig = Figure()
        ax = fig.subplots()
        ax.barh(bar_names, bar_values)
        ax.set_xlabel(x_label)
        ax.set_ylabel(y_label)
        ax.set_title(title)
        fig.tight_layout()
        fig.savefig(buf, format="png", bbox_inches="tight")
        buf.seek(0)
        return buf

    @staticmethod
    def get_number_and_unit(num_bytes: int) -> tuple[float, str]:
        """Returns the number of bytes in a human-readable format. Round the final result to 2 decimal places."""
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if num_bytes < 1024.0:
                return round(num_bytes, 2), unit
            num_bytes /= 1024.0
        return round(num_bytes, 2), "PB"

    @classmethod
    def format_byte_string(cls, num_bytes: int) -> str:
        """Returns the number of bytes in a human-readable format."""
        num, unit = cls.get_number_and_unit(num_bytes)
        return f"{num} {unit}"

    @staticmethod
    def is_ephemeral(mode: PrivateMode | LimitedPrivateMode, has_at_least_one_private_channel: bool) -> bool:
        if isinstance(mode, PrivateMode):
            return (
                True
                if mode == PrivateMode.ephemeral
                or (has_at_least_one_private_channel and mode == PrivateMode.ephemeral_only_if_private)
                else False
            )
        else:
            return (
                True
                if mode == LimitedPrivateMode.ephemeral
                or (has_at_least_one_private_channel and mode == LimitedPrivateMode.ephemeral_only_if_private)
                else False
            )

    @command(name="user", description="View user statistics.")
    async def user(
        self,
        interaction: Interaction,
        member: Optional[Member],
        top_channels: Range[int, 0, 25] = 10,
        graph_only: bool = False,
        include_threads: bool = True,
        aggregate_threads: bool = True,
        private_mode: PrivateMode = PrivateMode.hide_name,
        before_month: Optional[Months] = None,
        before_year: Optional[Range[int, 2015]] = None,
        after_month: Optional[Months] = None,
        after_year: Optional[Range[int, 2015]] = None,
        statistic: StatisticMode = StatisticMode.messages,
    ):
        if member is None:
            member = interaction.user
            user_id = interaction.user.id
        else:
            if not await self.validate_guild_only(interaction, "The `member` option can only be used in a server."):
                return
            user_id = member.id
            if user_id != interaction.user.id:
                if not interaction.user.guild_permissions.administrator:
                    # Only admins can view other users' full stats
                    if private_mode not in [PrivateMode.hide, PrivateMode.hide_name, PrivateMode.exclude]:
                        return await interaction.response.send_message(
                            "You must be an administrator to view other users' full private threads.",
                            ephemeral=True,
                        )
        if include_threads and not await self.validate_guild_only(
            interaction, "The `include_threads` options can only be used in a server."
        ):
            return
        if not include_threads and aggregate_threads:
            aggregate_threads = False
        if not await self.validate_graph_options(interaction, graph_only, top_channels, "top_channels"):
            return
        date_valid, (before_date, after_date) = await self.parse_date_options(
            interaction, before_month, before_year, after_month, after_year
        )
        if not date_valid:
            return
        await interaction.response.defer(thinking=True)
        base = (
            Statistic.filter(author_id=user_id, guild_id=interaction.guild_id)
            .annotate(sum=Sum(statistic.field_name()))
            .order_by("-sum")
        )
        if before_date:
            base = base.filter(month__lte=before_date)
        if after_date:
            base = base.filter(month__gte=after_date)
        if not include_threads:
            base = base.filter(thread_id=None)
        if private_mode == PrivateMode.exclude:
            base = base.filter(is_private=False)
        if aggregate_threads:
            base = base.group_by("guild_id", "channel_id", "author_id", "is_private")
            values = base.values(
                "guild_id",
                "channel_id",
                "author_id",
                "sum",
                "is_private"
            )
        else:
            base = base.group_by("guild_id", "channel_id", "thread_id", "author_id", "is_private")
            values = base.values(
                "guild_id",
                "channel_id",
                "thread_id",
                "author_id",
                "sum",
                "is_private"
            )
        stats = []
        async for value in values:
            stat = Statistic(**value)
            stat.sum = int(value["sum"])
            stats.append(stat)
        if private_mode == PrivateMode.exclude:
            stats = [stat for stat in stats if not self.is_private_stat(stat)]
        total = sum([stat.sum for stat in stats])
        buf = None
        if not (top_channels <= 0 or not interaction.guild_id):  # Skip graph if set to 0
            if private_mode == PrivateMode.hide:
                top_channels_stats_pool = [stat for stat in stats if not self.is_private_stat(stat)]
            else:
                top_channels_stats_pool = stats
            graph_stats = top_channels_stats_pool[:top_channels]
            names = [
                stat.target_channel.name
                if (not self.is_private_stat(stat) or private_mode != PrivateMode.hide_name)
                else "**Private Channel or Thread**"
                for stat in graph_stats
            ]
            counts = [stat.sum for stat in graph_stats]
            buf = self.make_bar_graph(
                names,
                counts,
                f"{statistic.title_word()} sent by {member.display_name}",
                "Channel/Thread",
                statistic.label(),
            )
        assert (
            not graph_only
            or buf
            # Represents "if graph only, then a graph must be made" (p->q)
        ), "Somehow requested graph only but no graph was made."
        has_at_least_one_private_channel = next((self.is_private_stat(stat) for stat in stats), None) is not None
        if graph_only:
            return await interaction.followup.send(
                files=[
                    File(
                        buf,
                        filename="graph.png",
                        description="A graph of the top channels the user commented in",
                    )
                ],
                ephemeral=self.is_ephemeral(private_mode, has_at_least_one_private_channel),
            )
        embed = Embed(
            title=f"Statistics for {member.display_name}",
            description=f"Total {statistic.title_word().lower()}: **{total:,}**{' (**' + self.format_byte_string(total) + '**)' if statistic == StatisticMode.characters and total > 1024 else ''}\n",
        )
        for stat in stats[:50]:  # Limit to 50 channels listed so we do not go over the embed character limit
            embed.description += f"**<#{stat.target_channel_id}>**: {stat.sum:,}{' (' + self.format_byte_string(stat.sum) + ')' if statistic == StatisticMode.characters and stat.sum > 1024 else ''}\n"
        if interaction.guild_id:
            embed.description += (
                f"\nTotal channels: **{len([item for item in stats if not item.thread_id])}** "
                f"channels, **{sum([item.sum for item in stats if not item.thread_id]):,}** {statistic.title_word().lower()}"
            )
            if include_threads:
                embed.description += (
                    f"\nTotal threads: **{len([item for item in stats if item.thread_id])}** threads, "
                    f"**{sum([item.sum for item in stats if item.thread_id]):,}** {statistic.title_word().lower()}"
                )
                embed.description += f"\nTotal channels and threads: **{len(stats)}** distinct channels & threads"
        embed.description = embed.description.strip()
        if buf:
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
            if buf
            else [],
            ephemeral=self.is_ephemeral(private_mode, has_at_least_one_private_channel),
        )

    @command(name="channel", description="View channel statistics.")
    async def channel(
        self,
        interaction: Interaction,
        channel: Optional[SUPPORTED_COMMAND_CHANNEL_TYPES] = None,
        top_users: Range[int, 0, 25] = 10,
        graph_only: bool = False,
        include_threads: bool = False,
        bots: BotModes = BotModes.exclude,
        private_mode: LimitedPrivateMode = LimitedPrivateMode.show,
        before_month: Optional[Months] = None,
        before_year: Optional[Range[int, 2015]] = None,
        after_month: Optional[Months] = None,
        after_year: Optional[Range[int, 2015]] = None,
        statistic: StatisticMode = StatisticMode.messages,
    ):
        if channel is None:
            channel = interaction.channel
            channel_id = interaction.channel.id
        else:
            channel_id = channel.id
            if not channel.permissions_for(interaction.user).view_channel:
                return await interaction.response.send_message(
                    "You do not have permission to view that channel.", ephemeral=True
                )
        if include_threads and not await self.validate_guild_only(
            interaction, "The `include_threads` options can only be used in a server."
        ):
            return
        date_valid, (before_date, after_date) = await self.parse_date_options(
            interaction, before_month, before_year, after_month, after_year
        )
        if not date_valid:
            return
        if not await self.validate_graph_options(interaction, graph_only, top_users, "top_users"):
            return
        await interaction.response.defer(thinking=True)
        if isinstance(channel, Thread):
            include_threads = True
            base = Statistic.filter(channel_id=channel.parent.id, thread_id=channel.id, guild_id=interaction.guild_id)
        else:
            base = Statistic.filter(channel_id=channel_id, guild_id=interaction.guild_id)
        base = base.annotate(sum=Sum(statistic.field_name())).order_by("-sum")
        if before_date:
            base = base.filter(month__lte=before_date)
        if after_date:
            base = base.filter(month__gte=after_date)
        if bots == BotModes.exclude:
            base = base.filter(is_bot=False)
        elif bots == BotModes.only:
            base = base.filter(is_bot=True)
        if not include_threads:
            base = base.filter(thread_id=None)
        base = base.group_by("guild_id", "channel_id", "thread_id", "author_id")
        values = base.values(
            "guild_id",
            "channel_id",
            "thread_id",
            "author_id",
            "sum",
        )
        stats = []
        async for value in values:
            stat = Statistic(**value)
            stat.sum = int(value["sum"])
            stats.append(stat)
        total = sum([stat.sum for stat in stats])
        buf = None
        if top_users > 0:
            graph_stats = stats[:top_users]
            names = []
            for stat in graph_stats:
                user = interaction.guild.get_member(stat.author_id)
                names.append(user.display_name if user else "Unknown User")
            counts = [stat.sum for stat in graph_stats]
            buf = self.make_bar_graph(
                names, counts, f"{statistic.title_word()} sent in {channel.name}", "User", statistic.label()
            )
        assert (
            not graph_only
            or buf
            # Represents "if graph only, then a graph must be made" (p->q)
        ), "Somehow requested graph only but no graph was made."
        has_at_least_one_private_channel = next((self.is_private_stat(stat) for stat in stats), None) is not None
        if graph_only:
            return await interaction.followup.send(
                files=[
                    File(
                        buf,
                        filename="graph.png",
                        description="A graph of the top users in the channel",
                    )
                ],
                ephemeral=self.is_ephemeral(private_mode, has_at_least_one_private_channel),
            )
        embed = Embed(
            title=f"Statistics for {channel.name}",
            description=f"Total {statistic.title_word().lower()}: **{total:,}**{' (**' + self.format_byte_string(total) + '**)' if statistic == StatisticMode.characters and total > 1024 else ''}\n",
        )
        for stat in stats[:50]:  # Limit to 50 users listed so we do not go over the embed character limit
            user = interaction.guild.get_member(stat.author_id)
            embed.description += f"**{user.mention if user else 'Unknown User'}**: {stat.sum:,}{' (' + self.format_byte_string(stat.sum) + ')' if statistic == StatisticMode.characters and stat.sum > 1024 else ''}\n"
        if buf:
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
            if buf
            else [],
            ephemeral=self.is_ephemeral(private_mode, has_at_least_one_private_channel),
        )

    @command(name="threads", description="View statistics for threads in a channel.")
    async def threads(
        self,
        interaction: Interaction,
        channel: Optional[Union[TextChannel, ForumChannel]] = None,
        top_threads: Range[int, 0, 25] = 10,
        graph_only: bool = False,
        show_archived_private_threads: bool = False,
        show_all_private_threads: bool = False,
        private_mode: PrivateMode = PrivateMode.hide_name,
        bots: BotModes = "exclude",
        before_month: Optional[Months] = None,
        before_year: Optional[Range[int, 2015]] = None,
        after_month: Optional[Months] = None,
        after_year: Optional[Range[int, 2015]] = None,
        statistic: StatisticMode = StatisticMode.messages,
    ):
        """
        View statistics for threads in a channel.
        """
        if channel is None:
            channel = interaction.channel
            channel_id = interaction.channel.id
        else:
            channel_id = channel.id
        perms = channel.permissions_for(interaction.user)
        if not perms.view_channel:
            return await interaction.response.send_message(
                "You do not have permission to view that channel.", ephemeral=True
            )
        if not perms.manage_threads and show_all_private_threads and private_mode not in [PrivateMode.hide_name, PrivateMode.hide, PrivateMode.exclude]:
            return await interaction.response.send_message(
                "You do not have permission to view full private thread names.", ephemeral=True
            )
        if not await self.validate_guild_only(
            interaction, "The `include_threads` options can only be used in a server."
        ):
            return
        if not isinstance(channel, (TextChannel, ForumChannel)):
            return await interaction.response.send_message(
                "The channel must be a text channel or forum channel.", ephemeral=True
            )
        if isinstance(channel, ForumChannel):
            show_all_private_threads = False
            show_archived_private_threads = False
        date_valid, (before_date, after_date) = await self.parse_date_options(
            interaction, before_month, before_year, after_month, after_year
        )
        if not date_valid:
            return
        if not await self.validate_graph_options(interaction, graph_only, top_threads, "top_threads"):
            return
        await interaction.response.defer(thinking=True)
        valid_private_thread_ids = set()
        if channel.permissions_for(interaction.user).manage_threads:
            show_all_private_threads = True
        elif not show_all_private_threads:
            valid_private_thread_ids |= {thread.id for thread in channel.threads if
                                         thread.is_private() and interaction.user.id in [member.id for member in
                                                                                         thread.members]}
        if show_archived_private_threads and not show_all_private_threads:
            async for thread in channel.archived_threads(private=True, joined=True):
                if interaction.user.id in [member.id for member in thread.members]:
                    valid_private_thread_ids.add(thread.id)
        base = (
            Statistic.filter(channel_id=channel_id, guild_id=interaction.guild_id, thread_id__isnull=False)
            .annotate(sum=Sum(statistic.field_name()))
            .order_by("-sum")
        )
        if before_date:
            base = base.filter(month__lte=before_date)
        if after_date:
            base = base.filter(month__gte=after_date)
        if bots == BotModes.exclude:
            base = base.filter(is_bot=False)
        elif bots == BotModes.only:
            base = base.filter(is_bot=True)
        if private_mode == PrivateMode.exclude:
            base = base.filter(is_private=False)
        else:
            if valid_private_thread_ids:
                base = base.filter(Q(is_private=False) | Q(thread_id__in=valid_private_thread_ids))
        base = base.group_by("guild_id", "channel_id", "thread_id", "is_private")
        values = base.values(
            "guild_id",
            "channel_id",
            "thread_id",
            "is_private",
            "sum",
        )
        stats = []
        async for value in values:
            stat = Statistic(**value)
            stat.sum = int(value["sum"])
            stats.append(stat)
        if private_mode == PrivateMode.exclude:
            stats = [stat for stat in stats if not self.is_private_stat(stat)]
        total = sum([stat.sum for stat in stats])
        buf = None
        if top_threads > 0:
            if private_mode == PrivateMode.hide:
                top_threads_stats_pool = [stat for stat in stats if not self.is_private_stat(stat)]
            else:
                top_threads_stats_pool = stats
            graph_stats = top_threads_stats_pool[:top_threads]
            names = [
                stat.thread.name
                if (not self.is_private_stat(stat) or private_mode != PrivateMode.hide_name)
                else "**Private Thread**"
                for stat in graph_stats
            ]
            counts = [stat.sum for stat in graph_stats]
            buf = self.make_bar_graph(
                names, counts, f"{statistic.title_word()} in Threads in {channel.name}", "Thread", statistic.label()
            )
        assert (
            not graph_only
            or buf
            # Represents "if graph only, then a graph must be made" (p->q)
        ), "Somehow requested graph only but no graph was made."
        has_at_least_one_private_channel = next((self.is_private_stat(stat) for stat in stats), None) is not None
        if graph_only:
            return await interaction.followup.send(
                files=[
                    File(
                        buf,
                        filename="graph.png",
                        description="A graph of the top threads the channel has",
                    )
                ],
                ephemeral=self.is_ephemeral(private_mode, has_at_least_one_private_channel),
            )
        embed = Embed(
            title=f"Statistics for Threads in {channel.name}",
            description=f"Total threads: **{len(stats)}**, {statistic.title_word().lower()}: **{total:,}**{' (**' + self.format_byte_string(total) + '**)' if statistic == StatisticMode.characters and total > 1024 else ''}\n",
        )
        for stat in stats[:50]:  # Limit to 50 channels listed so we do not go over the embed character limit
            embed.description += f"**<#{stat.target_channel_id}>**: {stat.sum:,}{' (' + self.format_byte_string(stat.sum) + ')' if statistic == StatisticMode.characters and stat.sum > 1024 else ''}\n"
        embed.description = embed.description.strip()
        if buf:
            embed.set_image(url="attachment://graph.png")
        await interaction.followup.send(
            embed=embed,
            files=[
                File(
                    buf,
                    filename="graph.png",
                    description="A graph of the top threads the channel has",
                )
            ]
            if buf
            else [],
            ephemeral=self.is_ephemeral(private_mode, has_at_least_one_private_channel),
        )

    @command(name="server_channels", description="View statistics of all channels in the server.")
    async def server_channels(
        self,
        interaction: Interaction,
        top_channels: Range[int, 0, 25] = 10,
        graph_only: bool = False,
        include_all_channels: bool = False,
        include_threads: bool = True,
        aggregate_threads: bool = True,
        private_mode: PrivateMode = PrivateMode.hide_name,
        before_month: Optional[Months] = None,
        before_year: Optional[Range[int, 2015]] = None,
        after_month: Optional[Months] = None,
        after_year: Optional[Range[int, 2015]] = None,
        statistic: StatisticMode = StatisticMode.messages,
    ):
        if not await self.validate_guild_only(
            interaction, "The command can only be used in a server."
        ):
            return
        if include_all_channels and not interaction.user.guild_permissions.administrator:
            # Only admins can view all channel's full stats
            if private_mode not in [PrivateMode.hide, PrivateMode.hide_name, PrivateMode.exclude]:
                return await interaction.response.send_message(
                    "You must be an administrator to view other users' full private threads.",
                    ephemeral=True,
                )
        if not include_threads and aggregate_threads:
            aggregate_threads = False
        if not await self.validate_graph_options(interaction, graph_only, top_channels, "top_channels"):
            return
        date_valid, (before_date, after_date) = await self.parse_date_options(
            interaction, before_month, before_year, after_month, after_year
        )
        if not date_valid:
            return
        await interaction.response.defer(thinking=True)
        valid_channel_ids = set()
        base = (
            Statistic.filter(guild_id=interaction.guild_id)
            .annotate(sum=Sum(statistic.field_name()))
            .order_by("-sum")
        )
        if before_date:
            base = base.filter(month__lte=before_date)
        if after_date:
            base = base.filter(month__gte=after_date)
        if not include_threads:
            base = base.filter(thread_id=None)
        if private_mode == PrivateMode.exclude:
            base = base.filter(is_private=False)
        if not include_all_channels and not interaction.user.guild_permissions.administrator:
            for channel in interaction.guild.channels:
                if isinstance(channel, CategoryChannel):
                    continue
                if not channel.permissions_for(interaction.user).read_messages:
                    continue
                valid_channel_ids.add(channel.id)
            base = base.filter(channel_id__in=valid_channel_ids)
        if aggregate_threads:
            base = base.group_by("guild_id", "channel_id", "is_private")
            values = base.values(
                "guild_id",
                "channel_id",
                "sum",
                "is_private"
            )
        else:
            base = base.group_by("guild_id", "channel_id", "thread_id", "is_private")
            values = base.values(
                "guild_id",
                "channel_id",
                "thread_id",
                "sum",
                "is_private"
            )
        stats = []
        async for value in values:
            stat = Statistic(**value)
            stat.sum = int(value["sum"])
            stats.append(stat)
        if private_mode == PrivateMode.exclude:
            stats = [stat for stat in stats if not self.is_private_stat(stat)]
        total = sum([stat.sum for stat in stats])
        buf = None
        if not (top_channels <= 0 or not interaction.guild_id):  # Skip graph if set to 0
            if private_mode == PrivateMode.hide:
                top_channels_stats_pool = [stat for stat in stats if not self.is_private_stat(stat)]
            else:
                top_channels_stats_pool = stats
            graph_stats = top_channels_stats_pool[:top_channels]
            names = [
                stat.target_channel.name
                if (not self.is_private_stat(stat) or private_mode != PrivateMode.hide_name)
                else "**Private Channel or Thread**"
                for stat in graph_stats
            ]
            counts = [stat.sum for stat in graph_stats]
            buf = self.make_bar_graph(
                names,
                counts,
                f"{statistic.title_word()} sent in {interaction.guild.name}",
                "Channel/Thread",
                statistic.label(),
            )
        assert (
            not graph_only
            or buf
            # Represents "if graph only, then a graph must be made" (p->q)
        ), "Somehow requested graph only but no graph was made."
        has_at_least_one_private_channel = next((self.is_private_stat(stat) for stat in stats), None) is not None
        if graph_only:
            return await interaction.followup.send(
                files=[
                    File(
                        buf,
                        filename="graph.png",
                        description="A graph of the top channels in the server",
                    )
                ],
                ephemeral=self.is_ephemeral(private_mode, has_at_least_one_private_channel),
            )
        embed = Embed(
            title=f"Statistics for {interaction.guild.name}",
            description=f"Total {statistic.title_word().lower()}: **{total:,}**{' (**' + self.format_byte_string(total) + '**)' if statistic == StatisticMode.characters and total > 1024 else ''}\n",
        )
        for stat in stats[:50]:  # Limit to 50 channels listed so we do not go over the embed character limit
            embed.description += f"**<#{stat.target_channel_id}>**: {stat.sum:,}{' (' + self.format_byte_string(stat.sum) + ')' if statistic == StatisticMode.characters and stat.sum > 1024 else ''}\n"
        embed.description += (
            f"\nTotal channels: **{len([item for item in stats if not item.thread_id])}** "
            f"channels, **{sum([item.sum for item in stats if not item.thread_id]):,}** {statistic.title_word().lower()}"
        )
        if include_threads:
            embed.description += (
                f"\nTotal threads: **{len([item for item in stats if item.thread_id])}** threads, "
                f"**{sum([item.sum for item in stats if item.thread_id]):,}** {statistic.title_word().lower()}"
            )
            embed.description += f"\nTotal channels and threads: **{len(stats)}** distinct channels & threads"
        embed.description = embed.description.strip()
        if buf:
            embed.set_image(url="attachment://graph.png")
        await interaction.followup.send(
            embed=embed,
            files=[
                File(
                    buf,
                    filename="graph.png",
                    description="A graph of the top channels in the server",
                )
            ]
            if buf
            else [],
            ephemeral=self.is_ephemeral(private_mode, has_at_least_one_private_channel),
        )

    @command(name="server_users", description="View statistics of all users in the server.")
    async def server_users(
        self,
        interaction: Interaction,
        top_users: Range[int, 0, 25] = 10,
        graph_only: bool = False,
        include_threads: bool = False,
        bots: BotModes = BotModes.exclude,
        private_mode: LimitedPrivateMode = LimitedPrivateMode.show,
        before_month: Optional[Months] = None,
        before_year: Optional[Range[int, 2015]] = None,
        after_month: Optional[Months] = None,
        after_year: Optional[Range[int, 2015]] = None,
        statistic: StatisticMode = StatisticMode.messages,
    ):
        if not await self.validate_guild_only(
                interaction, "The command can only be used in a server."
        ):
            return
        date_valid, (before_date, after_date) = await self.parse_date_options(
            interaction, before_month, before_year, after_month, after_year
        )
        if not date_valid:
            return
        if not await self.validate_graph_options(interaction, graph_only, top_users, "top_users"):
            return
        await interaction.response.defer(thinking=True)
        base = (
            Statistic.filter(guild_id=interaction.guild_id)
            .annotate(sum=Sum(statistic.field_name()))
            .order_by("-sum")
        )
        if before_date:
            base = base.filter(month__lte=before_date)
        if after_date:
            base = base.filter(month__gte=after_date)
        if bots == BotModes.exclude:
            base = base.filter(is_bot=False)
        elif bots == BotModes.only:
            base = base.filter(is_bot=True)
        if not include_threads:
            base = base.filter(thread_id=None)
        base = base.group_by("author_id")
        values = base.values(
            "author_id",
            "sum",
        )
        stats = []
        async for value in values:
            stat = Statistic(**value)
            stat.sum = int(value["sum"])
            stats.append(stat)
        total = sum([stat.sum for stat in stats])
        buf = None
        if top_users > 0:
            graph_stats = stats[:top_users]
            names = []
            for stat in graph_stats:
                user = interaction.guild.get_member(stat.author_id)
                names.append(user.display_name if user else "Unknown User")
            counts = [stat.sum for stat in graph_stats]
            buf = self.make_bar_graph(
                names, counts, f"{statistic.title_word()} sent in {interaction.guild.name}", "User", statistic.label()
            )
        assert (
            not graph_only
            or buf
            # Represents "if graph only, then a graph must be made" (p->q)
        ), "Somehow requested graph only but no graph was made."
        has_at_least_one_private_channel = next((self.is_private_stat(stat) for stat in stats), None) is not None
        if graph_only:
            return await interaction.followup.send(
                files=[
                    File(
                        buf,
                        filename="graph.png",
                        description="A graph of the top users in the server",
                    )
                ],
                ephemeral=self.is_ephemeral(private_mode, has_at_least_one_private_channel),
            )
        embed = Embed(
            title=f"Statistics for {interaction.guild.name}",
            description=f"Total {statistic.title_word().lower()}: **{total:,}**{' (**' + self.format_byte_string(total) + '**)' if statistic == StatisticMode.characters and total > 1024 else ''}\n",
        )
        for stat in stats[:50]:  # Limit to 50 users listed so we do not go over the embed character limit
            user = interaction.guild.get_member(stat.author_id)
            embed.description += f"**{user.mention if user else 'Unknown User'}**: {stat.sum:,}{' (' + self.format_byte_string(stat.sum) + ')' if statistic == StatisticMode.characters and stat.sum > 1024 else ''}\n"
        if buf:
            embed.set_image(url="attachment://graph.png")
        await interaction.followup.send(
            embed=embed,
            files=[
                File(
                    buf,
                    filename="graph.png",
                    description="A graph of the top users in the server",
                )
            ]
            if buf
            else [],
            ephemeral=self.is_ephemeral(private_mode, has_at_least_one_private_channel),
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
        is_private = StatisticsView.is_private_thread(channel)
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
                if interaction.app_permissions.manage_threads:
                    async for thread in channel.archived_threads(limit=None, private=True):
                        self.queue.put_nowait(RecalculateTask(thread, since_last=since_last))
                else:
                    async for thread in channel.archived_threads(limit=None, private=True, joined=True):
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
                        is_private=StatisticsView.is_private(message.channel),
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
