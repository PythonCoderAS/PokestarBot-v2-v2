from tortoise.expressions import Q
from tortoise.functions import Sum
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

from .shared import (
    PrivateMode,
    StatisticMode,
    Months,
    validate_graph_options,
    validate_guild_only,
    parse_date_options,
    BotModes,
    LimitedPrivateMode,
    is_private_stat,
)
from .user import UserHandler
from .channel import ChannelHandler
from .threads import ThreadsHandler
from ..shared import SUPPORTED_CHANNEL_TYPES, SUPPORTED_COMMAND_CHANNEL_TYPES
from ....models.statistic import Statistic
from typing import Optional, Union, cast
from enum import Enum, auto


class StatisticsView(Group, name="view", description="View statistics information."):
    @command(name="user", description="View user statistics.")
    async def user(
        self,
        interaction: Interaction,
        member: Optional[Member],
        top_channels: Range[int, 0, 25] = 10,
        graph_only: bool = False,
        include_threads: bool = True,
        private_mode: PrivateMode = PrivateMode.aggregate,
        before_month: Optional[Months] = None,
        before_year: Optional[Range[int, 2015]] = None,
        after_month: Optional[Months] = None,
        after_year: Optional[Range[int, 2015]] = None,
        statistic: StatisticMode = StatisticMode.messages,
        hidden: bool = False,
    ):
        return await UserHandler(
            interaction,
            member=member,
            top_channels=top_channels,
            graph_only=graph_only,
            include_threads=include_threads,
            private_mode=private_mode,
            before_month=before_month,
            before_year=before_year,
            after_month=after_month,
            after_year=after_year,
            statistic=statistic,
            hidden=hidden,
        ).run()

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
        hidden: bool = False,
    ):
        return await ChannelHandler(
            interaction,
            channel=channel,
            top_users=top_users,
            graph_only=graph_only,
            include_threads=include_threads,
            bots=bots,
            private_mode=private_mode,
            before_month=before_month,
            before_year=before_year,
            after_month=after_month,
            after_year=after_year,
            statistic=statistic,
            hidden=hidden,
        ).run()

    @command(name="active_threads", description="View statistics for active threads in a channel.")
    async def active_threads(
        self,
        interaction: Interaction,
        channel: Optional[Union[TextChannel, ForumChannel]] = None,
        top_threads: Range[int, 0, 25] = 10,
        graph_only: bool = False,
        private_mode: PrivateMode = PrivateMode.hide_name,
        bots: BotModes = "exclude",
        before_month: Optional[Months] = None,
        before_year: Optional[Range[int, 2015]] = None,
        after_month: Optional[Months] = None,
        after_year: Optional[Range[int, 2015]] = None,
        statistic: StatisticMode = StatisticMode.messages,
        hidden: bool = False,
    ):
        return await ThreadsHandler(
            interaction,
            channel=channel,
            top_threads=top_threads,
            graph_only=graph_only,
            private_mode=private_mode,
            bots=bots,
            before_month=before_month,
            before_year=before_year,
            after_month=after_month,
            after_year=after_year,
            statistic=statistic,
            hidden=hidden,
        ).run()

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
        if not await validate_guild_only(interaction, "The command can only be used in a server."):
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
        if not await validate_graph_options(interaction, graph_only, top_channels, "top_channels"):
            return
        date_valid, (before_date, after_date) = await parse_date_options(
            interaction, before_month, before_year, after_month, after_year
        )
        if not date_valid:
            return
        await interaction.response.defer(thinking=True)
        valid_channel_ids = set()
        base = (
            Statistic.filter(guild_id=interaction.guild_id).annotate(sum=Sum(statistic.field_name())).order_by("-sum")
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
            values = base.values("guild_id", "channel_id", "sum", "is_private")
        else:
            base = base.group_by("guild_id", "channel_id", "thread_id", "is_private")
            values = base.values("guild_id", "channel_id", "thread_id", "sum", "is_private")
        stats = []
        async for value in values:
            stat = Statistic(**value)
            stat.sum = int(value["sum"])
            stats.append(stat)
        if private_mode == PrivateMode.exclude:
            stats = [stat for stat in stats if not is_private_stat(stat)]
        total = sum([stat.sum for stat in stats])
        buf = None
        if not (top_channels <= 0 or not interaction.guild_id):  # Skip graph if set to 0
            if private_mode == PrivateMode.hide:
                top_channels_stats_pool = [stat for stat in stats if not is_private_stat(stat)]
            else:
                top_channels_stats_pool = stats
            graph_stats = top_channels_stats_pool[:top_channels]
            names = [
                stat.target_channel.name
                if (not is_private_stat(stat) or private_mode != PrivateMode.hide_name)
                else "**Private Channel or Thread**"
                for stat in graph_stats
            ]
            counts = [stat.sum for stat in graph_stats]
            buf = make_bar_graph(
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
        has_at_least_one_private_channel = next((is_private_stat(stat) for stat in stats), None) is not None
        if graph_only:
            return await interaction.followup.send(
                files=[
                    File(
                        buf,
                        filename="graph.png",
                        description="A graph of the top channels in the server",
                    )
                ],
                ephemeral=is_ephemeral(private_mode, has_at_least_one_private_channel),
            )
        embed = Embed(
            title=f"Statistics for {interaction.guild.name}",
            description=f"Total {statistic.title_word().lower()}: **{total:,}**{' (**' + format_byte_string(total) + '**)' if statistic == StatisticMode.characters and total > 1024 else ''}\n",
        )
        for stat in stats[:50]:  # Limit to 50 channels listed so we do not go over the embed character limit
            embed.description += f"**<#{stat.target_channel_id}>**: {stat.sum:,}{' (' + format_byte_string(stat.sum) + ')' if statistic == StatisticMode.characters and stat.sum > 1024 else ''}\n"
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
            ephemeral=is_ephemeral(private_mode, has_at_least_one_private_channel),
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
        if not await validate_guild_only(interaction, "The command can only be used in a server."):
            return
        date_valid, (before_date, after_date) = await parse_date_options(
            interaction, before_month, before_year, after_month, after_year
        )
        if not date_valid:
            return
        if not await validate_graph_options(interaction, graph_only, top_users, "top_users"):
            return
        await interaction.response.defer(thinking=True)
        base = (
            Statistic.filter(guild_id=interaction.guild_id).annotate(sum=Sum(statistic.field_name())).order_by("-sum")
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
            buf = make_bar_graph(
                names, counts, f"{statistic.title_word()} sent in {interaction.guild.name}", "User", statistic.label()
            )
        assert (
            not graph_only
            or buf
            # Represents "if graph only, then a graph must be made" (p->q)
        ), "Somehow requested graph only but no graph was made."
        has_at_least_one_private_channel = next((is_private_stat(stat) for stat in stats), None) is not None
        if graph_only:
            return await interaction.followup.send(
                files=[
                    File(
                        buf,
                        filename="graph.png",
                        description="A graph of the top users in the server",
                    )
                ],
                ephemeral=is_ephemeral(private_mode, has_at_least_one_private_channel),
            )
        embed = Embed(
            title=f"Statistics for {interaction.guild.name}",
            description=f"Total {statistic.title_word().lower()}: **{total:,}**{' (**' + format_byte_string(total) + '**)' if statistic == StatisticMode.characters and total > 1024 else ''}\n",
        )
        for stat in stats[:50]:  # Limit to 50 users listed so we do not go over the embed character limit
            user = interaction.guild.get_member(stat.author_id)
            embed.description += f"**{user.mention if user else 'Unknown User'}**: {stat.sum:,}{' (' + format_byte_string(stat.sum) + ')' if statistic == StatisticMode.characters and stat.sum > 1024 else ''}\n"
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
            ephemeral=is_ephemeral(private_mode, has_at_least_one_private_channel),
        )
