from datetime import date
from typing import Optional, Union

from discord import Embed, Member, Interaction, Thread, NotFound, ForumChannel, TextChannel
from tortoise.queryset import QuerySet, ValuesQuery

from src.models import Statistic
from .shared import (
    PrivateMode,
    validate_guild_only,
    validate_graph_options,
    is_private_stat,
    format_stat,
    format_stat_graph_name,
    format_stat_embed_label,
    LimitedPrivateMode,
    BotModes,
    filter_bot_mode,
)
from .base import BaseStatisticsViewHandler, ValidationError
from ..shared import SUPPORTED_COMMAND_CHANNEL_TYPES


class ThreadsHandler(BaseStatisticsViewHandler):
    channel: Optional[Union[TextChannel, ForumChannel]]
    top_threads: int
    bots: BotModes
    private_mode: LimitedPrivateMode

    def __init__(self, interaction: Interaction, **kwargs):
        super().__init__(interaction, **kwargs)
        if self.channel is None:
            self.channel = interaction.channel
        if isinstance(self.channel, Thread):
            self.channel = self.channel.parent

    async def validate(self):
        validate_guild_only(self.interaction, "Threads only exist in a server")
        validate_graph_options(self.graph_only, self.top_threads, "top_threads")
        perms = self.channel.permissions_for(self.interaction.user)
        if not perms.view_channel and perms.read_message_history:
            raise ValidationError("You do not have permission to view threads in that channel.")
        if not isinstance(self.channel, (TextChannel, ForumChannel)):
            raise ValidationError("You can only view threads in a text channel.")

    def get_queryset(self, *, before_date: Optional[date], after_date: Optional[date]) -> ValuesQuery:
        base = super().get_queryset(before_date=before_date, after_date=after_date)
        threads = [thread.id for thread in self.channel.threads]
        base = base.filter(channel_id=self.channel.id, guild_id=self.interaction.guild_id, thread_id__in=threads)
        base = filter_bot_mode(base, self.bots)
        return base.group_by("guild_id", "channel_id", "thread_id").values(
            "guild_id",
            "channel_id",
            "thread_id",
            "sum",
        )

    async def get_graph_data(self, stats: list[Statistic]) -> tuple[list[str], list[int], str, str, str]:
        if self.private_mode == PrivateMode.hide:
            top_threads_stats_pool = [stat for stat in stats if not stat.is_private]
        else:
            top_threads_stats_pool = stats
        graph_stats = top_threads_stats_pool[: self.top_threads]
        names = [format_stat_graph_name(stat, self.private_mode, self.interaction.user.id) for stat in graph_stats]
        counts = [stat.sum for stat in graph_stats]
        return (
            names,
            counts,
            f"{self.statistic.title_word()} in Threads in {self.channel.name}",
            "Thread",
            self.statistic.label(),
        )

    def can_make_graph(self) -> bool:
        return self.top_threads > 0

    def make_embed(self, stats: list[Statistic]) -> Embed:
        total = sum(stat.sum for stat in stats)
        unique_threads = len({stat.thread_id for stat in stats})
        embed = Embed(
            title=f"Statistics for Threads in {self.channel.name}",
            description=f"Total {self.statistic.title_word().lower()}: {format_stat(total, self.statistic)}\nTotal unique threads: **{unique_threads}**\n",
        )
        for stat in stats[:50]:  # Limit to 50 users listed so we do not go over the embed character limit
            label = format_stat_embed_label(stat, self.private_mode)
            embed.description += f"**{label}**: {format_stat(stat.sum, self.statistic)}\n"
        embed.description = embed.description.strip()
        return embed
