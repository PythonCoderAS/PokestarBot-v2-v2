from datetime import date
from typing import Optional

from discord import Embed, Member, Interaction, Thread, NotFound
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


class ChannelHandler(BaseStatisticsViewHandler):
    channel: Optional[SUPPORTED_COMMAND_CHANNEL_TYPES] = (None,)
    top_users: int
    include_threads: bool
    bots: BotModes
    private_mode: LimitedPrivateMode

    def __init__(self, interaction: Interaction, **kwargs):
        super().__init__(interaction, **kwargs)
        if self.channel is None:
            self.channel = interaction.channel

    async def validate(self):
        user_id = self.interaction.user.id
        if self.include_threads:
            validate_guild_only(self.interaction, "The `include_threads` options can only be used in a server.")
        validate_graph_options(self.graph_only, self.top_users, "top_users")
        if isinstance(self.channel, Thread):
            if self.channel.is_private():
                try:
                    await self.channel.fetch_member(user_id)
                except NotFound:
                    raise ValidationError("You do not have permission to view that thread.")
        else:
            if not self.channel.permissions_for(self.interaction.user).view_channel:
                raise ValidationError("You do not have permission to view that channel.")

    def get_queryset(self, *, before_date: Optional[date], after_date: Optional[date]) -> ValuesQuery:
        base = super().get_queryset(before_date=before_date, after_date=after_date)
        if isinstance(self.channel, Thread):
            self.include_threads = True
            base = base.filter(
                channel_id=self.channel.parent.id, thread_id=self.channel.id, guild_id=self.interaction.guild_id
            )
        else:
            base = base.filter(channel_id=self.channel.id, guild_id=self.interaction.guild_id)
        if not self.include_threads:
            base = base.filter(thread_id=None)
        base = filter_bot_mode(base, self.bots)
        return base.group_by("guild_id", "channel_id", "thread_id", "author_id").values(
            "guild_id",
            "channel_id",
            "thread_id",
            "author_id",
            "sum",
        )

    async def get_graph_data(self, stats: list[Statistic]) -> tuple[list[str], list[int], str, str, str]:
        graph_stats = stats[: self.top_users]
        names = []
        for stat in graph_stats:
            user = self.interaction.guild.get_member(stat.author_id)
            names.append(user.display_name if user else "Unknown User")
        counts = [stat.sum for stat in graph_stats]
        return (
            names,
            counts,
            f"{self.statistic.title_word()} sent in {self.channel.name}",
            "User",
            self.statistic.label(),
        )

    def can_make_graph(self) -> bool:
        return self.top_users > 0

    def make_embed(self, stats: list[Statistic]) -> Embed:
        total = sum(stat.sum for stat in stats)
        unique_users = len({stat.author_id for stat in stats})
        embed = Embed(
            title=f"Statistics for {self.channel.name}",
            description=f"Total {self.statistic.title_word().lower()}: {format_stat(total, self.statistic)}\nTotal unique users: **{unique_users}**\n",
        )
        for stat in stats[:50]:  # Limit to 50 users listed so we do not go over the embed character limit
            user = self.interaction.guild.get_member(stat.author_id)
            label = user.mention if user else "Unknown User"
            embed.description += f"**{label}**: {format_stat(stat.sum, self.statistic)}\n"
        embed.description = embed.description.strip()
        return embed
