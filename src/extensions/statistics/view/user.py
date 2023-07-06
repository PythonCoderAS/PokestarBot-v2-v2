from datetime import date
from typing import Optional

from discord import Embed, Member, Interaction
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
    CAN_SHOW_PRIVATE_CHANNELS_SAFELY,
)
from .base import BaseStatisticsViewHandler, ValidationError


class UserHandler(BaseStatisticsViewHandler):
    member: Optional[Member]
    top_channels: int
    include_threads: bool
    private_mode: PrivateMode

    def __init__(self, interaction: Interaction, **kwargs):
        super().__init__(interaction, **kwargs)
        if self.member is None:
            self.member = interaction.user

    async def validate(self):
        user_id = self.interaction.user.id
        validate_guild_only(self.interaction, "The `member` option can only be used in a server.")
        if self.member.id != user_id:
            if not self.interaction.user.guild_permissions.administrator:
                # Only admins can view other users' full stats
                if self.private_mode not in CAN_SHOW_PRIVATE_CHANNELS_SAFELY:
                    raise ValidationError(
                        "You must be an administrator to view other users' full private threads.",
                    )
        if self.include_threads:
            validate_guild_only(self.interaction, "The `include_threads` options can only be used in a server.")
        validate_graph_options(self.graph_only, self.top_channels, "top_channels")

    def get_queryset(self, *, before_date: Optional[date], after_date: Optional[date]) -> ValuesQuery:
        base = (
            super()
            .get_queryset(before_date=before_date, after_date=after_date)
            .filter(author_id=self.member.id, guild_id=self.interaction.guild_id)
        )
        if not self.include_threads:
            base = base.filter(thread_id=None)
        return base.group_by("guild_id", "channel_id", "thread_id", "author_id", "is_private").values(
            "guild_id", "channel_id", "thread_id", "author_id", "sum", "is_private"
        )

    async def get_graph_data(self, stats: list[Statistic]) -> tuple[list[str], list[int], str, str, str]:
        if self.private_mode == PrivateMode.hide:
            top_channels_stats_pool = [stat for stat in stats if not stat.is_private]
        else:
            top_channels_stats_pool = stats
        graph_stats = top_channels_stats_pool[: self.top_channels]
        names = [format_stat_graph_name(stat, self.private_mode, self.interaction.user.id) for stat in graph_stats]
        counts = [stat.sum for stat in graph_stats]
        return (
            names,
            counts,
            f"{self.statistic.title_word()} sent by {self.member.display_name}",
            "Channel/Thread",
            self.statistic.label(),
        )

    def can_make_graph(self) -> bool:
        return not (self.top_channels <= 0 or not self.interaction.guild_id)

    def make_embed(self, stats: list[Statistic]) -> Embed:
        total = sum(stat.sum for stat in stats)
        embed = Embed(
            title=f"Statistics for {self.member.display_name}",
            description=f"Total {self.statistic.title_word().lower()}: {format_stat(total, self.statistic)}\n",
        )
        for stat in stats[:50]:  # Limit to 50 channels listed so we do not go over the embed character limit
            label = format_stat_embed_label(stat, self.private_mode)
            embed.description += f"**{label}**: {format_stat(stat.sum, self.statistic)}\n"
        if self.interaction.guild_id:
            total_channels = len({item.channel_id for item in stats})
            total_channels_sum = sum([item.sum for item in stats if not item.thread_id])
            total_threads = len({item.thread_id for item in stats if item.thread_id}) + sum(
                item.agg_count for item in stats if item.thread_id
            )
            total_threads_sum = sum([item.sum for item in stats if item.thread_id])
            embed.description += (
                f"\nTotal channels: **{total_channels}** "
                f"channels, **{total_channels_sum:,}** {self.statistic.title_word().lower()}"
            )
            if self.include_threads:
                embed.description += (
                    f"\nTotal threads: **{total_threads}** threads, "
                    f"**{total_threads_sum,}** {self.statistic.title_word().lower()}"
                )
                embed.description += (
                    f"\nTotal channels and threads: **{total_channels + total_threads}** distinct channels & threads"
                )
        embed.description = embed.description.strip()
        return embed
