from abc import ABC, abstractmethod
from datetime import date
from typing import Optional

from discord import Interaction, Embed, File
from tortoise.functions import Sum
from tortoise.queryset import QuerySet, ValuesQuery

from ....models import Statistic
from .shared import Months, StatisticMode, parse_date_options, make_bar_graph, is_private_stat, PrivateMode, \
    LimitedPrivateMode, is_ephemeral, aggregate_threads, ALL_PRIVATE_AGGREGATE, ALL_AGGREGATE
from .errors import ValidationError



class BaseStatisticsViewHandler(ABC):
    statistic: StatisticMode
    before_month: Months
    before_year: int
    after_month: Months
    after_year: int
    graph_only: bool
    private_mode: PrivateMode | LimitedPrivateMode

    def __init__(self, interaction: Interaction, **kwargs):
        self.interaction = interaction
        for key, value in kwargs.items():
            setattr(self, key, value)

    @abstractmethod
    async def validate(self):
        """
        Validate the view.

        In order to signify a validation error, throw an instance of :class:`ValidationError`.

        Do not validate the before/after month/year here. That is done in :meth:`run`.
        """
        pass

    def get_queryset(self, *, before_date: Optional[date], after_date: Optional[date]) -> QuerySet | ValuesQuery:
        base = Statistic.annotate(sum=Sum(self.statistic.field_name()))
        if before_date:
            base = base.filter(month__lte=before_date)
        if after_date:
            base = base.filter(month__gte=after_date)
        return base

    async def filter_stats(self, stats: list[Statistic]) -> list[Statistic]:
        return stats

    @abstractmethod
    async def get_graph_data(self, stats: list[Statistic]) -> tuple[list[str], list[int], str, str, str]:
        """Return: (bar_names: list[str], bar_values: list[str], title: str, x_label: str, y_label: str)"""
        pass

    @abstractmethod
    def can_make_graph(self) -> bool:
        return True

    @abstractmethod
    def make_embed(self, stats: list[Statistic]) -> Embed:
        pass

    async def get_stats(self, *, before_date: date, after_date: date) -> list[Statistic]:
        queryset = self.get_queryset(before_date=before_date, after_date=after_date)
        stats = []
        values = [value async for value in queryset]
        if self.private_mode in ALL_AGGREGATE:
            condition = (lambda item: item["is_private"]) if self.private_mode in ALL_PRIVATE_AGGREGATE else (lambda item: True)
            values = aggregate_threads(values, condition=condition)
        for value in values:
            stat = Statistic(**value)
            stat.sum = int(value["sum"])
            stat.agg_count = int(value.get("agg_count", 0)) # agg_count is generated by the aggregate_threads function and it tracks the # of aggregated threads
            stats.append(stat)
        stats.sort(key=lambda item: item.sum, reverse=True)
        return stats


    async def run(self):
        try:
            await self.validate()
            before_date, after_date = parse_date_options(
                self.before_month, self.before_year, self.after_month, self.after_year
            )
        except ValidationError as e:
            return await self.interaction.response.send_message(e.message, ephemeral=True, embeds=e.embeds)
        await self.interaction.response.defer(thinking=True)
        stats = await self.get_stats(before_date=before_date, after_date=after_date)
        stats = await self.filter_stats(stats)
        file = None
        if self.can_make_graph():
            bar_names, bar_values, title, x_label, y_label = await self.get_graph_data(stats)
            buf = make_bar_graph(bar_names, bar_values, title, x_label, y_label)
            file = File(
                        buf,
                        filename="graph.png",
                        description="A graph of the top channels the user commented in",
                    )
        has_at_least_one_private_channel = next((is_private_stat(stat) for stat in stats), None) is not None
        ephemeral = is_ephemeral(self.private_mode, has_at_least_one_private_channel)
        if self.graph_only:
            return await self.interaction.followup.send(
                files=[
                    file
                ],
                ephemeral=ephemeral,
            )
        embed = self.make_embed(stats)
        if file:
            embed.set_image(url="attachment://graph.png")
        await self.interaction.followup.send(
            embed=embed,
            files=[
                file
            ]
            if file
            else [],
            ephemeral=ephemeral,
        )




