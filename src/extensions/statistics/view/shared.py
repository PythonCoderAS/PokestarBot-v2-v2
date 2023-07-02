
from discord import (
    Interaction,
    Thread,
    DMChannel,
)

from .errors import ValidationError
from ..shared import SUPPORTED_CHANNEL_TYPES
from ....models.statistic import Statistic
from typing import Optional, cast, Callable
from enum import Enum, auto
from matplotlib.figure import Figure
from io import BytesIO
from datetime import date


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
    aggregate = auto() # This aggregates private threads into their own entry
    aggregate_all = auto()


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



def validate_guild_only(interaction: Interaction, error_message: str):
    """Returns if the interaction is in a guild. If not, raises an exception."""
    if not interaction.guild_id:
        raise ValidationError(error_message)



def validate_before_and_after_dates(
        before_month: Optional[Months] = None,
        before_year: Optional[int] = None,
        after_month: Optional[Months] = None,
        after_year: Optional[int] = None,
):
    """Returns if the before and after dates are valid. If not, throws an exception."""
    if (before_year, before_month).count(None) == 1:
        raise ValidationError(
            "The `before_month` and `before_year` options must be used together.",
        )
    elif (before_year, before_month).count(None) == 2:
        before_date = None
    else:
        assert before_month is not None and before_year is not None
        before_date = date(before_year, cast(int, before_month.value), 1)
    if (after_year, after_month).count(None) == 1:
        raise ValidationError(
            "The `after_month` and `after_year` options must be used together.",
        )
    elif (after_year, after_month).count(None) == 2:
        after_date = None
    else:
        assert after_month is not None and after_year is not None
        after_date = date(after_year, cast(int, after_month.value), 1)
    if after_date and after_date > date.today():
        raise ValidationError("The `after_date` option cannot be in the future.")
    if before_date and after_date:
        if before_date < after_date:
            raise ValidationError(
                "The `before_date` option must be after the `after_date` option.",
            )



def validate_graph_options(
        graph_only: bool, top_items: int, param_name: str
):
    """Returns if the graph options are valid. If not, throws an exception."""
    if not graph_only:
        return True
    if graph_only and top_items <= 0:
        raise ValidationError(
            f"The `{param_name}` option must be greater than 0 when using the `graph_only` option.",
        )
    return validate_guild_only("The `graph_only` option can only be used in a server.")



def is_private(channel: SUPPORTED_CHANNEL_TYPES) -> bool:
    """Returns if the channel or thread is private."""
    if isinstance(channel, DMChannel):
        return True  # DMs are always private
    elif isinstance(channel, Thread):
        return channel.is_private() or is_private(channel.parent)
    elif channel is None:
        return False
    else:
        at_everyone_perms = channel.permissions_for(channel.guild.default_role)
        return not at_everyone_perms.view_channel



def is_private_stat(stat: Statistic) -> bool:
    """Returns if the statistic is private."""
    return stat.is_private if stat.is_private is not None else is_private(stat.target_channel)



def parse_date_options(
        before_month: Optional[Months] = None,
        before_year: Optional[int] = None,
        after_month: Optional[Months] = None,
        after_year: Optional[int] = None,
) -> tuple[Optional[date], Optional[date]]:
    """Returns the parsed date options.

    Return format: (valid, (before_date, after_date))
    """
    validate_before_and_after_dates(
            before_month, before_year, after_month, after_year
    )
    if before_month:
        # We only need to check the month since the validator will check to make sure the year exists as well
        before_date = date(before_year, before_month.value, 1)
    else:
        before_date = None
    if after_month:
        after_date = date(after_year, cast(int, after_month.value), 1)
    else:
        after_date = None
    return before_date, after_date



def make_bar_graph(bar_names: list[str], bar_values: list[str], title: str, x_label: str, y_label: str) -> BytesIO:
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



def get_number_and_unit(num_bytes: int) -> tuple[float, str]:
    """Returns the number of bytes in a human-readable format. Round the final result to 2 decimal places."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num_bytes < 1024.0:
            return round(num_bytes, 2), unit
        num_bytes /= 1024.0
    return round(num_bytes, 2), "PB"



def format_byte_string(num_bytes: int) -> str:
    """Returns the number of bytes in a human-readable format."""
    num, unit = get_number_and_unit(num_bytes)
    return f"{num} {unit}"



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

def aggregate_threads(stats: list[dict], condition: Callable[[dict], bool]=lambda item: item["is_private"]) -> list[dict]:
    """Aggregate all thread statistics into one common object."""
    thread_stats = {}
    non_thread_stats = []
    for item in stats:
        if item.get("thread_id", None) is None or not condition(item):
            non_thread_stats.append(item)
            continue
        copy = item.copy()
        copy.pop("thread_id")
        key = tuple([item[1] for item in sorted(copy.items(), key=lambda item: item[0])]) # Sorts all the keys based on the key name and then returns a tuple of all the values
        if key not in thread_stats:
            thread_stats[key] = item
        else:
            thread_stats[key]["sum"] += item["sum"]
            thread_stats[key]["agg_count"] = thread_stats[key].get("agg_count", 0) + 1
    return list(thread_stats.values()) + non_thread_stats

def format_stat(stat_value: int, statistic: StatisticMode):
    return f"**{stat_value:,}**{' (**' + format_byte_string(stat_value) + '**)' if statistic == StatisticMode.characters and stat_value > 1024 else ''}"

def format_stat_graph_name(stat: Statistic, private_mode: PrivateMode) -> str:
    if stat.target_channel is None:
        return "Deleted/unknown channel or thread"
    elif stat.thread_id is not None and (((private_mode == PrivateMode.aggregate and is_private_stat(stat))) or (private_mode == PrivateMode.aggregate_all)):
        if ((private_mode == PrivateMode.aggregate and is_private_stat(stat))):
            return "All private threads in #" + stat.channel.name
        elif (private_mode == PrivateMode.aggregate_all):
            return "All threads in #" + stat.channel.name
        else:
            raise ValueError(f"Invalid set of options: {(stat.thread_id, private_mode)!r}")
    elif (not is_private_stat(stat) or private_mode != PrivateMode.hide_name):
        return stat.target_channel.name
    else:
        return "Private channel or thread"

def format_stat_embed_label(stat: Statistic, private_mode: PrivateMode) -> str:
    if stat.thread_id:
        if stat.is_private and private_mode == PrivateMode.aggregate:
            return f"All private threads in <#{stat.channel_id}>"
        elif private_mode == PrivateMode.aggregate_all:
            return f"All threads in <#{stat.channel_id}>"
    return f"<#{stat.target_channel_id}>"
