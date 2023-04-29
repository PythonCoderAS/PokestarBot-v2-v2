from discord.ext.commands import GroupCog, Bot
from discord.app_commands import Group, command, Range
from discord import Interaction, Member, Embed, File, Message, Thread, TextChannel, ForumChannel, VoiceChannel, StageChannel
from collections import defaultdict
from asyncio import Lock
from ..models.statistic import Statistic
from typing import Optional, Union
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from io import BytesIO

class StatisticsView(Group, name="view", description="View statistics information."):
    @command(name="user", description="View user statistics.")
    async def user(self, interaction: Interaction, member: Optional[Member], top_channels: Range[int, 0, 25] = 10):
        if member is None:
            member = interaction.user
            user_id = interaction.user.id
        else:
            if not interaction.guild_id:
                return await interaction.response.send_message("The `member` option can only be used in a server.", ephemeral=True)
            user_id = member.id
        await interaction.response.defer(thinking=True)
        stats = await Statistic.filter(author_id=user_id, guild_id=interaction.guild_id).order_by("-messages").all()
        total = sum([stat.messages for stat in stats])
        buf = BytesIO()
        made_graph = False
        if not (top_channels <= 0 or not interaction.guild_id): # Skip graph if set to 0
            graph_stats = stats[:top_channels]
            names = [stat.thread.name if stat.thread else stat.channel.name for stat in graph_stats]
            counts = [stat.messages for stat in graph_stats]
            fig, ax = plt.subplots()
            ax.barh(names, counts)
            ax.set_xlabel('# of Messages')
            ax.set_ylabel('Channel/Thread')
            ax.set_title(f'Messages sent by {member.display_name}')
            fig.tight_layout()
            fig.savefig(buf, format='png', bbox_inches = 'tight')
            buf.seek(0)
            plt.clf()
            made_graph = True
        embed = Embed(title=f"Statistics for {member.display_name}", description=f"Total messages: **{total:,}**\n")
        for stat in stats[:50]: # Limit to 50 channels listed so we do not go over the embed character limit
            embed.description += f"**<#{stat.target_channel_id}>**: {stat.messages:,}\n"
        if interaction.guild_id:
            embed.description += f"\nTotal channels: **{len([item for item in stats if not item.thread_id])}** channels, **{sum([item.messages for item in stats if not item.thread_id]):,} messages**"
            embed.description += f"\nTotal threads: **{len([item for item in stats if item.thread_id])}** threads, **{sum([item.messages for item in stats if item.thread_id]):,} messages**"
            embed.description += f"\nTotal channels and threads: **{len(stats)}** distinct channels & threads"
        embed.description = embed.description.strip()
        if made_graph:
            embed.set_image(url="attachment://graph.png")
        await interaction.followup.send(embed=embed, files=[File(buf, filename="graph.png", description="A graph of the top channels the user commented in")] if made_graph else [])

class StatisticsRecalculate(Group, name="recalculate", description="Recalculate statistics information."):
    def __init__(self, statistics: Optional["Statistics"], **kwargs):
        self.statistics = statistics
        super().__init__(**kwargs)

    async def recalculate_channel(self, channel: Union[VoiceChannel, ForumChannel, TextChannel, Thread, StageChannel]):
        kwargs = {}
        kwargs["guild_id"] = channel.guild.id if channel.guild else None
        message_count: defaultdict[int, int] = defaultdict(int)
        if isinstance(channel, Thread):
            kwargs["thread_id"] = channel.id
            kwargs["channel_id"] = channel.parent.id
        else:
            kwargs["channel_id"] = channel.id
        async with self.statistics.locks[channel.id]:
            async for message in channel.history(limit=None):
                message_count[message.author.id] += 1
            await Statistic.filter(**kwargs).delete()
            await Statistic.bulk_create([Statistic(author_id=author_id, messages=count, **kwargs) for author_id, count in message_count.items()])

    @command(name="channel", description="Recalculate channel statistics.")
    async def channel(self, interaction: Interaction, channel: Optional[Union[VoiceChannel, ForumChannel, TextChannel, Thread, StageChannel]] = None, include_threads: bool = False, only_threads: bool = False):
        bot: Bot = interaction.client
        if only_threads:
            include_threads = True
        if not await bot.is_owner(interaction.user):
            return await interaction.response.send_message("Only the bot owner can use this command.", ephemeral=True)
        if channel is None:
            channel = interaction.channel
        await interaction.response.send_message(f"Queued recalculation for {channel.mention}.", ephemeral=True)
        if isinstance(channel, ForumChannel):
            include_threads = True
        else:
            if not only_threads:
                await self.recalculate_channel(channel)
        if include_threads and isinstance(channel, (TextChannel, ForumChannel)):
            for thread in channel.threads:
                await self.recalculate_channel(thread)
            async for thread in channel.archived_threads(limit=None, private=False):
                await self.recalculate_channel(thread)
            if interaction.app_permissions.manage_threads:
                async for thread in channel.archived_threads(limit=None, private=True):
                    await self.recalculate_channel(thread)
            else:
                async for thread in channel.archived_threads(limit=None, private=True, joined=True):
                    await self.recalculate_channel(thread)


class Statistics(GroupCog, group_name="statistics", description="View statistics information."):
    view = StatisticsView()
    recalculate = StatisticsRecalculate(None)
    def __init__(self):
        # We lock per (channel, author) to ensure that the same object isn't modified twice
        self.locks: defaultdict[tuple[int, int], Lock] = defaultdict(Lock)
        self.recalculate.statistics = self


    @GroupCog.listener()
    async def on_message(self, message: Message):
        async with self.locks[message.channel.id]:
            if isinstance(message.channel, Thread):
                existing = await Statistic.filter(author_id=message.author.id, thread_id=message.channel.id).first()
                if existing is None:
                    await Statistic.create(author_id=message.author.id, guild_id=message.guild.id, thread_id=message.channel.id, channel_id=message.channel.parent.id, messages=1)
                else:
                    existing.messages += 1
                    await existing.save()
            else:
                existing = await Statistic.filter(author_id=message.author.id, channel_id=message.channel.id, thread_id=None).first()
                if existing is None:
                    await Statistic.create(author_id=message.author.id, guild_id=message.guild.id if message.guild else None, channel_id=message.channel.id, messages=1)
                else:
                    existing.messages += 1
                    await existing.save()


async def setup(bot: Bot):
    await bot.add_cog(Statistics())