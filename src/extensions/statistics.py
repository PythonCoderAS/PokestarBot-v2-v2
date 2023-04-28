from discord.ext.commands import GroupCog, Bot
from discord.app_commands import Group, command, guild_only
from discord import Interaction, Member, Embed, File
from ..models.statistic import Statistic
from typing import Optional
import matplotlib.pyplot as plt
from io import BytesIO

class StatisticsView(Group, name="view", description="View statistics information."):
    @command(name="user", description="View user statistics.")
    async def user(self, interaction: Interaction, member: Optional[Member], top_channels: Optional[int] = 10):
        if member is None:
            user_id = interaction.user.id
        else:
            if not interaction.guild_id:
                return await interaction.response.send_message("The `member` option can only be used in a server.", ephemeral=True)
            user_id = member.id
        await interaction.response.defer(thinking=True)
        stats = await Statistic.filter(author_id=user_id, guild_id=interaction.guild_id)
        total = sum([stat.messages for stat in stats])
        buf = BytesIO()
        made_graph = False
        if top_channels <= 0 or not interaction.guild_id: # Skip graph if set to 0
            graph_stats = stats[:top_channels]
            names = [stat.thread.name if stat.thread else stat.channel.name for stat in graph_stats]
            counts = [stat.messages for stat in graph_stats]
            fig, ax = plt.subplots()
            ax.barh(names, counts)
            ax.set_xlabel('Messages')
            ax.set_ylabel('Channel')
            ax.set_title(f'Messages sent by {member.display_name}')
            fig.tight_layout()
            fig.savefig(buf, format='png')
            buf.seek(0)
            plt.rese
            made_graph = True
        embed = Embed(title=f"Statistics for {member.display_name}", description=f"Total messages: **{total[0] if total else 0:,}**\n")
        for stat in stats:
            embed.description += f"**{stat.target_channel.mention}**: {stat.messages:,}\n"
        if interaction.guild_id:
            embed.description += f"\nTotal channels: **{len(item for item in stats if not item.thread_id)}** channels, **{sum([item.messages for item in stats if not item.thread_id]):,} messages**"
            embed.description += f"\nTotal threads: **{len(item for item in stats if item.thread_id)}** threads, **{sum([item.messages for item in stats if item.thread_id]):,} messages**"
            embed.description += f"\nTotal channels and threads: **{len(stats)}** distinct channels & threads"
        embed.description = embed.description.strip()
        if made_graph:
            embed.set_image(url="attachment://graph.png")
        await interaction.followup.send(embed=embed, files=[File(buf, filename="graph.png", description="A graph of the top channels the user commented in")] if made_graph else None)

class Statistics(GroupCog, name="statistics", description="View statistics information."):
    view = StatisticsView()

async def setup(bot: Bot):
    await bot.add_cog(Statistics(bot))