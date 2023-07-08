from typing import TYPE_CHECKING, Optional
from discord import TextChannel
from discord.ext.commands import GroupCog
from discord.app_commands import command, guild_only, Group
from discord.app_commands.checks import has_permissions
from ..settings import BooruChannel as BooruChannelDataclass

if TYPE_CHECKING:
    from ..bot import PokestarBot, BotInteraction


@has_permissions(create_public_threads=True, read_message_history=True)
class BooruChannel(
    Group,
    name="booru_channel",
    description="Configure booru channels.",
):
    def __init__(self, bot: Optional["PokestarBot"] = None):
        super().__init__()
        self.bot = bot

    @command(name="add", description="Add a booru channel.")
    async def add(self, interaction: "BotInteraction", channel: TextChannel):
        settings = self.bot.settings.get(interaction.guild)
        if channel.id in [x.channel_id for x in settings.booru_channels]:
            return await interaction.response.send_message("This channel is already a booru channel!", ephemeral=True)
        booru_channel = BooruChannelDataclass(channel.id)
        settings.booru_channels.append(booru_channel)
        await interaction.response.send_message(f"Added {channel.mention} as a booru channel.")
        await self.bot.settings.save_guild(interaction.guild)

    @command(name="remove", description="Remove a booru channel.")
    async def remove(self, interaction: "BotInteraction", channel: TextChannel):
        settings = self.bot.settings.get(interaction.guild)
        if channel.id not in [x.channel_id for x in settings.booru_channels]:
            return await interaction.response.send_message("This channel is not a booru channel!", ephemeral=True)
        settings.booru_channels = [x for x in settings.booru_channels if x.channel_id != channel.id]
        await interaction.response.send_message(f"Removed {channel.mention} as a booru channel.")
        await self.bot.settings.save_guild(interaction.guild)


@guild_only()
class Settings(GroupCog, group_name="settings", description="Set server settings."):
    booru_channel = BooruChannel()

    def __init__(self, bot: "PokestarBot"):
        super().__init__()
        self.bot = bot
        self.booru_channel.bot = bot


async def setup(bot: "PokestarBot"):
    return await bot.add_cog(Settings(bot))
