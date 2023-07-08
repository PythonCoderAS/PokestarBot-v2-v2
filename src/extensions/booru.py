from discord.ext.commands import Cog
from discord import Message, Thread, AllowedMentions
from asyncio import Lock
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..bot import PokestarBot


class Booru(Cog):
    def __init__(self, bot: "PokestarBot"):
        self.bot = bot
        self.lock = Lock()

    @Cog.listener()
    async def on_message(self, message: Message):
        if not isinstance(message.channel, Thread):
            return
        if message.channel.is_private():
            return
        if len(message.attachments) == 0:
            return
        if message.channel.parent.id not in [x.id for x in self.bot.settings.get(message.guild).booru_channels]:
            return
        async with self.lock:
            label = "an image" if len(message.attachments) == 1 else f"{len(message.attachments)} images"
            await message.channel.parent.send(
                f"{message.author.mention} posted {label} in {message.channel.mention}: {message.jump_url}",
                allowed_mentions=AllowedMentions.none(),
            )
            if len(message.attachments) == 1:
                await message.channel.parent.send(message.attachments[0].url)


async def setup(bot: "PokestarBot"):
    return await bot.add_cog(Booru(bot))
