from discord.app_commands import AppCommandError, CommandInvokeError
from discord.ext.commands import Bot, when_mentioned
from discord import Intents, MemberCacheFlags, Interaction, Embed
from pkgutil import iter_modules
from pathlib import Path
from .models import init as init_models
from .models.mixins import BotMixin
from .settings import ServerSettings
from .singleton import SingletonClass
from traceback import format_exception, print_exception


class PokestarBot(Bot, SingletonClass):
    def __init__(self, *args, **kwargs):
        intents = Intents.default()
        intents.members = True
        intents.message_content = True
        kwargs["intents"] = intents
        kwargs["member_cache_flags"] = kwargs.get("member_cache_flags", MemberCacheFlags.all())
        kwargs["chunk_guilds_at_startup"] = kwargs.get("chunk_guilds_at_startup", True)
        super().__init__(when_mentioned, *args, **kwargs)
        BotMixin.bot = self

        self.slash_command_error = self.tree.error(self.slash_command_error)
        self.settings = ServerSettings(self)

    async def slash_command_error(self, interaction: Interaction, error: AppCommandError):
        if interaction.response.is_done():
            method = interaction.followup.send
        else:
            method = interaction.response.send_message
        if isinstance(error, CommandInvokeError):
            printed_error = error.original
        else:
            printed_error = error
        if not await self.is_owner(interaction.user):
            await method("An error has occured! Ask the bot operator for help.", ephemeral=True)
        else:
            traceback_text = f"```python\n{''.join(format_exception(printed_error))}\n```"
            if len(traceback_text) > 4096:
                await method("An error has occured and the traceback is too large to send back.", ephemeral=True)
            else:
                await method(
                    "An error has occured!",
                    embeds=[Embed(title="Traceback", description=traceback_text)],
                    ephemeral=True,
                )
        print_exception(printed_error)

    async def setup_hook(self):
        await init_models()
        for module in iter_modules([str(Path(__file__).parent / "extensions")]):
            await self.load_extension(f"src.extensions.{module.name}")
        await self.load_extension("jishaku")
        await self.settings.load()

    async def close(self) -> None:
        await self.settings.save()
        return await super().close()


BotInteraction = Interaction[PokestarBot]
