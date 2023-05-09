from discord.ext.commands import Bot, when_mentioned
from discord import Intents, MemberCacheFlags
from pkgutil import iter_modules
from pathlib import Path
from .models import init as init_models
from .models.mixins import BotMixin
from .singleton import SingletonClass


class PokestarBot(Bot, SingletonClass):
    def __init__(self, *args, **kwargs):
        intents = Intents.default()
        intents.members = True
        kwargs["intents"] = intents
        kwargs["member_cache_flags"] = kwargs.get("member_cache_flags", MemberCacheFlags.all())
        kwargs["chunk_guilds_at_startup"] = kwargs.get("chunk_guilds_at_startup", True)
        super().__init__(when_mentioned, *args, **kwargs)
        BotMixin.bot = self

    async def setup_hook(self):
        await init_models()
        for module in iter_modules([str(Path(__file__).parent / "extensions")]):
            await self.load_extension(f"src.extensions.{module.name}")
        await self.load_extension("jishaku")
