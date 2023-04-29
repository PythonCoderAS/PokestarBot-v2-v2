from discord.ext.commands import Bot, when_mentioned
from discord import Intents
from pkgutil import iter_modules
from pathlib import Path
from .models import init as init_models
from .models.mixins import BotMixin
from typing import Optional, Self

class PokestarBot(Bot):

    bot: Optional["PokestarBot"] = None

    def __new__(cls) -> Self:
        if cls.bot is None:
            cls.bot = super().__new__(cls)
            BotMixin.bot = cls.bot
        return cls.bot

    def __init__(self, *args, **kwargs):
        intents = Intents.default()
        intents.members = True
        kwargs["intents"] = intents
        super().__init__(when_mentioned, *args, **kwargs)

    async def setup_hook(self):
        await init_models()
        for module in iter_modules([str(Path(__file__).parent / "extensions")]):
            try:
                await self.load_extension(f"src.extensions.{module.name}")
            except Exception as e:
                print(f"Failed to load extension {module.name}: {e}")
        await self.load_extension("jishaku")

