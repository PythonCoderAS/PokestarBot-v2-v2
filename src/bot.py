from discord.ext.commands import Bot, when_mentioned
from discord import Intents
from pkgutil import iter_modules
from pathlib import Path
from importlib import import_module
from .models import init as init_models

class PokestarBot(Bot):
    def __init__(self, *args, **kwargs):
        intents = Intents.default()
        intents.members = True
        kwargs["intents"] = intents
        super().__init__(when_mentioned, *args, **kwargs)

    async def setup_hook(self):
        await init_models()
        for module in iter_modules([Path(__file__).parent / "extensions"]):
            try:
                imported = import_module(f"{module.name}", package="src.extensions")
                if hasattr(imported, "setup"):
                    await self.load_extension(f"src.extensions.{module.name}")
            except Exception as e:
                print(f"Failed to load extension {module.name}: {e}")
        await self.load_extension("jishaku")

