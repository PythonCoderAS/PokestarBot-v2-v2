from dataclasses import dataclass, field, asdict
from typing import TYPE_CHECKING

from discord import Guild

from .models import ServerSettings as ServerSettingsModel
from .singleton import SingletonClass

if TYPE_CHECKING:
    from .bot import PokestarBot


@dataclass
class BooruChannel:
    id: int


@dataclass
class ServerSetting:
    booru_channels: list[BooruChannel] = field(default_factory=list)

    @classmethod
    def load_from_json_dict(cls, json_dict: dict):
        return cls(
            booru_channels=[BooruChannel(**booru_channel) for booru_channel in json_dict.get("booru_channels", [])]
        )


class ServerSettings(SingletonClass):
    def __init__(self, bot: "PokestarBot"):
        self.bot = bot
        self.settings = {}
        self.seen_guilds = set()

    async def load(self):
        self.settings = {}
        for server_settings in await ServerSettingsModel.all():
            self.settings[server_settings.guild_id] = ServerSetting.load_from_json_dict(server_settings.settings)
            self.seen_guilds.add(server_settings.guild_id)

    def get(self, guild: Guild) -> ServerSetting:
        return self.settings.setdefault(guild.id, ServerSetting())

    async def save(self):
        for guild_id, server_settings in self.settings.items():
            if guild_id not in self.seen_guilds:
                await ServerSettingsModel.create(guild_id=guild_id, settings=asdict(server_settings))
            else:
                await ServerSettingsModel.filter(guild_id=guild_id).update(settings=asdict(server_settings))
        self.seen_guilds = set(self.settings.keys())

    async def save_guild(self, guild: Guild):
        server_settings = self.get(guild)
        if guild.id not in self.seen_guilds:
            await ServerSettingsModel.create(guild_id=guild.id, settings=asdict(server_settings))
        else:
            await ServerSettingsModel.filter(guild_id=guild.id).update(settings=asdict(server_settings))
        self.seen_guilds.add(guild.id)
