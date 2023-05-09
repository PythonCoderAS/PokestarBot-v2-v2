from typing import Optional, TYPE_CHECKING, Union

from discord import (
    CategoryChannel,
    DMChannel,
    Guild,
    Member,
    Role,
    TextChannel,
    User,
    VoiceChannel,
    ForumChannel,
    StageChannel,
)
from tortoise import fields

if TYPE_CHECKING:
    from ..bot import PokestarBot


class BotMixin:
    bot: Optional["PokestarBot"] = None


class ChannelIdMixin(BotMixin):
    @property
    def channel(
        self,
    ) -> Optional[Union[TextChannel, CategoryChannel, VoiceChannel, ForumChannel, StageChannel, DMChannel,]]:
        if self.channel_id:
            return self.bot.get_channel(self.channel_id)
        return None


class AuthorIdMixin(BotMixin):
    @property
    def user(self) -> Optional[User]:
        if self.author_id:
            return self.bot.get_user(self.author_id)
        return None

    def is_member_of_guild(self, guild: Guild) -> bool:
        return isinstance(self.get_member(guild), Member)

    def get_member_or_user(self, guild: Guild) -> Optional[Union[Member, User]]:
        """Gets the member class of a user if they are in the guild, otherwise returns the user class.

        :param guild: The guild to check for the member class.
        :return: The member class if they are in the guild, otherwise the user class.
        """
        if self.author_id:
            return guild.get_member(self.author_id) or self.bot.get_user(self.author_id)
        return None

    def get_user(self) -> Optional[User]:
        if self.author_id:
            return self.bot.get_user(self.author_id)
        return None

    @property
    def mention_author(self) -> Optional[str]:
        """100% guaranteed to mention them regardless of user/bot status."""
        if self.author_id:
            return f"<@!{self.author_id}>"
        return None


class RoleMixin(BotMixin):
    def get_role(self, guild: Guild) -> Optional[Role]:
        if self.role_id:
            return guild.get_role(self.role_id)
        return None


class GuildIdMixin(BotMixin):
    @property
    def guild(self) -> Optional[Guild]:
        if self.guild_id:
            return self.bot.get_guild(self.guild_id)
        return None
