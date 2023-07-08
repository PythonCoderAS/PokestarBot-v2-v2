from tortoise import fields
from tortoise.models import Model
from .mixins import GuildIdMixin


class ServerSettings(GuildIdMixin, Model):
    id = fields.IntField(pk=True)
    guild_id = fields.BigIntField(null=False, index=True)
    settings = fields.JSONField(null=False)
