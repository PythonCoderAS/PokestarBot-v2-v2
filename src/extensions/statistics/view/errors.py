from discord import Embed


class ValidationError(ValueError):
    def __init__(self, message: str, embeds: list[Embed] | None = None):
        self.message = message
        self.embeds = embeds or []
