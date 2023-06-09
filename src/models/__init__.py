from tortoise import Tortoise
from .statistic import Statistic
from .settings import ServerSettings

TORTOISE_ORM = {
    "connections": {
        "default": "postgres://pokestarbot:pokestarbot@localhost/pokestarbot",
    },
    "apps": {
        "models": {
            "models": [__name__, "aerich.models"],
            "default_connection": "default",
        },
    },
    "use_tz": True,
    "maxsize": 20,
}


async def init():
    """Initialize the ORM."""
    await Tortoise.init(TORTOISE_ORM)
