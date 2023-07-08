from tortoise import BaseDBAsyncClient


async def upgrade(db: BaseDBAsyncClient) -> str:
    return """
        CREATE TABLE IF NOT EXISTS "serversettings" (
    "id" SERIAL NOT NULL PRIMARY KEY,
    "guild_id" BIGINT NOT NULL,
    "settings" JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS "idx_serversetti_guild_i_9eea21" ON "serversettings" ("guild_id");;"""


async def downgrade(db: BaseDBAsyncClient) -> str:
    return """
        DROP TABLE IF EXISTS "serversettings";"""
