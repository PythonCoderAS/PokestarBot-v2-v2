from tortoise import BaseDBAsyncClient


async def upgrade(db: BaseDBAsyncClient) -> str:
    return """
        CREATE TABLE IF NOT EXISTS "statistic" (
    "id" SERIAL NOT NULL PRIMARY KEY,
    "guild_id" BIGINT,
    "channel_id" BIGINT NOT NULL,
    "thread_id" BIGINT,
    "author_id" BIGINT NOT NULL,
    "messages" INT NOT NULL  DEFAULT 0,
    CONSTRAINT "uid_statistic_channel_3743df" UNIQUE ("channel_id", "thread_id", "author_id")
);
CREATE INDEX IF NOT EXISTS "idx_statistic_guild_i_54b4e5" ON "statistic" ("guild_id");
CREATE INDEX IF NOT EXISTS "idx_statistic_channel_fdbf18" ON "statistic" ("channel_id");
CREATE INDEX IF NOT EXISTS "idx_statistic_thread__f7ad54" ON "statistic" ("thread_id");
CREATE INDEX IF NOT EXISTS "idx_statistic_author__789272" ON "statistic" ("author_id");;"""


async def downgrade(db: BaseDBAsyncClient) -> str:
    return """
        DROP TABLE IF EXISTS "statistic";"""
