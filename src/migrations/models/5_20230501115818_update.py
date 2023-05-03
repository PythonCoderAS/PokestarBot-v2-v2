from tortoise import BaseDBAsyncClient


async def upgrade(db: BaseDBAsyncClient) -> str:
    return """
        ALTER TABLE "statistic" ADD CONSTRAINT "uid_statistic_channel_f53e9b" UNIQUE ("channel_id", "thread_id", "author_id", "month");
        ALTER TABLE "statistic" DROP CONSTRAINT "uid_statistic_channel_3743df";"""


async def downgrade(db: BaseDBAsyncClient) -> str:
    return """
        ALTER TABLE "statistic" ADD CONSTRAINT "uid_statistic_channel_3743df" UNIQUE ("channel_id", "thread_id", "author_id");
        ALTER TABLE "statistic" DROP CONSTRAINT "uid_statistic_channel_f53e9b";"""
