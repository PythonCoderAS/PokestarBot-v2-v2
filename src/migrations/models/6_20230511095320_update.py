from tortoise import BaseDBAsyncClient


async def upgrade(db: BaseDBAsyncClient) -> str:
    return """
        ALTER TABLE "statistic" ADD "is_bot" BOOL NOT NULL  DEFAULT False;
        ALTER TABLE "statistic" ADD "num_characters" BIGINT NOT NULL  DEFAULT 0;
        ALTER TABLE "statistic" ADD "num_words" BIGINT NOT NULL  DEFAULT 0;
        ALTER TABLE "statistic" ADD "num_attachments" BIGINT NOT NULL  DEFAULT 0;"""


async def downgrade(db: BaseDBAsyncClient) -> str:
    return """
        ALTER TABLE "statistic" DROP COLUMN "is_bot";
        ALTER TABLE "statistic" DROP COLUMN "num_characters";
        ALTER TABLE "statistic" DROP COLUMN "num_words";
        ALTER TABLE "statistic" DROP COLUMN "num_attachments";"""
