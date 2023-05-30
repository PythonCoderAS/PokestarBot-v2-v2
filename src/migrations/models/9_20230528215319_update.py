from tortoise import BaseDBAsyncClient


async def upgrade(db: BaseDBAsyncClient) -> str:
    return """
        ALTER TABLE "statistic" ADD "is_private" BOOL NOT NULL  DEFAULT False;
        ALTER TABLE "statistic" ALTER COLUMN "num_attachments" TYPE INT USING "num_attachments"::INT;
        ALTER TABLE "statistic" ALTER COLUMN "num_attachments" TYPE INT USING "num_attachments"::INT;
        ALTER TABLE "statistic" ALTER COLUMN "num_attachments" TYPE INT USING "num_attachments"::INT;
        ALTER TABLE "statistic" ALTER COLUMN "num_attachments" TYPE INT USING "num_attachments"::INT;
        CREATE INDEX "idx_statistic_is_priv_190520" ON "statistic" ("is_private");"""


async def downgrade(db: BaseDBAsyncClient) -> str:
    return """
        DROP INDEX "idx_statistic_is_priv_190520";
        ALTER TABLE "statistic" DROP COLUMN "is_private";
        ALTER TABLE "statistic" ALTER COLUMN "num_attachments" TYPE BIGINT USING "num_attachments"::BIGINT;
        ALTER TABLE "statistic" ALTER COLUMN "num_attachments" TYPE BIGINT USING "num_attachments"::BIGINT;
        ALTER TABLE "statistic" ALTER COLUMN "num_attachments" TYPE BIGINT USING "num_attachments"::BIGINT;
        ALTER TABLE "statistic" ALTER COLUMN "num_attachments" TYPE BIGINT USING "num_attachments"::BIGINT;"""
