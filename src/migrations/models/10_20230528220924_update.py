from tortoise import BaseDBAsyncClient


async def upgrade(db: BaseDBAsyncClient) -> str:
    return """
        ALTER TABLE "statistic" ALTER COLUMN "is_private" DROP DEFAULT;
        ALTER TABLE "statistic" ALTER COLUMN "is_private" DROP NOT NULL;"""


async def downgrade(db: BaseDBAsyncClient) -> str:
    return """
        ALTER TABLE "statistic" ALTER COLUMN "is_private" SET NOT NULL;
        ALTER TABLE "statistic" ALTER COLUMN "is_private" SET DEFAULT False;"""
