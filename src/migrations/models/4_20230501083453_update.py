from tortoise import BaseDBAsyncClient


async def upgrade(db: BaseDBAsyncClient) -> str:
    return """
        ALTER TABLE "statistic" ADD "month" DATE NOT NULL;
        ALTER TABLE "statistic" DROP COLUMN "updated_at";
        ALTER TABLE "statistic" DROP COLUMN "created_at";
        CREATE INDEX "idx_statistic_month_2095cb" ON "statistic" ("month");"""


async def downgrade(db: BaseDBAsyncClient) -> str:
    return """
        DROP INDEX "idx_statistic_month_2095cb";
        ALTER TABLE "statistic" ADD "updated_at" TIMESTAMPTZ NOT NULL  DEFAULT CURRENT_TIMESTAMP;
        ALTER TABLE "statistic" ADD "created_at" TIMESTAMPTZ NOT NULL  DEFAULT CURRENT_TIMESTAMP;
        ALTER TABLE "statistic" DROP COLUMN "month";"""
