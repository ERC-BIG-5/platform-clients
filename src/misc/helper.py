from typing import Optional

from sqlalchemy import ScalarResult
from sqlalchemy import select, BinaryExpression, func

from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_models import DBPost
from big5_databases.databases.external import DBConfig


# conf = DatabaseConfig("sqlite", (BASE_DATA_PATH / "twitter.sqlite").as_posix())
# db = DatabaseManager(conf)


def get_posts_with_custom_conditions(
        platform: str,
        db_config: DBConfig,
        conditions: Optional[BinaryExpression | list[BinaryExpression]] = None
) -> ScalarResult[DBPost]:
    db_manager = DatabaseManager(db_config)
    db_manager.init_database()

    with db_manager.get_session() as session:
        # Start with a base query
        query = select(DBPost)

        # Platform is required
        query = query.where(DBPost.platform == platform)

        if conditions is not None:
            if isinstance(conditions, list):
                for condition in conditions:
                    query = query.where(condition)
            else:
                assert conditions is not None
                query = query.where(conditions)

        # Execute the query and return the results
        result = session.execute(query)
        return result.scalars()


def get_posts_day_counts(platform: str,
                         db_config: DBConfig,
                         conditions: Optional[BinaryExpression | list[BinaryExpression]] = None
                         ) -> list[DBPost]:
    db_manager = DatabaseManager(db_config)
    db_manager.init_database()

    with db_manager.get_session() as session:
        # Start with a base query
        query = select(
            func.date(DBPost.date_created).label('day'),
            func.count().label('count')
        ).group_by(
            func.date(DBPost.date_created)
        )

        # Platform is required
        query = query.where(DBPost.platform == platform)

        if conditions is not None:
            if isinstance(conditions, list):
                for condition in conditions:
                    query = query.where(condition)
            else:
                assert conditions is not None
                query = query.where(conditions)

        # Execute the query and return the results
        result = session.execute(query)
        return result.all()
