from datetime import datetime
from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sqlalchemy import select, BinaryExpression, func

from src.const import BASE_DATA_PATH
from src.db.db_mgmt import DatabaseConfig, DatabaseManager
from src.db.db_models import DBPost, Base
from sqlalchemy import ScalarResult

# conf = DatabaseConfig("sqlite", (BASE_DATA_PATH / "twitter.sqlite").as_posix())
# db = DatabaseManager(conf)


def get_posts_with_custom_conditions(platform: str,
                         conditions: Optional[BinaryExpression | list[BinaryExpression]] = None
                         ) -> ScalarResult[DBPost]:
    with db.get_session() as session:
        # Start with a base query
        query = select(DBPost)

        # Platform is required
        query = query.where(DBPost.platform == platform)

        if conditions is not None:
            if isinstance(conditions, list):
                for condition in conditions:
                    query = query.where(condition)
            else:
                query = query.where(conditions)

        # Execute the query and return the results
        result = session.execute(query)
        return result.scalars()



def get_posts_day_counts(platform: str,
                         conditions: Optional[BinaryExpression | list[BinaryExpression]] = None
                         ) -> list[DBPost]:
    with db.get_session() as session:
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
                query = query.where(conditions)

        # Execute the query and return the results
        result = session.execute(query)
        return result.all()
