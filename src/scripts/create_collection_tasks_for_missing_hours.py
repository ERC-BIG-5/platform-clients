from datetime import datetime
from typing import Optional

from sqlalchemy import select, func
from src.const import BASE_DATA_PATH
from src.db.db_mgmt import DatabaseConfig, DatabaseManager
from src.db.db_models import DBPost, Base
from src.misc.helper import get_posts_with_custom_conditions
import pandas as pd

# todo. we have to use platform specific databases
conf = DatabaseConfig("sqlite", (BASE_DATA_PATH / "youtube.sqlite").as_posix())
db = DatabaseManager(conf)


def get_missing_hours(platform: str, year: int,
                      min_month: Optional[int] = None,
                      max_month: Optional[int] = None) -> list[tuple[str,str,int]]:
    with db.get_session() as session:
        # Start with a base query
        query = select(
            func.date(DBPost.date_created).label('day'),
            func.strftime('%H', DBPost.date_created).label('hour'),
            func.count().label('count')
        ).group_by(
            func.date(DBPost.date_created),
            func.strftime('%H', DBPost.date_created)
        )

        # Platform is required
    query = query.where(DBPost.platform == platform)
    if min_month or max_month:
        query = query.where(DBPost.date_created.between(
            datetime(year, max(1,min_month), 1),
            # todo. fix, this will exclude 12
            datetime(year + 1, min(12,max_month), 1)
        ))
    else:
        query = query.where(DBPost.date_created.between(
            datetime(year, 1, 1),
            datetime(year + 1, 1, 1)
        ))

    # Execute the query and return the results
    result = session.execute(query)
    return result.all()

if __name__ == "__main__":
    year = 2024
    results = get_missing_hours("youtube", year,2,3)
    #print(results)

    day_hour_dict = {(r[0], r[1]): r[2] for r in results}

    missing_hours = []
    for day in pd.date_range(start=datetime(year,2,1), end=datetime(year,3,1)):
        for hour in range(24):
            key = (day.strftime('%Y-%m-%d'), f'{hour:02d}')
            if key not in day_hour_dict:
                missing_hours.append((*key, 0))

    print(missing_hours)