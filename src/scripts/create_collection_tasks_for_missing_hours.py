from datetime import datetime

from sqlalchemy import select, func
from src.const import BASE_DATA_PATH
from src.db.db_mgmt import DatabaseConfig, DatabaseManager
from src.db.db_models import DBPost, Base
from src.misc.helper import get_posts_with_custom_conditions
import pandas as pd

# todo. we have to use platform specific databases
conf = DatabaseConfig("sqlite", (BASE_DATA_PATH / "db.sqlite").as_posix(), Base)
db = DatabaseManager(conf)


def get_missing_hours(platform: str, year: int) -> list[tuple[str,str,int]]:
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
    query = query.where(DBPost.date_created.between(
        datetime(year, 1, 1),
        datetime(year + 1, 1, 1)
    ))

    # Execute the query and return the results
    result = session.execute(query)
    return result.all()

year = 2022
results = get_missing_hours("youtube", year)
#print(results)

day_hour_dict = {(r[0], r[1]): r[2] for r in results}

missing_hours = []
for day in pd.date_range(start=datetime(year,1,1), end=datetime(year,12,31)):
    for hour in range(24):
        key = (day.strftime('%Y-%m-%d'), f'{hour:02d}')
        if key not in day_hour_dict:
            missing_hours.append((*key, 0))

print(missing_hours)