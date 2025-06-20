import json
from datetime import datetime, timedelta
from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_utils import get_posts_by_period
from big5_databases.databases.external import TimeWindow

per_day = get_posts_by_period(DatabaseManager.sqlite_db_from_path("tiktok.sqlite"), TimeWindow.DAY)
# print(per_day)
created_per_day = {day_c[0]: day_c[1] for day_c in per_day[0]}
current_ = datetime(2022, 1, 1)

missing_days_tasks = []
idx = 0
rows = []
while current_ < datetime(2024, 1, 1):
    day_str = f"{current_.strftime('%Y-%m-%d')}"
    dc = created_per_day.get(day_str)
    # print(dc)
    if dc < 100:
        print(dc)
    # rows.append([day_str, dc])
    # break
#     if not dc:
#         # print(day_str)
#         # missing_days.append(day_str)
#         missing_days_tasks.append({
#             "platform": "tiktok",
#             "task_name": f"2022/23_add_miss_{idx}",
#             "collection_config": {
#                 "is_random": True,
#                 "limit": 20,
#                 "from_time": day_str,
#                 "to_time": day_str
#             }
#         }
#         )
#         idx += 1
    current_ += timedelta(days=1)
#
# print(json.dumps(missing_days_tasks))
