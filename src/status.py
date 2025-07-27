from pathlib import Path
from typing import Optional

from sqlalchemy.sql.schema import Table

from big5_databases.databases import db_utils
from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_utils import count_posts, check_platforms
from src.platform_orchestration import PlatformOrchestrator


def general_databases_status(task_status: bool = True, databases: Optional[list[Path]] = None):
    orchestrator = PlatformOrchestrator()

    task_status_types = ["done", "init", "paused", "aborted"] if task_status else []
    results = []

    def calc_row(db: DatabaseManager, platform_: str) -> dict[str, str | int]:
        if task_status:
            tasks = db_utils.count_states(db)
            status_numbers = [str(tasks.get(t, 0)) for t in task_status_types]
        else:
            status_numbers = []
        total_posts = str(count_posts(db=db))
        size = str(f"{int(db_utils.file_size(db) / (1024 * 1024))} Mb")
        return {"platform": platform_, "total":total_posts,
                "size": size, "path":str(db.config.db_connection.db_path)} | dict(zip(task_status_types, status_numbers))

    # use a database
    if databases:
        for db_path in databases:
            db = DatabaseManager.sqlite_db_from_path(db_path, create=False)
            platforms = list(check_platforms(db))
            if len(platforms) > 1:
                raise ValueError("Database has more than one platform")
            platform = platforms[0]

            row = calc_row(db, platform)
            results.append(row)

    # normal method. use databases as defined in RUN-CONFIG
    else:
        for platform, manager in orchestrator.platform_managers.items():
            row = calc_row(manager.platform_db.db_mgmt, platform)
            results.append(row)

    return results
