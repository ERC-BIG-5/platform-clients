import asyncio
import json
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich import print
from rich.console import Console
from rich.table import Table

from databases import db_utils
from databases.db_merge import DBMerger
from databases.db_mgmt import DatabaseManager
from databases.db_stats import generate_db_stats
from databases.db_utils import reset_task_states, check_platforms, count_posts
from databases.external import CollectionStatus, DBConfig, SQliteConnection, TimeColumn
from databases.meta_database import add_db
from src.const import BASE_DATA_PATH
from src.platform_orchestration import PlatformOrchestrator
from tools.env_root import root
from tools.project_logging import get_logger

app = typer.Typer(name="Platform-Collection commands",
                  short_help="Information and process commands for platform collection")
console = Console()


# cuz Typer does not work with literals
class TimeWindow_(str, Enum):
    DAY = "day"
    MONTH = "month"
    YEAR = "year"


@app.command(short_help="Get the number of posts, and tasks statuses of all specified databases (RUN_CONFIG)")
def status(task_status: bool = True,
           databases: Optional[
               Annotated[list[Path], typer.Option(help="Use this database instead of the RUN_CONFIG dbs")]] = None):
    orchestrator = PlatformOrchestrator()

    task_status_types = ["done", "init", "paused", "aborted"] if task_status else []
    table = Table("platform", "total", "size", *task_status_types)

    def calc_row(db: DatabaseManager, platform_: str) -> list[str | int]:
        if task_status:
            tasks = db.count_states()
            status_numbers = [str(tasks.get(t, 0)) for t in task_status_types]
        else:
            status_numbers = []
        total_posts = str(count_posts(db=db))
        size = str(f"{int(db_utils.file_size(db) / (1024 * 1024))} Mb")
        return [platform_, total_posts, size] + status_numbers

    # use a database
    if databases:
        for db_path in databases:
            db = DatabaseManager.sqlite_db_from_path(db_path, create=False)
            platforms = list(check_platforms(db))
            if len(platforms) > 1:
                raise ValueError("Database has more than one platform")
            platform = platforms[0]

            row = calc_row(db, platform)
            table.add_row(*row)

    # normal method. use databases as defined in RUN-CONFIG
    else:
        for platform, manager in orchestrator.platform_managers.items():
            row = calc_row(manager.platform_db.db_mgmt, platform)
            table.add_row(*row)

    console.print(table)


def complete_path(current: str):
    return [sub.name for sub in Path(current).glob("*")]


@app.command(short_help="Get the stats of a database. monthly or daily count")
def db_stats(
        db_path: Annotated[Path, typer.Option(help="Path to sqlite database")],
        period: Annotated[TimeWindow_, typer.Option(help="day,month,year")] = TimeWindow_.DAY,
        time_column: Annotated[TimeColumn, typer.Option(help="Time column created or collected")] = TimeColumn.CREATED,
        store: bool = True):
    stats = generate_db_stats(DatabaseManager.sqlite_db_from_path(db_path, False), period)
    print(stats.model_dump())
    if store:
        stats_dir = BASE_DATA_PATH / f"stats"
        stats_dir.mkdir(parents=True, exist_ok=True)
        dest = stats_dir / f"{db_path.stem}-{datetime.now():%Y%m%d_%H%M}.json"
        json.dump(stats.model_dump(), dest.open("w", encoding="utf-8"))


# todo, use Enum
def autocomplete_conflict_types() -> list[str]:
    return ["post", "task"]


@app.command(short_help="Check the posts,tasks of two databases for orverlaps")
def check_conflicts(item_type: Annotated[str, typer.Option(autocompletion=autocomplete_conflict_types)],
                    db1: Path, db2: Path):
    if item_type == "post":
        conflicts = DBMerger.find_conflicting_posts([db1, db2])
    else:
        conflicts = DBMerger.find_conflicting_tasks(dbs=[db1, db2])
    conflicts_dir = BASE_DATA_PATH / f"conflicts"
    conflicts_dir.mkdir(parents=True, exist_ok=True)
    dest_file = conflicts_dir / f"{db1.stem}-{db2.stem}_{item_type}_{datetime.now():%Y%m%d_%H%M}.json"
    print(conflicts)
    print(f"Results dumped to: {dest_file}")
    json.dump(conflicts, dest_file.open("w", encoding="utf-8"))


@app.command(short_help="Reset all tasks that are not DONE to INIT (platform dbs of RUN_CONFIG)")
def reset_undone_tasks(platforms: Optional[
    Annotated[list[str], typer.Option(help="select the platforms, or reset for all")]] = None):
    orchestrator = PlatformOrchestrator()
    for platform, manager in orchestrator.platform_managers.items():
        if not platforms or platform in platforms:
            tasks_ids = [t.id for t in
                         manager.platform_db.get_tasks_of_states([CollectionStatus.DONE, CollectionStatus.INIT], True)]
            reset_task_states(manager.platform_db.db_mgmt, tasks_ids)


@app.command(short_help="Merge 2 databases")
def merge_dbs(
        result_db: Path,
        platform: str,
        db1: Path,
        db2: Path):
    merger = DBMerger(result_db, platform)
    merger.merge([db1, db2])


@app.command(short_help="list task groups, by string-prefix")
def merge_dbs(
        result_db: Path,
        platform: str,
        db1: Path,
        db2: Path):
    pass

@app.command(short_help="Metadatabase keeps status, post numbers and other stats of all databases")
def init_meta_database():
    meta_db = DatabaseManager(config=DBConfig(
        db_connection=SQliteConnection(db_path=root() / "data/col_db/new_main.sqlite"),
        create=True,
        require_existing_parent_dir=False,
        tables=["platform_databases2"]
    ))
    meta_db.init_database()

    if (db_file := (root() / "data/databases.json")).exists():
        dbs = json.load(db_file.open( "r", encoding="utf-8"))
        for db in dbs:
            add_db(db, meta_db)
    else:
        print(db_file, "does not exist")

@app.command(short_help="Run the main collection (better just run with python- cuz crashes look annoying)")
def collect():
    orchestrator = None
    try:
        orchestrator = PlatformOrchestrator()
        # Check for new tasks first
        orchestrator.check_new_client_tasks()
        orchestrator.fix_tasks()
        # Progress all tasks
        asyncio.run(orchestrator.progress_tasks(None))
    except KeyboardInterrupt:
        if orchestrator:
            asyncio.run(orchestrator.abort_tasks())
        print("bye bye")
    except Exception as e:
        get_logger(__name__).error(f"Error in main program flow: {str(e)}")
        raise


if __name__ == '__main__':
    # if len(sys.argv) > 1:
    #     print(sys.argv[1])
    #     if sys.argv[1] == "status":
    #         status()
    #     else:
    #         print("Unknown command")
    # else:
    #     main()
    # app()
    # collect()
    db_stats("data/col_db/tiktok/rm/tiktok.sqlite")
