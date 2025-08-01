import asyncio
import json
import typer
from datetime import datetime
from enum import Enum
from pathlib import Path
from rich import print
from rich.console import Console
from rich.table import Table
from tools.env_root import root
from typing import Annotated, Optional, Any

from big5_databases.databases.c_db_merge import merge_database
from big5_databases.databases.db_merge import DBMerger
from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_stats import generate_db_stats
from big5_databases.databases.db_utils import reset_task_states
from big5_databases.databases.external import CollectionStatus, DBConfig, SQliteConnection, DBStats
from big5_databases.databases.meta_database import add_db, MetaDatabase
from src.const import BASE_DATA_PATH, BIG5_CONFIG
from src.platform_orchestration import PlatformOrchestrator
from src.status import general_databases_status

app = typer.Typer(name="Platform-Collection commands",
                  short_help="Information and process commands for platform collection")
console = Console()


# cuz Typer does not work with literals
class TimeWindow_(str, Enum):
    DAY = "day"
    MONTH = "month"
    YEAR = "year"


@app.command()
def init():
    MetaDatabase(True)


@app.command(short_help="Get the number of posts, and tasks statuses of all specified databases (RUN_CONFIG)")
def database_names():
    orchestrator = PlatformOrchestrator()
    for platform, manager in orchestrator.platform_managers.items():
        print(platform, manager.platform_db.db_config.connection_str)


@app.command(short_help="Get the number of posts, and tasks statuses of all specified databases (RUN_CONFIG)")
def status(task_status: bool = True,
           databases: Optional[
               Annotated[list[Path], typer.Option(help="Use this database instead of the RUN_CONFIG dbs")]] = None):
    results: list[dict[str, Any]] = general_databases_status(task_status, databases)
    table = Table(*list(results[0].keys()))
    for r in results:
        table.add_row(*r.values())
    console.print(table)


def complete_path(current: str):
    return [sub.name for sub in Path(current).glob("*")]


@app.command(short_help="Get the stats of a database. monthly or daily count")
def db_stats(
        db_path: Annotated[Optional[Path], typer.Argument(help="Path to sqlite database")] = None,
        period: Annotated[TimeWindow_, typer.Option(help="day,month,year")] = TimeWindow_.DAY,
        use_last_if_available: bool = typer.Option(True),
        store: bool = True):
    def stats_for(db: DatabaseManager):
        stats_dir = BASE_DATA_PATH / f"stats"
        stats_dir.mkdir(parents=True, exist_ok=True)
        db_path = db.config.db_connection.db_path
        create_stats = not use_last_if_available
        stats = None
        if not create_stats:
            stats_files = list(stats_dir.glob(f"{db_path.stem}-*.json"))
            if len(stats_files) >= 1:
                stats_file = sorted(stats_files)[-1]
                stats = DBStats.model_validate(json.load(stats_file.open()))
            else:
                create_stats = True
        if create_stats:
            stats = generate_db_stats(db, period)
        print(stats.model_dump())
        if store and create_stats:
            dest = stats_dir / f"{db_path.stem}-{datetime.now():%Y%m%d_%H%M}.json"
            json.dump(stats.model_dump(), dest.open("w", encoding="utf-8"), indent=2)

    if not db_path:
        orchestrator = PlatformOrchestrator()
        for platform, manager in orchestrator.platform_managers.items():
            stats_for(manager.platform_db.db_mgmt)
    else:
        stats_for(DatabaseManager.sqlite_db_from_path(db_path, False))


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
        src_db: Path,
        target_db: Path):
    stats = merge_database(src_db, target_db)
    print(stats)


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
        dbs = json.load(db_file.open("r", encoding="utf-8"))
        for db in dbs:
            add_db(db, meta_db)
    else:
        print(db_file, "does not exist")


async def _collect(run_forever: bool = False):
    orchestrator = PlatformOrchestrator()
    if run_forever:
        await orchestrator.run_collect_loop()
    else:
        await orchestrator.collect()

@app.command(short_help="Run the main collection (better just run with python- cuz crashes look annoying)")
def collect(run_conf: Annotated[Optional[str], typer.Option()] = None, run_forever: bool = False):
    if run_conf:
        BIG5_CONFIG.run_config_file_name = run_conf
    asyncio.run(_collect(run_forever))


if __name__ == '__main__':
    try:
        # status()
        collect()
        # collect("phase2_tiktok.yaml")
        #status()
        # Path("/home/rsoleyma/projects/big5/platform_clients/data/dbs/phase2/youtube.sqlite").unlink(missing_ok=True)
        #asyncio.run(collect(False))
    except KeyboardInterrupt:
        pass
    # database_names()
    # asyncio.run(collect(run_forever=True))

    # db_stats(store=False)
    # status()
    # status(databases=[Path("exp/youtube-exp.sqlite")])
    # db_stats()

    # res = parse_task_data({
    #     "platform": "youtube",
    #     "group_prefix": "2023_en_all_by_weeks",
    #     "static_params": {
    #         "limit": 2000,
    #         "language": "en",
    #         "part": [
    #             "snippet",
    #             "contentDetails",
    #             "status",
    #             "statistics",
    #             "topicDetails",
    #             "localizations"
    #         ]
    #     },
    #     "time_config": {
    #         "start": "2023-01-01T00:00:00Z",
    #         "end": "2023-01-02T00:00:00Z",
    #         "interval": {
    #             "days": 1
    #         }
    #     }
    # })
    #
    # print(res)
