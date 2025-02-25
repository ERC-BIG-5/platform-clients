import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich import print
from rich.console import Console
from rich.table import Table

from databases.db_merge import DBMerger
from databases.db_stats import count_posts, generate_db_stats, BASE_DATA_PATH
from databases.db_utils import reset_task_states
from databases.external import CollectionStatus
from src.platform_orchestration import PlatformOrchestrator
from tools.project_logging import get_logger

app = typer.Typer(name="Platform-Collection commands",
                  short_help="Information and process commands for platform collection")
console = Console()


@app.command(short_help="Get the number of posts, and tasks statuses of all specified databases (RUN_CONFIG)")
def status(task_status: bool = True,
           alternative_dbs: Optional[list[Path]] = None):
    orchestrator = PlatformOrchestrator()
    task_status_types = ["done", "init", "paused", "aborted"]
    table = Table("platform", "total", *task_status_types)

    for platform, manager in orchestrator.platform_managers.items():
        if task_status:
            tasks = manager.platform_db.count_states()
            total_posts = count_posts(db_manager=manager.platform_db.db_mgmt)
            status_numbers = [str(tasks.get(t, 0)) for t in task_status_types]
            row = [platform, str(total_posts)] + status_numbers
            table.add_row(*row)

    console.print(table)


def complete_path(current: str):
    return [sub.name for sub in Path(current).glob("*")]


@app.command(short_help="Get the stats of a database. monthly or daily count")
def db_stats(
        db_path: Annotated[Optional[str], typer.Option(help="Path to sqlite database")],
        daily_count: Annotated[bool, typer.Argument()] = False,
        store: bool = True):
    p = Path(db_path)
    if not p.exists():
        raise FileNotFoundError(f"File {db_path} does not exist")

    stats = generate_db_stats(p, daily_count)
    print(stats.model_dump())
    if store:
        stats_dir = BASE_DATA_PATH / f"stats"
        stats_dir.mkdir(parents=True, exist_ok=True)
        dest = stats_dir / f"{p.stem}-{datetime.now():%Y%m%d_%H%M}.json"
        json.dump(stats.model_dump(), dest.open("w", encoding="utf-8"))


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
    collect()
