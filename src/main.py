import asyncio
import json
from datetime import datetime
from pathlib import Path

from databases.db_stats import count_posts, generate_db_stats, BASE_DATA_PATH
from src.platform_orchestration import PlatformOrchestrator
from tools.project_logging import get_logger
from rich import print
from rich.table import Table

from rich.console import Console
import typer

app = typer.Typer()
console = Console()


@app.command()
def status(task_status: bool = True):
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


@app.command()
def db_stats(db_path: str, daily_count: bool = False, store: bool = True):
    p = Path(db_path)
    if not p.exists():
        raise ValueError(f"{p} does not exist")
    stats = generate_db_stats(p, daily_count)
    print(stats.model_dump())
    if store:
        dest = BASE_DATA_PATH / f"stats/{p.stem}-{datetime.now():%Y%m%d_%H%M}.json"
        json.dump(stats.model_dump(), dest.open("w", encoding="utf-8"))


@app.command()
def collect():
    try:
        orchestrator = PlatformOrchestrator()
        # Check for new tasks first
        orchestrator.check_new_client_tasks()
        orchestrator.fix_tasks()
        # Progress all tasks
        asyncio.run(orchestrator.progress_tasks(None))
    except KeyboardInterrupt:
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
    app()
