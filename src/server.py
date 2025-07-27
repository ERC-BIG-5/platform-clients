import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, BackgroundTasks
from starlette.requests import Request
from starlette.responses import RedirectResponse

from big5_databases.databases.db_settings import SqliteSettings
from big5_databases.databases.external import ClientTaskConfig
from src.clients.clients_models import ClientTaskGroupConfig, all_task_schemas
from src.clients.task_parser import generate_configs, parse_task_data
from src.platform_orchestration import PlatformOrchestrator
from starlette.exceptions import HTTPException

from src.status import general_databases_status
from tools.project_logging import get_logger


class PlatformClientState:
    def __init__(self):
        self.orchestrator = PlatformOrchestrator()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state = PlatformClientState()
    task = asyncio.create_task(app.state.orchestrator.run_collect_loop())
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)
executor = ThreadPoolExecutor(max_workers=4)


@app.get("/")
async def redirect_docs():
    return RedirectResponse(url="/docs")

@app.post("/generate_tasks",response_model_exclude_defaults=True, response_model_exclude_unset=True)
async def generate_tasks(tasks: all_task_schemas) -> list[ClientTaskConfig]:
    tasks = parse_task_data(tasks)
    return tasks

@app.post("/submit")
async def submit(request: Request, tasks: all_task_schemas,
                  background_tasks: BackgroundTasks):
    orch: PlatformOrchestrator = request.app.state.orchestrator
    tasks = parse_task_data(tasks)

    added_tasks, all_added = orch.add_tasks(tasks)

    # print(added_tasks)
    #background_tasks.add_task(orch.progress_tasks)
    return added_tasks


@app.post("/continue")
async def collect(request: Request, platform_name: str, background_tasks: BackgroundTasks):
    orch: PlatformOrchestrator = request.app.state.orchestrator
    loop = asyncio.get_event_loop()
    loop.run_in_executor(executor, orch.progress_tasks, [platform_name])
    # background_tasks.add_task(orch.progress_tasks, [platform_name])


@app.get("/run_state")
async def status(request: Request):
    orch: PlatformOrchestrator = request.app.state.orchestrator
    return orch.get_status()

@app.get("/set_activate")
async def set_activate(request: Request, platform: str, active: bool):
    orch: PlatformOrchestrator = request.app.state.orchestrator
    orch.platform_managers[platform].active = active
    return orch.get_status()


@app.get("/status")
async def status(task_status: bool = True, databases: Optional[list[Path]] = None):
    return general_databases_status(task_status, databases)


@app.get("/databases")
async def databases() -> dict[str, str]:
    return {platform: str(manager.platform_db.db_config.db_connection.db_path.relative_to(SqliteSettings().SQLITE_DBS_BASE_PATH)) for platform, manager in
            PlatformOrchestrator().platform_managers.items()}
