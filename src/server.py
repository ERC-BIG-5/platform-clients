import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from fastapi import FastAPI, BackgroundTasks
from starlette.requests import Request
from starlette.responses import RedirectResponse

from big5_databases.databases.external import ClientTaskConfig
from src.clients.clients_models import ClientTaskGroupConfig
from src.clients.task_groups import generate_configs
from src.platform_orchestration import PlatformOrchestrator
from starlette.exceptions import HTTPException


class PlatformClientState:
    def __init__(self):
        self.orchestrator = PlatformOrchestrator()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state = PlatformClientState()
    yield


app = FastAPI(lifespan=lifespan)
executor = ThreadPoolExecutor(max_workers=4)

@app.get("/")
async def redirect_docs():
    return RedirectResponse(url="/docs")


@app.post("/submit")
async def collect(request: Request, tasks: ClientTaskConfig | list[ClientTaskConfig] | ClientTaskGroupConfig,
            background_tasks: BackgroundTasks):
    orch: PlatformOrchestrator = request.app.state.orchestrator
    if isinstance(tasks, ClientTaskConfig):
        added_tasks, all_added = orch.process_tasks([tasks])
    elif isinstance(tasks, ClientTaskGroupConfig):
        task_group_conf, tasks = generate_configs(tasks)
        added_tasks, all_added = orch.process_tasks(tasks, task_group_conf)
    elif isinstance(tasks, list):
        added_tasks, all_added = orch.process_tasks(tasks)
    else:
        raise HTTPException(400, detail=None, headers=None)
    # print(added_tasks)
    background_tasks.add_task(orch.progress_tasks)
    return added_tasks

@app.post("/continue")
async def collect(request: Request, platform_name: str, background_tasks: BackgroundTasks):
    orch: PlatformOrchestrator = request.app.state.orchestrator
    loop = asyncio.get_event_loop()
    loop.run_in_executor(executor, orch.progress_tasks, [platform_name])
    #background_tasks.add_task(orch.progress_tasks, [platform_name])

@app.get("/status")
async def collect(request: Request):
    orch: PlatformOrchestrator = request.app.state.orchestrator
    return orch.get_status()
