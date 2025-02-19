import asyncio

from src.platform_orchestration import PlatformOrchestrator
from tools.project_logging import get_logger

def main():
    try:
        orchestrator = PlatformOrchestrator()
        # Check for new tasks first
        orchestrator.check_new_client_tasks()
        orchestrator.fix_tasks()
        # Progress all tasks
        asyncio.run(orchestrator.progress_tasks(None))
    except KeyboardInterrupt:
        orchestrator.abort_tasks()
        print("bye bye")
    except Exception as e:
        get_logger(__name__).error(f"Error in main program flow: {str(e)}")
        raise


if __name__ == '__main__':
    main()
