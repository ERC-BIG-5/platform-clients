from tools.env_root import root

def main():
    try:
        root(".")

        import asyncio

        from src.platform_orchestration import PlatformOrchestrator

        from tools.project_logging import get_logger

        orchestrator = PlatformOrchestrator()
        # Check for new tasks first
        orchestrator.check_new_client_tasks()
        # Progress all tasks
        asyncio.run(orchestrator.progress_tasks(None))

    except Exception as e:
        get_logger(__name__).error(f"Error in main program flow: {str(e)}")
        raise


if __name__ == '__main__':
    main()
