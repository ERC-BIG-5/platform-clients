import asyncio
from typing import Optional, Sequence

from src.clients.clients_main import check_new_client_tasks
from src.misc.project_logging import get_b5_logger
from src.platform_orchestration import PlatformOrchestrator

logger = get_b5_logger(__file__)


async def progress_tasks(platforms: Optional[Sequence[str]] = None) -> None:
    """
    Progress all tasks for specified (or all) platforms

    Args:
        platforms: Optional list of platform names to process. If None, process all platforms.
    """
    orchestrator = PlatformOrchestrator()
    await orchestrator.progress_tasks(platforms)


def main():
    """Main entry point for the application"""
    try:
        # Check for new tasks first
        check_new_client_tasks()

        # Progress all tasks
        asyncio.run(progress_tasks())

    except Exception as e:
        logger.error(f"Error in main program flow: {str(e)}")
        raise


if __name__ == '__main__':
    main()
