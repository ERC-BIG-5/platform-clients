from src.clients.clients_main import check_new_client_tasks, progress_tasks, get_platform_client
from src.clients.clients_models import ClientConfig
from src.const import BIG5_CONFIG
from src.db.db_session import init_db


def main():
    init_db()
    check_new_client_tasks()
    progress_tasks()
    # test:
    """
    yt = get_platform_client("youtube",
        ClientConfig.model_validate({"auth_config": {
        "GOOGLE_API_KEY": "ALT_GOOGLE_API_KEY"}}))
    """

if __name__ == '__main__':
    main()
