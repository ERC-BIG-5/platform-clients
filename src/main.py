from src.clients.clients_main import check_new_client_tasks, get_platform_client
from src.clients.clients_models import ClientConfig
from src.db.db_session import init_db
from src.misc.files import init_folders


def main():
    init_folders()
    init_db()
    check_new_client_tasks()
    # progress_tasks()
    # test:
    yt = get_platform_client("youtube",
                             ClientConfig.model_validate({"auth_config": {
                                 "GOOGLE_API_KEY": "ALT_GOOGLE_API_KEY"
                             }
                             }))
    print(yt)


if __name__ == '__main__':
    main()
