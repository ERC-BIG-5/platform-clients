from src.clients.client_task import check_new_client_tasks
from src.clients.clients_main import progress_tasks
from src.db.db_funcs import get_posts
from src.db.db_session import init_db
from src.misc.files import init_folders


if __name__ == '__main__':
    init_folders()
    init_db()
    check_new_client_tasks()
    progress_tasks()

    # with Session() as session:
    #     user = DBPost(platform="twitter",
    #                   post_type="post",
    #                   # user_id=1,
    #                   # content_schema_id=2,
    #                   date_created=datetime.now(),
    #                   date_collected=datetime.now(),
    #                   # collection_task_name="1",
    #                   collection_step=0)
    #     session.add(user)
    #     session.commit()

    print(get_posts("twitter"))