from datetime import datetime

from src.db.db_funcs import get_posts
from src.db.db_models import DBPost
from src.db.db_session import init_db, Session
from src.misc.files import init_folders


if __name__ == '__main__':
    init_folders()
    init_db()

    with Session() as session:
        user = DBPost(platform="twitter",
                      post_type="post",
                      # user_id=1,
                      # content_schema_id=2,
                      date_created=datetime.now(),
                      date_collected=datetime.now(),
                      # collection_task_name="1",
                      collection_step=0)
        session.add(user)
        session.commit()

    print(get_posts("twitter"))