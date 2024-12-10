from alembic.config import Config

from alembic import command

db_paths = [
    'data/main.sqlite',
    'data/youtube.sqlite',
]

alembic_cfg = Config("alembic.ini")

for db_path in db_paths:
    # Update the SQLAlchemy URL for each database
    alembic_cfg.set_main_option('sqlalchemy.url', f'sqlite:///{db_path}')

    # Run the upgrade
    command.upgrade(alembic_cfg, "head", True)