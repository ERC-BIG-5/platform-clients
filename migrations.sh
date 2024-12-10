export PYTHONPATH=$PYTHONPATH:/home/rsoleyma/projects/Tools/

alembic revision --autogenerate -m "make transient"
alembic upgrade head