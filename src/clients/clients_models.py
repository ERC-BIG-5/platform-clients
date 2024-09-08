from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.const import CollectionStatus, ENV_FILE_PATH
from src.db import db_funcs
from src.db.db_session import Session


class CollectConfig(BaseModel):
    model_config = {'extra': "allow"}
    query: Optional[str] = None
    limit: Optional[int] = None
    from_time: Optional[str] = None
    to_time: Optional[str] = None
    language: Optional[str] = None
    location_base: Optional[str] = None
    location_mod: Optional[str] = None

class ClientConfig(BaseModel):
    model_config = {'extra': "allow"}
    auth_config: Optional[dict[str, str]] = None
    request_delay: Optional[int] = None

class ClientTaskConfig(BaseModel):
    model_config = {'extra': "allow"}
    id: Optional[int] = None
    task_name: str
    platform: str
    collection_config: list[CollectConfig]
    client_config: Optional[ClientConfig] = None
    status: CollectionStatus = CollectionStatus.INIT
    time_added: Optional[datetime] = None
    steps_done: Optional[int] = -1
    #
    current_step_config: Optional[CollectConfig] = None

    @property
    def next_task_idx(self):
        return self.steps_done + 1

    def update_current_config(self):
        if not self.current_step_config:
            self.current_step_config = CollectConfig()
        for step in range(self.next_task_idx + 1):
            fields = self.collection_config[step].model_dump(exclude_unset=True, exclude_defaults=True)
            for k, v in fields.items():
                setattr(self.current_step_config, k, v)

    def next(self):
        with Session() as session:
            db_obj = db_funcs.get_task(self.id)
            db_obj.steps_done += 1
            session.add(db_obj)
            session.commit()
        self.steps_done += 1

    def __len__(self):
        return len(self.collection_config)

    def has_more(self) -> bool:
        """
        check, if there are more steps to do.
        +1, because, steps_done=0, means we did, the first step.
        :return:
        """
        return (self.steps_done + 1) < len(self)

    def __repr__(self):
        return f"Collection-Task: {self.task_name} ({self.platform})"


class BaseEnvSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH,
                                      case_sensitive=True,
                                      env_file_encoding='utf-8',
                                      extra='allow')