import asyncio
import json
from contextlib import aclosing
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import select
import orjson
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from twscrape import API
from twscrape.api import API as TwitterAPI
from twscrape.models import Tweet

from src.const import ENV_FILE_PATH, BASE_DATA_PATH
from src.db.db_models import DBPost
from src.db.db_session import db_manager, init_db


class TwitterSetting(BaseSettings):
    TWITTER_USERNAME: str
    TWITTER_PASSWORD: SecretStr
    TWITTER_EMAIL: str
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')


class TwitterClient:

    def __init__(self):
        self.api = self.get_api()

    def get_api(self) -> TwitterAPI:
        return API()

    async def main(self):
        api = self.api  # or API("path-to.db") - default is `accounts.db`

        # ADD ACCOUNTS (for CLI usage see BELOW)
        settings = TwitterSetting()
        await api.pool.add_account(settings.TWITTER_USERNAME, settings.TWITTER_PASSWORD.get_secret_value(),
                                   settings.TWITTER_EMAIL, settings.TWITTER_PASSWORD.get_secret_value())
        await api.pool.login_all()
        return api

    async def search(self, q: str):
        return self.api.search(q)


    async def test_grab(self, from_time: datetime, until_inc: timedelta):
        # api = asyncio.run(main())
        #print(client.api)
        # loop = asyncio.get_event_loop()
        # result = loop.run_until_complete(client.search("bill gates"))
        tweets = []

        async def process_results(from_: str, until: str, filter: Optional[str], max: int = 100):
            try:
                q = f"lang:en since:{from_} until:{until}"
                if filter:
                    q += f" {filter}"
                async with aclosing(self.api.search(q)) as gen:
                    async for tweet in gen:
                        tweets.append(tweet.dict())
                        if len(tweets) >= max:
                            break
            except Exception as e:
                print(e)

        until = from_time + until_inc
        from_s = from_time.strftime("%Y-%m-%d_%H:%M:%S_UTC")
        until_s = until.strftime("%Y-%m-%d_%H:%M:%S_UTC")
        filter = "-filter:replies"
        max_tweets = 5
        await process_results(from_s, until_s, filter, max_tweets)
        #loop.run_until_complete(process_results(from_s, until_s, filter, max_tweets))
        dd = orjson.dumps(tweets)
        dest  = BASE_DATA_PATH / f"test/twitter/twitter.2023{from_s}_{until_s}.json"
        dest.write_text(dd.decode("utf-8"), encoding="utf-8")
        print(f"fumped {dest}")


def check_date():
    dates = []
    data = json.load((BASE_DATA_PATH / f"test/twitter/twitter.2023.json").open())
    for d in data:
        dates.append(datetime.fromisoformat(d["date"]))
    return dates


if __name__ == "__main__":


    async def run_test_grabs():
        client = TwitterClient()
        start_time = datetime.fromisoformat("2023-01-01_00:00:00")
        ent_time_delta = timedelta(minutes=1)
        inc: timedelta = timedelta(hours=1)
        current_t = start_time

        while True:
            await client.test_grab(current_t, inc)
            current_t += inc


    init_db()
    with db_manager.session_scope() as session:
        print(session.execute(select(DBPost)).all())
    # asyncio.run(run_test_grabs())
