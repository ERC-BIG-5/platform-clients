import asyncio
import json
from contextlib import aclosing
from datetime import datetime, timedelta
from random import randint
from typing import Optional

import orjson
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.exc import IntegrityError
from time import sleep
from twscrape import API
from twscrape.account import Account
from twscrape.api import API as TwitterAPI

from src.const import ENV_FILE_PATH, BASE_DATA_PATH
from src.db.db_mgmt import DatabaseManager
from src.db.db_models import DBPost
from src.db.platform_db_mgmt import PlatformDB


class TwitterSetting(BaseSettings):
    TWITTER_USERNAME: str
    TWITTER_PASSWORD: SecretStr
    TWITTER_EMAIL: str
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')


def post_url(data: dict) -> str:
    # like in the unpacker, but the api now contains 'username' not screenname
    return f"https://x.com/{data['user']['username']}/status/{data['id']}"


START_TIME = datetime.fromisoformat("2023-01-01_00:00:00")
MINUTES_OFFSET = timedelta(minutes=7)


class TwitterClient:

    def __init__(self):
        self.api = asyncio.run(self.get_api())
        self.db_mgmt = DatabaseManager(PlatformDB.get_platform_default_db("twitter"))


    @staticmethod
    async def get_api() -> TwitterAPI:
        api =  API()
        accounts:list[Account] = await api.pool.get_all()
        #await api.pool.delete_inactive()
        # ADD ACCOUNTS (for CLI usage see BELOW)
        settings = TwitterSetting()
        await api.pool.add_account(settings.TWITTER_USERNAME, settings.TWITTER_PASSWORD.get_secret_value(),
                                   settings.TWITTER_EMAIL, settings.TWITTER_PASSWORD.get_secret_value())
        # todo trigger, retry login.
        # res = await api.pool.login_all()
        return api

    async def search(self, q: str):
        return self.api.search(q)

    async def test_grab(self, from_time: datetime, until_time_delta: timedelta) -> list[dict]:

        tweets = []

        async def process_results(from_: str, until_: str, filter_: Optional[str], max_: int = 5):
            try:
                q = f"lang:en since:{from_} until:{until_}"
                if filter_:
                    q += f" {filter_}"
                async with aclosing(self.api.search(q, limit=5)) as gen:
                    async for tweet in gen:
                        tweets.append(tweet.dict())
                        if len(tweets) >= max_:
                            break
            except Exception as e:
                print(e)

        until = from_time + until_time_delta
        from_s = from_time.strftime("%Y-%m-%d_%H:%M:%S_UTC")
        until_s = until.strftime("%Y-%m-%d_%H:%M:%S_UTC")
        print(f"from: {from_s}, until: {until_s}")
        filter_ = "-filter:replies -filter:quote"
        max_tweets = 5
        await process_results(from_s, until_s, filter_, max_tweets)

        print(f"collected: {len(tweets)} tweets")
        return tweets

    def get_last_time(self) -> Optional[datetime]:
        with self.db_mgmt.get_session() as session:
            try:
                obj: DBPost = session.query(DBPost).order_by(DBPost.id.desc()).first()
                return obj.date_created
            except Exception as e:
                print(e)
                return None

    async def complete_collect(self):
        inc: timedelta = timedelta(hours=1)
        start_time = self.get_last_time()
        if start_time:
            start_time = start_time.replace(minute=0, second=0, microsecond=0)
            start_time = start_time + inc
        else:
            start_time = START_TIME

        start_time = start_time + MINUTES_OFFSET

        until_time_delta = timedelta(minutes=1)
        current_t = start_time
        dump_thresh = 10
        all_tweets: list[dict] = []

        while True:
            all_tweets.extend(await self.test_grab(current_t, until_time_delta))
            current_t += inc
            if len(all_tweets) >= dump_thresh:
                print("dumping tweets to db...")
                with self.db_mgmt.get_session() as session:
                    for tweet in all_tweets:
                        session.add(DBPost(
                            platform="twitter",
                            platform_id=tweet["id_str"],
                            post_url=post_url(tweet),
                            date_created=tweet["date"],
                            content=orjson.loads(orjson.dumps(tweet))
                        ))
                    while True:
                        try:
                            session.commit()
                            all_tweets.clear()
                            break
                        except IntegrityError as err:
                            print(err)
                            pass
                # print(session.execute(select(DBPost)).all())

            sleeptime = randint(5,15)
            print("sleeeping", sleeptime)
            sleep(sleeptime)

def check_date():
    dates = []
    data = json.load((BASE_DATA_PATH / f"test/twitter/twitter.2023.json").open())
    for d in data:
        dates.append(datetime.fromisoformat(d["date"]))
    return dates


if __name__ == "__main__":
    asyncio.run(TwitterClient().complete_collect())
