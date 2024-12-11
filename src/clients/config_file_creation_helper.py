import json
from datetime import datetime, timedelta
from typing import Optional, Generator

from src.const import CLIENTS_TASKS_PATH, BASE_DATA_PATH
from tools.files import read_data


def time_interval_steps(from_date: datetime, to_date: datetime, delta: timedelta) -> Generator[datetime, None, None]:
    """

    :param from_date:
    :param to_date:
    :param delta:
    :return:
    """
    current_date = from_date
    yield current_date
    # steps: list[datetime] = [current_date]
    while current_date <= to_date:
        current_date += delta
        yield current_date


def simple_time_window_configs(
        from_date: datetime,
        to_date: datetime,
        delta: timedelta,
        time_boundary: Optional[timedelta] = None) -> Generator[dict[str, str], None, None]:

    def get_bounded_dt(dt: datetime) -> str:
        return (dt + time_boundary).isoformat() + "Z"

    time_steps_gen = list(time_interval_steps(from_date, to_date, delta))

    # yield base_time_config
    for time_step in time_steps_gen:
        step_time_config: dict[str, str] = {
            "from_time": time_step.isoformat() + "Z"
        }
        if time_boundary:
            step_time_config["to_time"] = get_bounded_dt(time_step)
            if time_boundary < timedelta():
                from_t = step_time_config["from_time"]
                step_time_config["from_time"] = step_time_config["to_time"]
                step_time_config["to_time"] = from_t
        yield step_time_config


def fit_into_config_data(config_data: dict, config_generator: Generator[dict[str, str], None, None]) -> dict:
    conf = config_data["collection_config"]
    for idx, gen_step in enumerate(config_generator):
        if idx >= len(conf):
            conf.append(gen_step)
        else:
            conf[idx].update(gen_step)
    return config_data


if __name__ == '__main__':
    # res = time_interval_steps(datetime(
    #     year=2024, month=9, day=1, hour=12
    # ), datetime.now(), timedelta(hours=2))
    # print(res)
    # for step in res:
    #     print(step.isoformat())
    # print(step.strftime("%Y-%m-%d %H:%M"))

    start = datetime(year=2023, month=1, day=1, hour=0)
    # end = datetime(year=2023, month=1, day=1, hour=3)
    end = datetime(year=2024, month=1, day=1, hour=0)

    time_window_data = simple_time_window_configs(start,
                                                  end,
                                                  timedelta(hours=1),
                                                  timedelta(hours=-1))

    # print(json.dumps(list(time_window_data), indent=2))
    prefac_path = BASE_DATA_PATH / "client_task_prefac"
    fp = prefac_path / "youtube-test.json"
    new_fp = CLIENTS_TASKS_PATH / "youtube-test2.json"
    config = fit_into_config_data(read_data(fp), time_window_data)
    print(len(config["collection_config"]))
    json.dump(config, new_fp.open("w", encoding="utf-8"), indent=2, ensure_ascii=False)
