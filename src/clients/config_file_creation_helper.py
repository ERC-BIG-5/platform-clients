import json
from datetime import datetime, timedelta
from typing import Optional, Generator

from src.const import CLIENTS_TASKS_PATH
from src.misc.files import read_data


def time_interval_steps(from_date: datetime, to_date: datetime, delta: timedelta) -> Generator[datetime, None, None]:
    """

    :param from_date:
    :param to_date:
    :param delta:
    :return:
    """
    current_date = from_date
    # steps: list[datetime] = [current_date]
    while current_date <= to_date:
        current_date += delta
        yield current_date


def simple_time_window_configs(
        from_date: datetime,
        to_date: datetime,
        delta: timedelta,
        time_boundary: Optional[timedelta] = None) -> Generator[dict[str, str], None, None]:
    base_time_config: dict[str, str] = {"from_time": from_date.isoformat() + "Z"}

    def get_bounded_dt(dt: datetime) -> str:
        return (dt + time_boundary).isoformat() + "Z"

    if time_boundary:
        base_time_config["to_time"] = get_bounded_dt(from_date)
    time_steps_gen = time_interval_steps(from_date, to_date, delta)

    yield base_time_config
    for time_step in time_steps_gen:
        step_time_config: dict[str, str] = {
            "from_time": time_step.isoformat() + "Z"
        }
        if time_boundary:
            step_time_config["to_time"] = get_bounded_dt(time_step)
        yield step_time_config


def fit_into_config_data(config_data: dict, config_generator: Generator[dict[str, str], None, None]) -> dict:
    gen_base_data = next(config_generator)
    base = config_data.setdefault("base_collection_config", {})
    base.update(gen_base_data)
    steps = config_data.setdefault("collection_steps", [])
    for idx, gen_step in enumerate(config_generator):
        if idx >= len(steps):
            steps.append(gen_step)
        else:
            steps[idx].update(gen_step)
    return config_data


if __name__ == '__main__':
    # res = time_interval_steps(datetime(
    #     year=2024, month=9, day=1, hour=12
    # ), datetime.now(), timedelta(hours=2))
    # print(res)
    # for step in res:
    #     print(step.isoformat())
    # print(step.strftime("%Y-%m-%d %H:%M"))

    start = datetime(year=2024, month=9, day=1, hour=12)
    end = datetime.now()

    time_window_data = simple_time_window_configs(start,
                                                  end,
                                                  timedelta(hours=2),
                                                  timedelta(minutes=15)
                                                  )

    # print(json.dumps(list(time_window_data), indent=2))
    fp = CLIENTS_TASKS_PATH / "youtube-test.json"
    new_fp = CLIENTS_TASKS_PATH / "youtube-test_mod.json"
    config = fit_into_config_data(read_data(fp), time_window_data)
    json.dump(config, new_fp.open("w", encoding="utf-8"), indent=2, ensure_ascii=False)
