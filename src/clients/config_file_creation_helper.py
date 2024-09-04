import json
from datetime import datetime, timedelta
from typing import Optional, Generator


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
        time_boundary: Optional[timedelta] = None):
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

    print(json.dumps(list(simple_time_window_configs(start,
                                                     end,
                                                     timedelta(hours=2),
                                                     timedelta(minutes=15)
                                                     )), indent=2))
