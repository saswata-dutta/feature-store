# -*- coding: utf-8 -*-

import time
from datetime import datetime

import pytz

IST = pytz.timezone("Asia/Kolkata")


def to_datetime(epoch_secs: int) -> datetime:
    return datetime.fromtimestamp(epoch_secs, tz=IST)


def to_partition(epoch_secs: int) -> str:
    dt = to_datetime(epoch_secs)
    return dt.strftime("y=%Y/m=%m/d=%d")


def current_epoch_millis() -> int:
    return int(round(time.time() * 1000))
