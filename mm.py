import logging
from pprint import pprint

import pandas as pd

from monroe import *
from monroe.db import *


def init_logging():
    log = logging.getLogger()
    formatter = logging.Formatter('%(relativeCreated)d %(levelname)s: %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)


if __name__ == '__main__':
    init_logging()
    # pprint(pd.DataFrame(list(select(ping, limit=10))))
    # import time
    # time.sleep(20)
    # pprint(select(ping, limit=10))
    # time.sleep(20)
    # pprint(select(ping, limit=10))
    # pprint(getdf([ping, gps], limit=1000))
    # pprint(getdf([http], start_time='2017-01-01', limit=20))
    # pprint(getdf([event], start_time='2017-01-01', limit=20))
    # pprint(getdf([sensor], start_time='2017-01-01', limit=20))
    # pprint(getdf([modem], start_time='2017-08-01', limit=20))
    # pprint(getdf([traceroute], start_time='2017-08-01', limit=20))
    # pprint(getdf([traceroute], start_time='2017-08-01', limit=20))
    # pprint(unique_values(event))
    # pprint(all_nodes())
    pprint(getdf([ping], limit=1000))
