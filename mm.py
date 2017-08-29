import logging
from pprint import pprint

import pandas as pd

from monroe_anal import *
from monroe_anal.db import *


def init_logging():
    log = logging.getLogger()
    formatter = logging.Formatter('%(relativeCreated)d %(levelname)s: %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)


if __name__ == '__main__':
    init_logging()
    pprint(getdf([gps], limit=1000))
    print(table_timerange(gps, 472))
    print(all_nodes())
    print(nodes_per_table())
