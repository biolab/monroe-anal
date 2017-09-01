import logging
from itertools import chain
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
    # pprint(getdf([ping], limit=1000))
    # pprint(getdf([modem], limit=1000))
    # print(table_timerange(gps, 472))
    # print(all_nodes())
    # print(nodes_per_table())

    # results = query_async([
    #     "SELECT sample(Operator, 100) as Operator,NodeId FROM modem WHERE time >= '2017-07-13T00:00:00.870000+00:00'   AND time <= '2017-07-13T23:59:59.780000+00:00' group by Iccid",
    #     "SELECT sample(CID, 100) as CID,NodeId FROM modem WHERE time >= '2017-07-13T00:00:00.870000+00:00'   AND time <= '2017-07-13T23:59:59.780000+00:00' group by Iccid",
    #     "SELECT sample(DeviceState, 100) as DeviceState,NodeId FROM modem WHERE time >= '2017-07-13T00:00:00.870000+00:00'   AND time <= '2017-07-13T23:59:59.780000+00:00' group by Iccid",
    #     "SELECT sample(DeviceMode, 100) as DeviceMode,NodeId FROM modem WHERE time >= '2017-07-13T00:00:00.870000+00:00'   AND time <= '2017-07-13T23:59:59.780000+00:00' group by Iccid",
    #     "SELECT sample(Frequency, 100) as Frequency,NodeId FROM modem WHERE time >= '2017-07-13T00:00:00.870000+00:00'   AND time <= '2017-07-13T23:59:59.780000+00:00' group by Iccid",
    #     "SELECT sample(MCC_MNC, 100) as MCC_MNC,NodeId FROM modem WHERE time >= '2017-07-13T00:00:00.870000+00:00'   AND time <= '2017-07-13T23:59:59.780000+00:00' group by Iccid",
    #     "SELECT sample(IP_Address, 100) as IP_Address,NodeId FROM modem WHERE time >= '2017-07-13T00:00:00.870000+00:00'   AND time <= '2017-07-13T23:59:59.780000+00:00' group by Iccid",
    #     "SELECT sample(ECIO, 100) as ECIO,NodeId FROM modem WHERE time >= '2017-07-13T00:00:00.870000+00:00'   AND time <= '2017-07-13T23:59:59.780000+00:00' group by Iccid",
    #     "SELECT sample(RSRQ, 100) as RSRQ,NodeId FROM modem WHERE time >= '2017-07-13T00:00:00.870000+00:00'   AND time <= '2017-07-13T23:59:59.780000+00:00' group by Iccid",
    #     "SELECT sample(RSSI, 100) as RSSI,NodeId FROM modem WHERE time >= '2017-07-13T00:00:00.870000+00:00'   AND time <= '2017-07-13T23:59:59.780000+00:00' group by Iccid",
    # ])
    # results = query_async(['SELECT * FROM modem_min GROUP BY *'])
    # df = pd.DataFrame(list(chain.from_iterable(rows for result in results for _, rows in result.items())))
    # print(df.describe())
    df = getdf('ping', resample='2h', interpolate=True)
    print(df.describe())
