from pandas import DataFrame
from tinkoff.invest import Client, InstrumentStatus, GetLastTradesRequest
from tinkoff.invest.services import InstrumentsService, MarketDataService
from tinkoff.invest.utils import now
import csv
from datetime import timedelta, datetime
from time import sleep

TOKEN = "t.MQPlrmlOFPU7bPvPH7K_WBqbfuHl9UcHQTlrlAexN8VNdPKcfpOHuAJC-Jhj_s5tw5PafIoNxElJftFxPObRdw"

def run():
    with Client(TOKEN) as cl:
        last_trades: MarketDataService = cl.market_data
        r = last_trades.get_last_trades(figi='BBG004730N88', from_=now() - timedelta(days=2), to=now())
        # with open('LastTrades.txt', 'a') as file:
        #     file.write(str(r.trades)+'\n')
        data = []
        for i in r.trades:
            price = i.price.units + i.price.nano / 1e9
            normalDay = i.time.strftime("%d")
            normalMonth = i.time.strftime("%m")
            normalYear = i.time.strftime("%Y")
            normalTime = i.time.strftime("%H:%M:%S")
            normalHours = i.time.strftime("%H")
            normalMins = i.time.strftime("%M")
            normalSec = i.time.strftime("%S")
            data.append({'figi': i.figi, 'direction': i.direction, 'price': price, 'quantity': i.quantity, 'day': normalDay, 'month': normalMonth, 'year': normalYear, 'time': normalTime, 'hours': normalHours, 'minutes': normalMins, 'seconds': normalSec})
        b = 1
        begin_time = '07:00'
        end_time = '21:00'
        time = datetime.utcnow()
        normal_time = time.strftime('%H:%M')
        if normal_time >= begin_time and normal_time <= end_time:
            while b < 434:
                with open(f"LastTrades_{b}", 'w') as f:
                    writer = csv.DictWriter(
                        f, fieldnames=list(data[0].keys()), quoting=csv.QUOTE_NONNUMERIC)
                    writer.writeheader()
                    for d in data:
                        writer.writerow(d)
                b += 1
                sleep(3600)
                return 0



if __name__ == '__main__':
    run()