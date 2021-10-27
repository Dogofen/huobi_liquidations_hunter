import requests
import pickle
import json
import logging
import datetime
from os.path import expanduser
import os
from time import sleep


class HuobiLiquidationHunter(object):
    base_html_string = "https://api.hbdm.com/linear-swap-api/v1/swap_liquidation_orders?contract_code="
    pairs = ['ETH-USDT', 'BTC-USDT', 'XRP-USDT']
    sides_dict = {
        'short': 'buy',
        'long': 'sell',
        'sell': 'long',
        'buy': 'short'
    }
    page_index = 1
    new_liqs = []
    liq_start_date = datetime.datetime.now() - datetime.timedelta(days=90)

    logger = False
    liqs = {}
    big_liqs = {}

    def __init__(self):
        name = 'liquidation_hunter'
        home = os.path.expanduser('~')

        # Prints logger info to terminal
        _logger = logging.getLogger()
        _logger.setLevel(logging.DEBUG)  # Change this to DEBUG if you want a lot more info
        ch = logging.StreamHandler()
        # create formatter
        _format = '%(asctime)s:%(levelname)s: %(message)s'
        filename = '%s/git/huobi_liquidations_hunter/{}.log'.format(name) % home
        logging.basicConfig(format=_format, filename=filename)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        ch.setFormatter(formatter)
        _logger.addHandler(ch)
        self.logger = _logger
        self.logger.debug("liquidations hunter init.")

    def get_liquidations(self, pair, page_index):
        req = requests.get("{}{}&trade_type=0&create_date=90&page_size=50&page_index={}".format(
            self.base_html_string, pair, page_index)
        )
        return json.loads(req.content)['data']

    def update_liquidations(self):
        for pair in self.pairs:
            page_index = 1
            new_liqs = []
            with open("liquidations.{}.huobi".format(pair), 'rb') as liq_file:
                liqs = pickle.load(liq_file)
            liquidations = self.get_liquidations(pair, page_index)
            total_pages = liquidations['total_page']
            break_loop = False
            updated = False
            while page_index < total_pages + 1:
                for liq_entry in liquidations['orders']:
                    if liq_entry in liqs:
                        break_loop = True
                        break
                    if not updated:
                        updated = True
                    new_liqs.append(liq_entry)
                if break_loop:
                    break
                page_index = page_index + 1
                liquidations = self.get_liquidations(pair, page_index)
            if updated:
                self.logger.info("{} liquidations were updated on {}".format(len(new_liqs), pair))
                liqs = new_liqs + liqs
                with open("liquidations.{}.huobi".format(pair), 'wb') as liq_file:
                    pickle.dump(liqs, liq_file)
            else:
                self.logger.debug("No liquidations were updated on {}.".format(pair))

    def create_15m_liquidations_chart(self):
        liq_15_dict = {}
        for pair in self.pairs:
            with open("liquidations.{}.huobi".format(pair), 'rb') as liq_file:
                liqs = pickle.load(liq_file)
            liq_dict = {}
            for x in liqs:
                dt = datetime.datetime.fromtimestamp(int(x['created_at'] / 1000))
                time = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute - dt.minute % 15)
                x['time'] = time.strftime("%d/%m/%Y, %H:%M")

                if not x['time'] in liq_dict.keys():
                    liq_dict[x['time']] = {"buy": 0, "sell": 0}
                liq_dict[x['time']][x['direction']] += x['amount']
            liq_15_dict[pair] = liq_dict
        with open('liquidations.15m.huobi', 'wb') as lq:
            pickle.dump(liq_15_dict, lq)
        return liq_15_dict

    def get_liqs_thresh_hold(self, side, liq_15m):
        thresh_hold = {}
        side = self.sides_dict[side]
        for pair in self.pairs:
            liqs = []
            for kl in liq_15m[pair].items():
                if kl[1][side] > 0 and datetime.datetime.strptime(kl[0], '%d/%m/%Y, %H:%M') > self.liq_start_date:
                    liqs.append(kl[1][side])
            liqs.sort()
            thresh_hold[pair] = [round(liqs[int(len(liqs) * 0.993)], 4), round(liqs[int(len(liqs) * 0.922)], 4)]
        os.system('echo {} > {}_thresh_hold'.format(thresh_hold, self.sides_dict[side]))
        return thresh_hold

    def alert_on_high_liquidations(self, side, liq_size):
        with open("liquidations.15m.huobi", 'rb') as liq_15file:
            liq_15m = pickle.load(liq_15file)

        thresh_hold = self.get_liqs_thresh_hold(side, liq_15m)

        with open("big.liquidations.huobi", 'rb') as liq_file:
            big_liqs = pickle.load(liq_file)

        _side = False
        now = datetime.datetime.now()
        if side == 'short':
            _side = 'buy'
        elif side == 'long':
            _side = 'sell'
        else:
            self.logger.error("long or short must be stated")
        for pair in self.pairs:
            for k in liq_15m[pair].items():
                if liq_size == 0:
                    liq_size_str = 'Big'
                else:
                    liq_size_str = 'Medium'
                discord_msg = "huobi exchange: {} {} {} liquidations detected: {}".format(
                    liq_size_str, pair, side, k
                )
                if k[1][_side] >= thresh_hold[pair][liq_size]:
                    if discord_msg in big_liqs:
                        continue
                    big_liqs.append(discord_msg)

                    self.logger.info(discord_msg)
                    liq_date = datetime.datetime.strptime(k[0], '%d/%m/%Y, %H:%M')
                    if liq_date > now - datetime.timedelta(seconds=18000) and liq_size_str == 'Big':
                        os.system('echo "{}" > {}/git/discord_alerts/discord_msg'.format(discord_msg, expanduser('~')))
                        sleep(30)
        with open('big.liquidations.huobi', 'wb') as lq:
            pickle.dump(big_liqs, lq)
