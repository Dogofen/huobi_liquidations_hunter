from huobi_liquidations_hunter import HuobiLiquidationHunter
from time import sleep


bot = HuobiLiquidationHunter()

while True:
    try:
        bot.update_liquidations()
        bot.create_15m_liquidations_chart()
        bot.alert_on_high_liquidations('long', 0)
        bot.alert_on_high_liquidations('long', 1)
        bot.alert_on_high_liquidations('short', 0)
        bot.alert_on_high_liquidations('short', 1)
    except Exception as e:
        print(e)
        bot.logger.error(e)
    sleep(15*60)
    sleep(9)
