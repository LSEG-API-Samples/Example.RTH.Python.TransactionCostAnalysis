from data_extraction import DataExtraction
from authorization import Authorization
from extraction_funcs import add_extraction_timeline, extract_tick_data_async
import asyncio

def extract(simulated_trades, request_type, save_path):
    config_path = 'configuration/rth_config.json'
    token = Authorization(config_path).get_token()
    if request_type == 'tick_history':
        simulated_trades = add_extraction_timeline(simulated_trades, 120000, 120000)
    de = DataExtraction(save_path,config_path, token, request_type)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(extract_tick_data_async(de, simulated_trades))
    asyncio.run(de.close_session())