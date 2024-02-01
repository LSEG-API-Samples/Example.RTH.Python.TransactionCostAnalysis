from rth_extraction.data_extraction import DataExtraction
from rth_extraction.authorization import Authorization
from rth_extraction.extraction_funcs import add_extraction_timeline, extract_tick_data_async
import asyncio

def extract(df, request_type, file_path):
    config_path = 'configuration/rth_config.json'
    token = Authorization(config_path).get_token()
    if request_type == 'tick_history':
        df = add_extraction_timeline(df, 120000, 120000)
    de = DataExtraction(file_path,config_path, token, request_type)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(extract_tick_data_async(de, df))
    asyncio.run(de.close_session())