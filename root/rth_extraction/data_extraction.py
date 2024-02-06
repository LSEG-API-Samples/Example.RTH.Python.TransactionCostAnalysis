import aiohttp
import asyncio
import gzip
import pandas as pd
import json
class DataExtraction:

    def __init__(self, file_path, config_file_path, token, request_type) -> None:
        self.file_path = file_path
        self.token = token
        # self.use_aws = False
        self.semaphore = asyncio.Semaphore(35)
        self.session = aiohttp.ClientSession()
        self.request_type = request_type
        self.config_file_path = config_file_path
        self.load_config_file()

    def load_config_file(self):
        global data_type, content_fields, request_conditions
        try:
            f = open(self.config_file_path)
            data = json.load(f)
            data_type = data['rth_request_type'][self.request_type]['data_type']
            content_fields = data['rth_request_type'][self.request_type]['content_fields']
            request_conditions = data['rth_request_type'][self.request_type]['conditions']
            print("Read credentials from file")
        except Exception:
            pass

        return data_type, content_fields, request_conditions


    async def close_session(self):
        await self.session.close()
        
    async def extract_tick_data(self, ric, start, end, signal_time):
        async with self.semaphore:
            request_url, request_headers, request_body = self.get_request_details(self.token, ric, start, end)
            job_id = await self.get_extraction_output(request_url, request_headers, request_body)
            await self.save_extraction_results(job_id, self.token, ric, signal_time)

    def get_request_details(self, token, ric, start, end):
        request_url = "https://selectapi.datascope.refinitiv.com/RestApi/v1/Extractions/ExtractRaw"
        print(ric, start, end)
        request_headers = {
            "Prefer": "respond-async",
            "Content-Type": "application/json",
            "Authorization": "token " + token
        }
        
        request_conditions['QueryStartDate'] = start
        request_conditions['QueryEndDate'] = end
        request_conditions['DisplaySourceRIC'] = True
        if self.request_type == 'tick_history':
            request_conditions["ApplyCorrectionsAndCancellations"] = False

        request_body = {
            "ExtractionRequest": {
                "@odata.type": data_type,
                "ContentFieldNames": content_fields,
                "IdentifierList": {
                    "@odata.type": "#DataScope.Select.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
                    "InstrumentIdentifiers": [
                        {"Identifier": ric, "IdentifierType": "Ric"}
                    ]
                },
                "Condition": request_conditions
            }
        }

        return request_url, request_headers, request_body
    
    async def get_extraction_output(self, request_url, request_headers, request_body):

        async with self.session.post(request_url, json=request_body, headers=request_headers, timeout = 600) as response:
            status_code = response.status
            print("HTTP status of the response: " + str(status_code))

            if status_code == 200:
                response_json = await response.json()
                job_id = response_json["JobId"]
                return job_id

            if status_code == 202:
                request_url = response.headers["location"]
                print('Extraction is not complete, we shall poll the location URL:')
                job_id = await self.wait_for_completion(status_code, request_url, request_headers) # type: ignore
                return job_id

    async def wait_for_completion(self, status_code, request_url, request_headers):
        while (status_code == 202):
            print('As we received a 202, we wait 15 seconds, then poll again (until we receive a 200)')
            await asyncio.sleep(15)
            async with self.session.get(request_url, headers=request_headers, timeout = 600) as response:
                status_code = response.status
                print('HTTP status of the response: ' + str(status_code))

                if status_code == 200:
                    response_json = await response.json()
                    job_id = response_json["JobId"]
                    return job_id

    async def save_extraction_results(self, job_id, token, ric, signal_time):
        request_url = "https://selectapi.datascope.refinitiv.com/RestApi/v1/Extractions/RawExtractionResults" + \
            "('" + job_id + "')" + "/$value"

        # if self.use_aws:
        #     request_headers = {
        #         "Prefer": "respond-async",
        #         "Content-Type": "text/plain",
        #         "Accept-Encoding": "gzip",
        #         "X-Direct-Download": "true",
        #         "Authorization": "token " + token
        #     }
        # else:
        request_headers = {
                "Prefer": "respond-async",
                "Content-Type": "text/plain",
                "Accept-Encoding": "gzip",
                "Authorization": "token " + token
            }

        async with self.session.get(request_url, headers=request_headers) as response:
            # if self.use_aws:
            #     print('Content response headers (AWS server): type: ' +
            #             response.headers["Content-Type"] + '\n')
            # else:
            print('Content response headers (TRTH server): type: ' +
                    response.headers["Content-Type"] + ' - encoding: ' + response.headers["Content-Encoding"] + '\n')
        
            await self.write_output_to_gzip(response, ric, signal_time)

    async def write_output_to_gzip(self, response, ric, signal_time):
        
        file_name_root = f"{ric.split('.')[0]}_{signal_time[:22].replace(':', '-')}"
        file_name = self.file_path + file_name_root + ".csv.gz"
        print('Saving compressed data to file: ' + file_name + ' ... please be patient')
        chunk_size = 1024
        with gzip.open(file_name, 'wb') as fd:
            async for chunk in response.content.iter_chunked(chunk_size):
                fd.write(chunk)
        fd.close()
        self.add_signal_time(file_name, signal_time)
        print('Finished saving compressed data to file: ' + file_name + '\n')

    def add_signal_time(self, file_name, signal_time):
        data = pd.read_csv(file_name, compression='gzip')
        data['signal_time'] = signal_time
        data.to_csv(file_name, compression='gzip')