import json
import requests


class Authorization:
    def __init__(self, credentials_file) -> None:
        self.credentials_file = credentials_file

    def get_token(self):
        username, password = self.load_credentials_from_file()
        token = self.request_token(username, password)
        return token

    def load_credentials_from_file(self):
        global username, password
        try:
            f = open(self.credentials_file)
            data = json.load(f)
            username = data['creds']['username']
            password = data['creds']['password']

            print("Read credentials from file")
        except Exception as e:
            print(e)

        return username, password

    def request_token(self, username, password):
        request_url = "https://selectapi.datascope.refinitiv.com/RestApi/v1/Authentication/RequestToken"

        request_headers = {
            "Prefer": "respond-async",
            "Content-Type": "application/json"
        }

        request_body = {
            "Credentials": {
                "Username": username,
                "Password": password
            }
        }
        print(request_body)
        response = requests.post(request_url, json=request_body,
                                 headers=request_headers)

        if response.status_code == 200:
            json_response = json.loads(response.text.encode('ascii', 'ignore'))
            token = json_response["value"]
            print('Authentication token (valid 24 hours):')
            return token
        else:
            print('Provide valid credentials, then repeat the request')
