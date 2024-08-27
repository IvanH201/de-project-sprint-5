
import requests
import datetime as dt

class ApiConnect:
    def __init__(self,
                 entity: str,
                 sort: str,
                 limit: str,
                 offset: str,
                 from_ts: str
                 ) -> None:

        self.entity = entity
        self.sort = sort
        self.limit = limit
        self.offset = offset
        self.from_ts = from_ts



    def url(self) -> str:
        return f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{self.entity}'


    def list_data(self):
        nickname = "IvanH"
        cohort = "27"
        apy_key = "25c27781-8fde-4b30-a22e-524044a7580f"


        headers = {
            'X-Nickname': nickname,
            'X-Cohort': cohort,
            'X-API-KEY': apy_key
        }

        if (self.entity=='couriers'):
            response = requests.get(self.url(), headers=headers, params={
                'sort_field': str(self.sort),
                'sort_direction': 'asc',
                'limit': str(self.limit),
                'offset': str(self.offset)
            })
        else:
            response = requests.get(self.url(), headers=headers, params={
                'sort_field': str(self.sort),
                'sort_direction': 'asc',
                'limit': str(self.limit),
                'offset': str(self.offset),
                'from': str(self.from_ts)
            })


        if response.status_code == 200:
            dict_list_res = response.json()
            return dict_list_res
        else:
            print("Error:", response.status_code, response.reason)


    