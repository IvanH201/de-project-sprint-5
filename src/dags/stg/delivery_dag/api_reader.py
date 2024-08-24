
import requests

class ApiConnect:
    def __init__(self,
                 entity: str,
                 sort: str,
                 limit: str,
                 offset: str
                 ) -> None:

        self.entity = entity
        self.sort = sort
        self.limit = limit
        self.offset = offset


    def url(self) -> str:
        return f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{self.entity}'


    def client(self):
        nickname = "IvanH"
        cohort = "27"
        apy_key = "25c27781-8fde-4b30-a22e-524044a7580f"


        headers = {
            'X-Nickname': nickname,
            'X-Cohort': cohort,
            'X-API-KEY': apy_key,
        }
    
        response = requests.get(self.url(), headers=headers, params={
            'sort_field': str(self.sort),
            'sort_direction': 'asc',
            'limit': str(self.limit),
            'offset': str(self.offset),
        })

        if response.status_code == 200:
            dict_list_res = response.json()
        else:
            print("Error:", response.status_code, response.reason)

        return dict_list_res
    