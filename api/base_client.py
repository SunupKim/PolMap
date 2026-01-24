# base_client.py
# A base API client class for making HTTP requests and parsing XML responses.

import requests
from bs4 import BeautifulSoup

class BaseAPIClient:
    def __init__(self, api_name, api_key, base_url, url_delimiter):
        # --- 무결성 체크 추가 ---
        if not api_key:
            raise ValueError(f"[{api_name}] API Key는 필수입니다. 빈 값을 넣을 수 없습니다.")
        if not base_url:
            raise ValueError(f"[{api_name}] Base URL이 정의되지 않았습니다.")
        self._api_name = api_name
        self._api_key = api_key
        self._base_url = base_url
        self._url_delimiter = url_delimiter

    @property    
    def base_url(self):
        return self._base_url
     
    @property
    def api_key(self):
        return self._api_key

    def build_url(self, base_url, delimiter="", *args):
        url = base_url
        for param in args:
            url += delimiter + str(param)
        return url
    
    def get_xml(self, url, **kwargs):
        r = requests.get(url, params=kwargs, timeout=10)
        r.raise_for_status()
        return BeautifulSoup(r.text, "xml")