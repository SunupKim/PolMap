# naver_news_client.py
import json
import urllib.request
import urllib.parse
from models.news_article_model import NewsArticleModel
from utils.text_normalizer import normalize_html_text

class NaverNewsClient:
    """네이버 뉴스 검색 API 클라이언트
    - 무상태
    - pandas 사용 안 함
    - 저장 / 중복 제거 / 누적 없음
    - API → list[dict] 반환만 담당
    """

    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret

    def fetch_news(self, keyword, start=1, display=100):
        encText = urllib.parse.quote(keyword)
        url = (
            f"https://openapi.naver.com/v1/search/news.json"
            f"?query={encText}&display={display}&start={start}&sort=date"
        )

        request = urllib.request.Request(url)
        request.add_header("X-Naver-Client-Id", self.client_id)
        request.add_header("X-Naver-Client-Secret", self.client_secret)

        try:
            with urllib.request.urlopen(request) as response:
                if response.getcode() != 200:
                    return []

                data = json.loads(response.read().decode("utf-8"))
                items = data.get("items", [])

                # --- NewsArticleModel 적용 구간 ---
                processed_items = []
                for item in items:
                    # 1. 모델 객체 생성 (이 시점에 news_id 자동 생성 및 검증 발생)
                    article_obj = NewsArticleModel(
                        search_keyword=keyword,
                        title=normalize_html_text(item.get('title', '')),
                        description=normalize_html_text(item.get('description', '')),
                        link=item.get('link', ''),
                        originallink=item.get('originallink', ''),
                        pubDate=item.get('pubDate', '')
                    )
                    # 2. 다시 딕셔너리로 변환하여 리스트에 추가 (to_dict 활용)
                    processed_items.append(article_obj.to_dict())
                
                return processed_items
        except Exception as e:
            print(f"   [API Error] {e}")
            return []
    

    def fetch_news_batch(self, keyword, total_count=500): # total_count는 fallback용, 따라서 여기 숫자는 무의미함
        rows = []

        for start in range(1, total_count + 1, 100):
            rows.extend(self.fetch_news(keyword, start=start)) #실제 API 호출이 여기서 일어난다.
            if len(rows) >= total_count:
                break

        return rows[:total_count]

    