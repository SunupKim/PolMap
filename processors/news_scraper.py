import requests
import time
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd

class NewsScraper:
    """
    네이버 뉴스 본문 크롤링 전문 객체
    - Step 4 로직 담당
    """
    def __init__(self, timeout: int = 7, delay: float = 0.1):
        self.timeout = timeout
        self.delay = delay
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

    def fetch_contents(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        
        #print(f"[Scraper] 총 {len(df)}건의 기사 본문 수집 시작...")
        contents = []
        
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Scraping News"):
            content = self._scrape_article(row['link'])
            contents.append(content)
            time.sleep(self.delay)
            
        df['content'] = contents
        return df

    def _scrape_article(self, url: str) -> str:
        try:
            resp = requests.get(url, headers=self.headers, timeout=self.timeout)
            if resp.status_code != 200: return ""
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            # 네이버 뉴스 주요 본문 셀렉터들
            targets = [
                soup.find('article', id='dic_area'),
                soup.find('div', id='newsct_article'),
                soup.find('div', id='articleBodyContents')
            ]
            
            for target in targets:
                if target:
                    # 노이즈(스크립트, 스타일 등) 제거
                    for s in target(['script', 'style', 'iframe', 'span', 'em']):
                        s.decompose()
                    return target.get_text(separator=' ', strip=True)
        except:
            pass
        return ""