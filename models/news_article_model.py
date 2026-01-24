
# news_article_model.py

from dataclasses import dataclass, field, asdict
from datetime import datetime
import hashlib
from email.utils import parsedate_to_datetime

@dataclass(frozen=True)
class NewsArticleModel:    
    search_keyword: str
    title: str
    description: str
    link: str
    originallink: str
    pubDate: str
    content: str = "" # 본문 필드를 미리 정의해두면 나중에 편리합니다.

    # 내가 추가한 필드
    collected_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    news_id: str = field(init=False)

    def __post_init__(self):
        object.__setattr__(            
            self,
            "news_id",
            hashlib.md5(self.link.encode()).hexdigest()[:12]                        
            )
    
    def to_dict(self):
        """객체를 딕셔너리로 변환 (Pandas DataFrame 생성용)"""
        return asdict(self)

    @property
    def pubDate_dt(self) -> datetime | None:

        try:
            return parsedate_to_datetime(self.pubDate)
        except Exception:
            return None