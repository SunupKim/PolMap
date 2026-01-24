
# news_article_model.py

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
import hashlib
from email.utils import parsedate_to_datetime
import pytz

@dataclass(frozen=True)
class NewsArticleModel:    
    search_keyword: str
    title: str
    description: str
    link: str
    originallink: str
    pubDate: str
    content: str = "" # 본문 필드를 미리 정의해두면 나중에 편리합니다.

    # 내가 추가한 필드 2개
    # 수집 시각(collected_at)을 KST(Asia/Seoul) 기준 ISO 8601로 저장
    # 서버가 UTC 기준일 수 있으므로 명시적으로 타임존 지정
    
    collected_at: str = field(
        default_factory=lambda: datetime.now(pytz.timezone("Asia/Seoul")).isoformat()
    )    
    news_id: str = field(init=False)

    def __post_init__(self):
        object.__setattr__(            
            self,
            "news_id",
            hashlib.md5(self.link.encode()).hexdigest()[:12]                        
            )
        
        # pubDate: RFC 2822 → ISO 8601 변환
        try:
            dt = parsedate_to_datetime(self.pubDate)
            if dt is not None:
                # 타임존 없는 경우 대비
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                object.__setattr__(self, "pubDate", dt.isoformat())
        except Exception:
            # 파싱 실패 시 원본 유지 (의도적 침묵)
            pass
    
    def to_dict(self):
        """객체를 딕셔너리로 변환 (Pandas DataFrame 생성용)"""
        return asdict(self)

    @property
    def pubDate_dt(self) -> datetime | None:

        try:
            return parsedate_to_datetime(self.pubDate)
        except Exception:
            return None