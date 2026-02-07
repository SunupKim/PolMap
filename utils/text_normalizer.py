# utils/text_normalizer.py
import html
import re

def normalize_html_text(text: str) -> str:
    if not text:
        return text
    # 1) HTML entity unescape (&quot; -> ", <b> 등)
    text = html.unescape(text)
    # 2) <b>, </b> 태그만 제거
    text = re.sub(r"</?b>", "", text)
    return text.strip()

class NewsTextNormalizer:
    BRACKET_PATTERNS = [
        r"\[.*?\]",     # [뉴스1전북]
        r"\(.*?\)",     # (속보)
        r"<.*?>",       # <단독>
        r"【.*?】",     # 【여론조사】
    ]

    @classmethod
    def normalize_title(cls, text: str) -> str:
        if not text:
            return ""
        text = normalize_html_text(text)
        for p in cls.BRACKET_PATTERNS:
            text = re.sub(p, "", text)
        return text.strip()
