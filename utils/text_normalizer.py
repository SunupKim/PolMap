# 1. utils/text_normalizer.py (신규 파일)
import html
import re

def normalize_html_text(text: str) -> str:
    if not text:
        return text
    # 1) HTML entity unescape (&quot; -> ", <b> 등)
    text = html.unescape(text)
    # 2) <b>, </b> 태그만 제거
    text = re.sub(r"</?b>", "", text)
    return text