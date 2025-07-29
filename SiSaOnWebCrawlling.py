import requests
from bs4 import BeautifulSoup
import re
import psycopg2

# ===== 웹 요청 및 디코딩 =====
url = "http://www.sisaon.co.kr/news/articleView.html?idxno=173771"
headers = {"User-Agent": "Mozilla/5.0"}
response = requests.get(url, headers=headers)

# EUC-KR 디코딩 → 깨진 문자 대체
html_text = response.content.decode('euc-kr', errors='replace')
soup = BeautifulSoup(html_text, 'html.parser')

# ===== 데이터 추출 =====
title_elem = soup.find('h1', class_='headTitle')
title = title_elem.text.strip() if title_elem else "제목 없음"

content_div = soup.find('div', id='article-view-content-div')
content_lines = []
if content_div:
    lines = list(content_div.stripped_strings)
    photo_keywords = ['사진[=:]', '사진제공', '사진출처', '이미지[=:]', 'Photo', 'photo', '뉴시스']
    for line in lines:
        if not any(re.search(keyword, line, re.IGNORECASE) for keyword in photo_keywords):
            content_lines.append(line)

content = "\n".join(content_lines)

# ===== UTF-8로 안전하게 재인코딩 =====
def safe_utf8(text):
    return text.encode('utf-8', errors='replace').decode('utf-8')

title = safe_utf8(title)
content = safe_utf8(content)

# ===== PostgreSQL 저장 =====
conn = None
cur = None

try:
    conn = psycopg2.connect(
        dbname="postgres",
        user="EZ0320",
        password="Excel*2002",
        host="database-1.c1iymg62ikbr.us-east-2.rds.amazonaws.com",
        port="5432"
    )
    cur = conn.cursor()

    # 테이블 생성
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sisaon_articles (
            id SERIAL PRIMARY KEY,
            title TEXT,
            content TEXT
        );
    """)

    # 데이터 삽입
    cur.execute("""
        INSERT INTO sisaon_articles (title, content)
        VALUES (%s, %s)
    """, (title, content))

    conn.commit()
    print("[+] 데이터 삽입 성공")

except Exception as e:
    print("오류 발생:", e)

finally:
    if cur: cur.close()
    if conn: conn.close()
