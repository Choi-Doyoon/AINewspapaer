import requests
from bs4 import BeautifulSoup

# 1. 대상 URL
url = "http://www.sisaon.co.kr/news/articleView.html?idxno=173756"

# 2. headers 설정 (User-Agent 없으면 차단되는 경우 있음)
headers = {
    "User-Agent": "Mozilla/5.0"
}

# 3. HTML 요청 및 파싱
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, 'html.parser')

# 4. 기사 제목 크롤링
title = soup.find('h1', class_='headTitle')
if title:
    print("[기사 제목]")
    print(title.text.strip())
else:
    print("제목을 찾을 수 없습니다.")

# 5. 기사 본문 크롤링
content_div = soup.find('div', id='article-view-content-div')
if content_div:
    print("\n[기사 본문]")
    # <br>이나 <p> 등을 고려해서 텍스트 추출
    paragraphs = content_div.find_all(['p', 'br'])
    for tag in content_div.stripped_strings:
        print(tag)
else:
    print("본문을 찾을 수 없습니다.")
