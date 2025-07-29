import feedparser
from pymongo import MongoClient
from datetime import datetime
import schedule
import time
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


# RSS 피드 URL 목록
RSS_FEEDS = {
    "ALL":"https://rss.donga.com/total.xml",
    "POLITICS": "https://rss.donga.com/politics.xml",
    "SOCIETY": "https://rss.donga.com/national.xml",
    "ECONOMY": "https://rss.donga.com/economy.xml",
    "INTERNATIONAL": "https://rss.donga.com/international.xml",
    "EDITORIALS": "https://rss.donga.com/editorials.xml",
    "SCIENCE": "https://rss.donga.com/science.xml",
    "CULTURE": "https://rss.donga.com/culture.xml",
    "SPORTS": "https://rss.donga.com/sports.xml",
    "HUMANITY": "https://rss.donga.com/inmul.xml",
    "HEALTH": "https://rss.donga.com/health.xml",
    "LEISURE": "https://rss.donga.com/leisure.xml",
    "BOOK": "https://rss.donga.com/book.xml",
    "SHOW": "https://rss.donga.com/show.xml",
    "WOMAN": "https://rss.donga.com/woman.xml",
    "TRIP": "https://rss.donga.com/travel.xml",
    "LIFEINFO": "https://rss.donga.com/lifeinfo.xml"
}

# MongoDB 연결
client = MongoClient("mongodb://localhost:27017/")
db = client["donga"]

def crawl_rss(rss_url, collection, source_name):
    feed = feedparser.parse(rss_url)
    for entry in feed.entries:
        article_id = entry.link
        if collection.find_one({"link": article_id}):
            continue
        doc = {
            "title": entry.title,
            "summary": entry.get("summary", ""),
            "link": article_id,
            "published": entry.get("published", ""),
            "published_parsed": datetime(*entry.published_parsed[:6]) if entry.get("published_parsed") else None,
            "source": source_name,
            "crawled_at": datetime.utcnow()
        }
        collection.insert_one(doc)
        print(f"[+] Saved to {collection.name}: {entry.title}")

# 주기적으로 전체 수집
def scheduled_job():
    print(f"\n[!] Running scheduled job at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    for category, rss_url in RSS_FEEDS.items():
        collection = db[category]
        collection.delete_many({})  # 기존 데이터 삭제
        print(f"[~] Cleared collection: {collection.name}")
        crawl_rss(rss_url, collection, "Chosun Ilbo")

# 1분마다 실행
schedule.every(1).minutes.do(scheduled_job)

# 처음 시작 시 한 번 실행
scheduled_job()

# 무한 루프 (스케줄러 동작)
while True:
    schedule.run_pending()
    time.sleep(1)
