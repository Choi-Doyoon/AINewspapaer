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
    "ALL":"https://www.segye.com/Articles/RSSList/segye_recent.xml",
    "POLITICS":"https://www.segye.com/Articles/RSSList/segye_politic.xml",
    "ECONOMY": "https://www.segye.com/Articles/RSSList/segye_economy.xml",
    "SOCIETY":"https://www.segye.com/Articles/RSSList/segye_society.xml",
    "INTERNATIONAL": "https://www.segye.com/Articles/RSSList/segye_international.xml",
    "COUNTRY":"https://www.segye.com/Articles/RSSList/segye_local.xml",
    "CULTURE": "https://www.segye.com/Articles/RSSList/segye_culture.xml",
    "OPINION": "https://www.segye.com/Articles/RSSList/segye_opinion.xml",
    "ENTERTAINMENT": "https://www.segye.com/Articles/RSSList/segye_entertainment.xml",
    "SPORTS": "https://www.segye.com/Articles/RSSList/segye_sports.xml",
    "PHOTO":"https://www.segye.com/Articles/RSSList/segye_photo.xml",
    "SPORTSWORLD_ALL":"https://www.sportsworldi.com/Articles/RSSList/sw_recent.xml",
    "SPORTSWORLD_SPORTS":"https://www.sportsworldi.com/Articles/RSSList/sw_sports.xml",
    "SPORTSWORLD_ENTERTAINMENT":"https://www.sportsworldi.com/Articles/RSSList/sw_entertainment.xml",
    "SPORTSWORLD_LIFE":"https://www.sportsworldi.com/Articles/RSSList/sw_life.xml",
    "WORLDFINANCE_ALL":"https://www.segyefn.com/views/rss/all_recent.xml",
    "WORLDFINANCE_FINANCE":"https://www.segyefn.com/views/rss/finance.xml",
    "WORLDFINANCE_INDUSTRY":"https://www.segyefn.com/views/rss/industry.xml",
    "WORLDFINANCE_PROPERTY":"https://www.segyefn.com/views/rss/property.xml",
    "WORLDFINANCE_STOCK":"https://www.segyefn.com/views/rss/stock.xml",
    "WORLDFINANCE_OPINION":"https://www.segyefn.com/views/rss/opinion.xml",
    "WORLDFINANCE_CSR":"https://www.segyefn.com/views/rss/csr.xml"
}

# MongoDB 연결
client = MongoClient("mongodb://localhost:27017/")
db = client["saegae"]

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