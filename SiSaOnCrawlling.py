import feedparser
from pymongo import MongoClient
from datetime import datetime

import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8'


# RSS 피드 URL 목록
RSS_FEEDS = {
    "ALL": "http://www.sisaon.co.kr/rss/allArticle.xml",
    "POPULAR": "http://www.sisaon.co.kr/rss/clickTop.xml",
    "PLANNING": "http://www.sisaon.co.kr/rss/S1N6.xml",
    "OPINION": "http://www.sisaon.co.kr/rss/S1N7.xml",
    "SIDAESANCHECK": "http://www.sisaon.co.kr/rss/S1N9.xml",
    "NEWS": "http://www.sisaon.co.kr/rss/S1N12.xml",
    "VISUALNEWS": "http://www.sisaon.co.kr/rss/S1N13.xml",
    "SCENE": "http://www.sisaon.co.kr/rss/S1N14.xml"
}

# MongoDB 연결
client = MongoClient("mongodb://localhost:27017/")
db = client["sisaon"]

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

# 전체 수집 실행
for category, rss_url in RSS_FEEDS.items():
    collection = db[category]
    crawl_rss(rss_url, collection, "SiSaOn")
