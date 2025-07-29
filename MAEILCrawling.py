import feedparser
from pymongo import MongoClient
from datetime import datetime

import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


# RSS 피드 URL 목록
RSS_FEEDS = {
    "HEADLINE": "https://www.mk.co.kr/rss/30000001/",
    "ALL": "https://www.mk.co.kr/rss/40300001/",
    "POLITICS":"https://www.mk.co.kr/rss/30200030/",
    "SOCIETY":"https://www.mk.co.kr/rss/50400012/",
    "BUSINESS":"https://www.mk.co.kr/rss/50100032/",
    "STOCK":"https://www.mk.co.kr/rss/50200011/",
    "ENTERTAINMENT":"https://www.mk.co.kr/rss/30000023/",
    "SPORTS":"https://www.mk.co.kr/rss/71000001/",
    "MBA":"https://www.mk.co.kr/rss/40200124/",
    "MINIANDRICHES":"https://www.mk.co.kr/rss/40200003/",
    "ECONOMY_ALL":"https://www.mk.co.kr/rss/50000001/",
    "CITYLIFE_ALL":"https://www.mk.co.kr/rss/60000007/"
}

# MongoDB 연결
client = MongoClient("mongodb://localhost:27017/")
db = client["maeil"]

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
    crawl_rss(rss_url, collection, "Maeil Economy")
