import feedparser
from pymongo import MongoClient
from datetime import datetime

import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


# RSS 피드 URL 목록
RSS_FEEDS = {
    "ALL": "https://www.chosun.com/arc/outboundfeeds/rss/?outputType=xml",
    "POLITICS":"https://www.chosun.com/arc/outboundfeeds/rss/category/politics/?outputType=xml",
    "ECONOMY": "https://www.chosun.com/arc/outboundfeeds/rss/category/economy/?outputType=xml",
    "SOCIETY":"https://www.chosun.com/arc/outboundfeeds/rss/category/national/?outputType=xml",
    "INTERNATIONAL": "https://www.chosun.com/arc/outboundfeeds/rss/category/international/?outputType=xml",
    "CULTURE": "https://www.chosun.com/arc/outboundfeeds/rss/category/culture-life/?outputType=xml",
    "OPINION": "https://www.chosun.com/arc/outboundfeeds/rss/category/opinion/?outputType=xml",
    "SPORTS": "https://www.chosun.com/arc/outboundfeeds/rss/category/sports/?outputType=xml",
    "ENTERTAINMENT": "https://www.chosun.com/arc/outboundfeeds/rss/category/entertainments/?outputType=xml"
}

# MongoDB 연결
client = MongoClient("mongodb://localhost:27017/")
db = client["chosun"]

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
    crawl_rss(rss_url, collection, "Chosun Ilbo")
