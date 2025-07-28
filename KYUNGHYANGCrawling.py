import feedparser
from pymongo import MongoClient
from datetime import datetime

import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


# RSS 피드 URL 목록
RSS_FEEDS = {
    "ALL": "https://www.khan.co.kr/rss/rssdata/total_news.xml",
    "OPINION":"https://www.khan.co.kr/rss/rssdata/opinion_news.xml",
    "ECONOMY":"https://www.khan.co.kr/rss/rssdata/economy_news.xml",
    "LOCAL":"https://www.khan.co.kr/rss/rssdata/local_news.xml",
    "CULTURE":"https://www.khan.co.kr/rss/rssdata/culture_news.xml",
    "SCIENCE":"https://www.khan.co.kr/rss/rssdata/science_news.xml",
    "HUMANITY":"https://www.khan.co.kr/rss/rssdata/people_news.xml",
    "NEWSLETTER":"https://www.khan.co.kr/rss/rssdata/newsletter_news.xml",
    "CARTOON":"https://www.khan.co.kr/rss/rssdata/cartoon_news.xml",
    "POLITICS":"https://www.khan.co.kr/rss/rssdata/politic_news.xml",
    "SOCIETY":"https://www.khan.co.kr/rss/rssdata/society_news.xml",
    "INTERNATIONAL":"https://www.khan.co.kr/rss/rssdata/kh_world.xml",
    "SPORTS":"https://www.khan.co.kr/rss/rssdata/kh_sports.xml",
    "LIFE":"https://www.khan.co.kr/rss/rssdata/life_news.xml",
    "ENGLISH":"https://www.khan.co.kr/rss/rssdata/english_news.xml",
    "INTERACTIVE":"https://www.khan.co.kr/rss/rssdata/interactive_news.xml"
}

# MongoDB 연결
client = MongoClient("mongodb://localhost:27017/")
db = client["kyunghyang"]

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
    crawl_rss(rss_url, collection, "Kyunghyang Shinmun")
