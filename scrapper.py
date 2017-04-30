import feedparser
import sqlite3
import datetime
import os
import mysql.connector
import newspaper

from newspaper import Article
from dateutil import parser
from functools import reduce

def scrap_articles(project_dir, batch_size, begin_index = 0):
    """ Download the articles from prior populated index and store them to mysql database. """
    
    # get the database files
    db_files = [f for f in os.listdir(project_dir) if f.endswith('.db')]
    
    # read the index from database files
    items = []
    read_links = set()
    for dbf in db_files:
        conn = sqlite3.connect(os.path.join(project_dir,dbf))
        cur = conn.cursor()
        
        rows = [row for row in cur.execute('SELECT link, publish_date FROM rss_items')]
        
        items.extend([r for r in rows if not r[0] in read_links])
        
        # update stored_links
        read_links.update([r[0] for r in rows])
        
        cur.close()
        conn.close()
    
    article_rows = []
    urls_download_later = []
    
    # process the articles in batch_size 
    count = 0
    for i in items:
        count = count + 1
        
        if count < begin_index:
            continue
        
        try:
            a = Article(i[0], language='id')
            
            # download the article
            a.download()
            
            # parse the article to extract the title, authors, text, publish date
            a.parse()
            
            if a.publish_date is None:
                a.publish_date = parser.parse(i[1])
            
            # prepare the data for inserting to mysql database
            article_rows.append((a.url, ','.join(a.authors), a.publish_date.strftime('%Y-%m-%d'), a.title, a.text, a.top_image))
        except:
            urls_download_later(i[0])
            print('Gagal mengunduh artikel ', a.url)
        
        if count % batch_size == 0:
            # execute the SQL command
            sql_insert_articles(article_rows)
            article_rows.clear()
            
            print('# articles processed: ', count)
    
    # insert the failed download
    if len(urls_download_later) > 0:
        sql_insert_download_later(urls_download_later)

def download_rss():
    """ Populate the article index from RSS feed and store it to database file using sqlite. """
    
    articles = []
    
    rss_sources = [('http://www.republika.co.id/rss',['/nasional/']), \
                ('http://feed.liputan6.com/rss2',['news.liputan6','pilkada.liputan6','regional.liputan6']), \
                ('http://rss.detik.com/index.php/detikcom_nasional',[]), \
                ('http://sindikasi.okezone.com/index.php/rss/1/RSS2.0',[]), \
                ('https://www.merdeka.com/feed/',[]), \
                ('http://rss.viva.co.id/get/nasional',[]), \
                ('http://www.suara.com/rss/news',[]), \
                ('http://www.tribunnews.com/rss',['/nasional/','/regional/','/metropolitan/']), \
                ('https://rss.tempo.co/index.php/teco/news/feed/start/0/limit/50/kanal/6',[]), \
                ('https://www.sindonews.com/feed',['nasional.sindo','daerah.sindo','metro.sindo']), \
                ('http://feed.metrotvnews.com/news',[]), \
                ('http://www.beritasatu.com/rss/nasional.xml',[]), \
                ('http://rimanews.com/rss.xml',['/nasional/','/budaya/']), \
                ('http://www.jpnn.com/index.php?mib=rss&id=215',[]), \
                ('http://www.antaranews.com/rss/nasional',[])]
                
    for url, str_contains in rss_sources:
        items = fetch_rss_feed(url, str_contains)
        articles.extend(items)
    
    kompas_items = fetch_from_website('http://nasional.kompas.com', ['/nasional.kompas','megapolitan.kompas','regional.kompas'])
    articles.extend(kompas_items)
    
    # insert into database file
    filename = datetime.date.today().strftime('%d%m%Y') + '.db'
    insert_into_db(filename, articles)

def download_website():
    """ Populate the article index directly from website and store it to database file using sqlite. """
    
    articles = []
    
    news_sources = [('http://www.republika.co.id',['/nasional/']), \
                ('http://www.liputan6.com',['news.liputan6','pilkada.liputan6','regional.liputan6']), \
                ('http://news.detik.com',['/berita']), \
                ('http://news.okezone.com',['news.okezone']), \
                ('https://www.merdeka.com',['/peristiwa','/politik','/uang','/jakarta']), \
                ('http://www.viva.co.id',['.news.viva']), \
                ('http://www.suara.com',['/news/']), \
                ('http://www.tribunnews.com',['/nasional/','/regional/','/metropolitan/']), \
                ('http://www.tempo.co',['nasional.tempo','pilkada.tempo','metro.tempo']), \
                ('http://www.sindonews.com',['nasional.sindo','daerah.sindo','metro.sindo','ekbis.sindo']), \
                ('http://www.metrotvnews.com',['news.metro']), \
                ('http://www.beritasatu.com',['/nasional/','/hukum/','/nusantara/','/megapolitan/','/bisnis/','/aktualitas/']), \
                ('http://www.rimanews.com',['/nasional/','/budaya/']), \
                ('http://www.jpnn.com',['/news/']), \
                ('http://www.antaranews.com',['/berita/']), \
                ('http://nasional.kompas.com',['/nasional.kompas','megapolitan.kompas','regional.kompas'])]
    
    for url, str_contains in news_sources:
        items = fetch_from_website(url, str_contains)
        articles.extend(items)
    
    insert_into_db('berita_new.db',articles)
    
    print('#articles: %d', len(rows))

def accept_url(str_list,str_url):
    """ Return True if any element in str_list is in the str_url. """
    return reduce(lambda y, z: y or z, map(lambda x: x in str_url, str_list))
    
def fetch_rss_feed(url, string_contains):
    """ Fetch the RSS entries from source url and filter by string_contains. """
    
    print('Fetching RSS from', url)
    
    d = feedparser.parse(url)
    items = [(item.id,item.link,item.title,item.published) for item in d.entries if 'id' in item]
    
    if len(string_contains) > 0:
        items = list(filter(lambda x: accept_url(string_contains,x[1]), items))
    
    return items

def fetch_from_website(url, string_contains):
    """ Fetch the RSS entries from website using newspaper library. """
    
    print('Fetching articles from website', url)

    # use newspaper library for scraping
    website = newspaper.build(url,memoize_articles=False)
    items = [(item.url,item.url,item.title,item.publish_date) for item in website.articles]
    
    if len(string_contains) > 0:
        items = list(filter(lambda x: accept_url(string_contains,x[1]), items))
    
    return items
    
def insert_into_db(db_filename,rows):
    """ Insert the entries to database file. """
    
    if not os.path.isfile(db_filename):
        conn = sqlite3.connect(db_filename)
        cur = conn.cursor()
        cur.execute('CREATE TABLE rss_items (id text, link text, title text, publish_date text)')
        conn.commit()
        conn.close()
        
    conn = sqlite3.connect(db_filename)
    cur = conn.cursor()
    
    # avoid duplicates
    existing_ids = [row[0] for row in cur.execute('SELECT id FROM rss_items')]
    rows = list(filter(lambda x : not x[0] in existing_ids, rows))
    
    # execute the insert statement
    cur.executemany('INSERT INTO rss_items VALUES(?,?,?,?)', rows)
    
    conn.commit()
    conn.close()
    
def sql_insert_articles(rows_to_insert):
    """ Insert the articles to the primary database (MySQL is used here). """
    
    cnx = mysql.connector.connect(user='...', password='...', host='...', database='berita')
    cursor = cnx.cursor()
    
    add_article = ('INSERT INTO articles(article_url,authors,publish_date,title,content,image_url) VALUES(%s,%s,%s,%s,%s,%s)')
    
    for row in rows_to_insert:
        try:
            cursor.execute(add_article, row)
        except:
            print('Gagal insert ke database ', row[0])
    
    cnx.commit()
    
    cursor.close()
    cnx.close()

def sql_insert_download_later(rows_to_insert):
    """ Insert the urls to download later. """
    cnx = mysql.connector.connect(user='...', password='...', host='...', database='berita')
    cursor = cnx.cursor()
    
    add_url = ('INSERT INTO download_later(article_url,last_attempt,repeated_n_times) VALUES(%s,%s,%s)')
    
    for url in rows_to_insert:
        try:
            cursor.execute(add_url, (url,datetime.date.today().strftime('%Y-%m-%d'),0))
        except:
            print('Gagal insert ke database ', url)
    
    cnx.commit()
    
    cursor.close()
    cnx.close()
