import json
import psycopg2
import glob

## Ket noi DB PSQL
conn = psycopg2.connect(
    host="localhost",
    database="lab01_tiki",
    user="postgres",
    password="halong123"
)
cur = conn.cursor()

## Tao table va cac cot
cur.execute("""
CREATE TABLE IF NOT EXISTS TIKI_PRODUCTS(
    id INTEGER PRIMARY KEY,
    name TEXT,
    url_key TEXT,
    price INTEGER,
    description TEXT,
    image_url TEXT
    )
""")

with open('/home/long/UNIGAP/Lab01_File/JSON_FILE_1.json','r',encoding='utf-8') as jsonfile:
    data = json.load(jsonfile)
    for product in data:
        cur.execute("""
            INSERT INTO TIKI_PRODUCTS (id, name, url_key, price, description, image_url)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """, (
                product['id'],
                product['name'],
                product['url_key'],
                product['price'],
                product['description'],
                json.dumps(product['image_url'])
            ))
conn.commit()
cur.close()
conn.close()