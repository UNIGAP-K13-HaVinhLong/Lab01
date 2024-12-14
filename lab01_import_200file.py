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
for file_path in glob.glob('/home/long/UNIGAP/Lab01_File/*.json'):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

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
    print(f"Da import thanh cong  {file_path}")
conn.commit()
cur.close()
conn.close()