from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import json

uri = "mongodb+srv://user_rw:ExW8FrG8hLEdW4i@it-tep.jybvn6z.mongodb.net/?retryWrites=true&w=majority&appName=IT-tep"
client = MongoClient(uri, server_api=ServerApi('1'))

# перевірити звязок з MongoDB
try:
    client.admin.command('ping')
    print("Успішне підключення до MongoDB!")
except Exception as e:
    print(e)
    
db_name = "media_db"
db = client[db_name]
print("\nБаза даних : ", db)


collection_name = "movies"

# отримати доступ до колекції чи створити, якщо не існує
collection = db[collection_name]
if collection_name not in db.list_collection_names():
    print("Колекція створена.")
else:
    print("Колекція вже існує.")
    
    
# завантажити дані х JSON-файлу
with open('movies.json', 'r') as file:
    data = json.load(file)
    # вставити дані в колекцію
    collection.insert_many(data)
    print("Дані завантажені у MongoDB.")
    

count = collection.count_documents({})
print(f"Кількість фільмів у колекції '{collection_name}': {count}")


# пошук записів по рокам
query = {"year": {"$gte": 2000, "$lte": 2020}}
print(f"Кількість фільмів у колекції '{collection_name}' за період 2000-2020 : {collection.count_documents(query)}")


actor = "Tom Cruise"
query = {"cast": actor}
print(f"Кількість фільмів за участю '{actor}' : {collection.count_documents(query)}")

# закрити  підключення
client.close()