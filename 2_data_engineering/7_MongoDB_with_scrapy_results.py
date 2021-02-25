# -----Импорт пакетов-------------------------------------------------------------
import pprint

from pymongo import MongoClient
import json

# -----Подключение----------------------------------------------------------------

# установить соединение с MongoClient
client = MongoClient(host='localhost', port=27017)

# удалить БД (каждый раз начинаем "с чистого листа")
client.drop_database('habr')

# создать БД
db = client['habr']

# создать коллекцию
habr_news_collection = db.news_collection

# ------Добавление------------------------------------------------------------------------------------------------------

# загрузить json полученный в результате работы scrapy
with open('./habr_news/habr_news.json') as json_file:
    habr_news = json.load(json_file)

# добавить все данные из json в mongodb
insert_many_result = db.news_collection.insert_many(habr_news)
print(f"inserted {len(insert_many_result.inserted_ids)} documents")

# ------Запросы---------------------------------------------------------------------------------------------------------

# количество документов в коллекции
habr_news_count = db.news_collection.count_documents({})
print(f"{habr_news_count} documents")

# получить имена коллекций из БД
habr_collection_names = db.list_collection_names()
print(f"collection names {habr_collection_names}")

# получить один любой документ из коллекции
habr_news_anyone = db.news_collection.find_one()
print(f"anyone news {pprint.pformat(habr_news_anyone)}")

# получить один документ из коллекции удовлетворяющий условию {'news_id': 542446}
habr_news_542446 = db.news_collection.find_one({'news_id': 542446})
print(f"anyone news {pprint.pformat(habr_news_542446)}")

# получить все документы из коллекции удовлетворяющие условию {'comments_counter': 3} + сортировка по 'news_id'
habr_news_with_3_comment = db.news_collection.find({'comments_counter': 3}).sort("news_id")
for doc in habr_news_with_3_comment:
    print(f"news with 3 comments {pprint.pformat(doc)}")

# получить все документы из коллекции удовлетворяющие условию {'author': 'maybe_elf'}
maybe_elf_habr_news = db.news_collection.find({'author': 'maybe_elf'})
for doc in maybe_elf_habr_news:
    print(f"maybe_elf habr news {pprint.pformat(doc)}")

# получить количество документов из коллекции поле tags которых содержит `facebook` (другие теги тоже допустимы)
facebook_habr_news_count = db.news_collection.count_documents({'tags': 'facebook'})
print(f"facebook news count {facebook_habr_news_count}")

# ------Обновление------------------------------------------------------------------------------------------------------

# установить в качестве `author` имя `MONGO`
# во всех документах удовлетворяющие условию {'hubs': {'$all': ['Астрономия']}}
# и получить количество обновленных
update_many_result = db.news_collection.update_many({'hubs': {'$all': ['Астрономия']}}, {'$set': {'author': 'MONGO'}})
print(f"updated {update_many_result.matched_count} documents")

# получить количество документов из коллекции, в которых 'author' = 'MONGO'
mongo_author_news_count = db.news_collection.count_documents({'author': 'MONGO'})
print(f"MONGO is author of {mongo_author_news_count} documents")

# ------Удаление--------------------------------------------------------------------------------------------------------

# удалить все документы, у которых 'comments_counter' равен 0
# и получить количество удаленных
delete_many_result = db.news_collection.delete_many({'comments_counter': 0})
print(f"deleted {delete_many_result.deleted_count} documents")
