from pymongo import MongoClient

from data.config import DATABASE

DB_CONF = DATABASE['mongodb']


class MongoHandler(object):
    "" "Создать класс обработки MongoDB" ""

    def __init__(self, **kwargs):
        #print(kwargs)
        self.db_name = 'test_db'
        client = MongoClient()
        self.db = client[self.db_name]  # Или указать так: db = client.test

    def drop(self):
        try:
            self.db.command("dropDatabase")
        except Exception as e:
            print("Delete DATABASE {0} failed! due to the reason of '{1}'".format(self.db_name, e))

    def collection(self, cl):
        return self.db[cl]  # Или указать так: cols = db.cl

    def insert_one_record(self, col, record: dict):
        """
             Вы можете напрямую вызвать метод библиотеки с полученной комбинацией,
             Типы ограничены здесь для демонстрационных целей,
             Следующие методы похожи, переписаны в соответствии с потребностями бизнеса
        """
        result = col.insert_one(record)
        #print(result)

    def insert_many_records(self, col, records: list):
        result = col.insert_many(records)
        #print(result)

    def find_one_record(self, col, condition: dict):
        result = col.find_one(condition)

        return result

    def find_records(self, col, condition: dict):
        results = col.find(condition)
        return results

    def update_one_record(self, col, condition: dict, data: dict):
        result = col.update_one(condition, data)
        #print('«Влиять на количество изменений:»', result.matched_count, result.modified_count)
        return result

    def update_records(self, col, condition: dict, data: dict):
        result = col.update_many(condition, data)
        #print('«Влиять на количество изменений:»', result.matched_count, result.modified_count)
        return result

    def delete_one_record(self, col, condition: dict):
        result = col.delete_one({'name': 'Kevin'})
        # print(result)
        return result.deleted_count

    def delete_records(self, col, condition: dict):
        # result = col.remove ({'name': 'Kevin'}) # Удалить все записи, соответствующие условиям, официальная рекомендация - delete_one и delete_many
        result = col.delete_many(condition)
        # print(result.deleted_count)
        return result.deleted_count