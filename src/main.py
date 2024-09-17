from db_creator import create_database, create_tables, insert_data
from db_manager import DBManager

print("Добро пожаловать в программу по работе с вакансиями с сайта hh.ru!")
"""print("Введите название базы данных:")
db_name = input()"""
print("Создание базы данных займёт время. Подождите, пожалуйста!")

create_database("test1")
create_tables("test1")
insert_data("test1")

test1 = DBManager("test1")
while True:
    print(
        """Выберите действие:
    1. Вывести топ-10 компаний и количество открытых вакансий в компаниях.
    2. Вывести все вакансии.
    3. Вывести среднюю зарплату по всем вакансиям.
    4. Вывести все вакансии, у которых зарплата выше средней по всем вакансиям.
    5. Вывести вакансии, содаржащие ключевое слово"""
    )
    answer = input()
    if answer == "1":
        result = test1.get_companies_and_vacancies_count()
        print(result)
    elif answer == "2":
        result = test1.get_all_vacancies()
        print(result)
    elif answer == "3":
        result = test1.get_avg_salary()
        print(result)
    elif answer == "4":
        result = test1.get_vacancies_with_higher_salary()
        print(result)
    elif answer == "5":
        print("Введите ключевое слово:")
        key_word = input()
        result = test1.get_vacancies_with_keyword(key_word)
        print(result)
    else:
        print("Вы не выбрали действие.")
