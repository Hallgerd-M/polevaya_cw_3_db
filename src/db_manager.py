import os
from typing import Any

import psycopg2
from dotenv import load_dotenv

load_dotenv()


class DBManager:
    """Класс для работы с таблицами"""

    def __init__(self, db_name: str):
        self.__db_name = db_name

    def __execute_query(self, query: str) -> Any:
        """Функция для создания запроса в базу данных"""
        conn = psycopg2.connect(
            dbname=self.__db_name,
            user=os.getenv("user"),
            password=os.getenv("password"),
            host=os.getenv("host"),
            port=os.getenv("port"),
            options="-c client_encoding=UTF8",
        )
        with conn:
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchall()
        conn.close()
        return result

    def get_companies_and_vacancies_count(self) -> None:
        """функция выводит список всех компаний и количество вакансий у каждой компании"""
        results = self.__execute_query(
            """SELECT DISTINCT employer_name, COUNT(*) AS vacancies_count FROM employers
        JOIN vacancies ON employers.hh_id = vacancies.employer_id
        GROUP BY employer_name;"""
        )
        for result in results:
             print(f"В компании '{result[0]}' открыто {result[1]} вакансий.")

    def get_all_vacancies(self) -> None:
        """Функция выводит список всех вакансий с указанием названия компании,
        названия вакансии, зп, ссылки на вакансию"""
        results = self.__execute_query(
            """SELECT employer_name, vacancies.name, vacancies.salary_from,
        vacancies.salary_to, vacancies.currency, vacancies.url
        FROM vacancies
        JOIN employers ON employers.hh_id = vacancies.employer_id;"""
        )
        if results:
            for result in results:
                if result[2] == 0 and result[3] == 0:
                    print(
                        f"Вакансия '{result[1]}' в компании '{result[0]}' ({result[5]})"
                    )
                elif result[2] == 0:
                    print(
                        f"Вакансия '{result[1]}' в компании '{result[0]}' с з/п до {result[3]} {result[4]}({result[5]})"
                    )
                elif result[3] == 0:
                    print(
                        f"Вакансия '{result[1]}' в компании '{result[0]}' с з/п от {result[2]} {result[4]}({result[5]})"
                    )
                else:
                    print(
                        f"Вакансия '{result[1]}' в компании '{result[0]}' с з/п от {result[2]} до {result[3]} {result[4]}({result[5]})"
                    )

    def get_avg_salary(self) -> str:
        """Функция выводит среднюю зп по вакансиям"""
        result = self.__execute_query(
            """SELECT AVG(avg_salary)
        FROM (SELECT id, (salary_from + salary_to) / 2 avg_salary
        FROM (SELECT id, CASE salary_from
        WHEN 0 THEN salary_to
        WHEN null THEN salary_to
        ELSE salary_from
        END salary_from,
        CASE salary_to
        WHEN 0 THEN salary_from
        WHEN null THEN salary_from
        ELSE salary_to
        END salary_to
        FROM vacancies
        WHERE salary_from > 0 AND salary_to > 0));"""
        )
        return f"Средняя заработная плата по всем вакансиям = {round(result[0][0])} RUR"

    def get_vacancies_with_higher_salary(self) -> None:
        """Функция выводит список всех вакансий, у которых зп выше средней по всем вакансиям"""
        avg_salary_1 = self.__execute_query(
            """SELECT AVG(avg_salary)
        FROM (SELECT id, (salary_from + salary_to) / 2 avg_salary
        FROM (SELECT id, CASE salary_from
        WHEN 0 THEN salary_to
        WHEN null THEN salary_to
        ELSE salary_from
        END salary_from,
        CASE salary_to
        WHEN 0 THEN salary_from
        WHEN null THEN salary_from
        ELSE salary_to
        END salary_to
        FROM vacancies
        WHERE salary_from > 0 AND salary_to > 0));"""
        )
        avg_salary = round(avg_salary_1[0][0])
        results = self.__execute_query(
            f"""SELECT V.name, V.salary_from, V.salary_to, V.currency, E.employer_name
        FROM vacancies as V
        JOIN employers as E ON V.employer_id = E.hh_id
        JOIN (SELECT id, (salary_from + salary_to) / 2 avg_salary
        FROM (SELECT id, case salary_from
        WHEN 0 THEN salary_to
        WHEN null THEN salary_to
        ELSE salary_from
        END salary_from,
        CASE salary_to
        WHEN 0 THEN salary_from
        WHEN null THEN salary_from
        ELSE salary_to
        END salary_to
        FROM vacancies
        WHERE salary_from > 0
        AND salary_to > 0)
        WHERE (salary_from + salary_to) / 2 > {avg_salary}) AS T on V.id = T.id
        ORDER BY salary_to DESC;"""
        )
        if results:
            for result in results:
                if result[2] == 0:
                    print(
                        f"Вакансия '{result[0]}'с зарплатой от {result[1]} {result[3]} в компании'{result[4]}'"
                    )
                elif result[1] == 0:
                    print(
                        f"Вакансия '{result[0]}'с зарплатой до {result[2]} {result[3]} в компании'{result[4]}'"
                    )
                else:
                    print(
                        f"Вакансия '{result[0]}'с зарплатой от {result[1]} до {result[2]} {result[3]} в компании'{result[4]}'"
                    )

    def get_vacancies_with_keyword(self, keyword: str) -> None:
        """Функция выводит список всех вакансий, в названии которых  содержатся переданные слова"""
        key_word = keyword[1:]
        results = self.__execute_query(
            f"""SELECT vacancies.name, salary_avg, currency, employer_name
        FROM vacancies
        JOIN employers ON employers.hh_id = vacancies.employer_id
        WHERE vacancies.name LIKE '%{key_word}%';"""
        )
        if results:
            for result in results:
                if result[1] != 0:
                    print(
                        f"Вакансия '{result[0]}' со средней з/п {result[1]} {result[2]} в компании '{result[3]}'"
                    )
                else:
                    print(f"Вакансия '{result[0]}' в компании '{result[3]}'")
        else:
            return "Нет вакансий с заданным словом"


test1 = DBManager("test1")
result1 = test1.get_companies_and_vacancies_count()
print(result1)
