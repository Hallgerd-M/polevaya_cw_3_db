import os

import psycopg2
from dotenv import load_dotenv

from src.hh_parser import HHParser

load_dotenv()


def create_database(db_name: str) -> None:
    """Функция создает базу данных, в случае, если такая уже существует, предварительно удаляет ее"""
    conn = psycopg2.connect(
        dbname="postgres",
        user=os.getenv("user"),
        password=os.getenv("password"),
        host=os.getenv("host"),
        port=os.getenv("port"),
        options="-c client_encoding=UTF8",
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
    cur.execute(f"CREATE DATABASE {db_name}")
    cur.close()
    conn.close()


def create_tables(db_name: str) -> None:
    """Функция создает две таблицы в переданной базе данных: employers и vacancies"""
    conn = psycopg2.connect(
        dbname=db_name,
        user=os.getenv("user"),
        password=os.getenv("password"),
        host=os.getenv("host"),
        port=os.getenv("port"),
        options="-c client_encoding=UTF8",
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE employers (
            id SERIAL PRIMARY KEY,
            hh_id INTEGER NOT NULL UNIQUE,
            employer_name VARCHAR(255) NOT NULL,
            url VARCHAR(255))"""
            )

            cur.execute(
                """CREATE TABLE vacancies (
                       id SERIAL PRIMARY KEY,
                       hh_id INTEGER NOT NULL UNIQUE,
                       name VARCHAR(255) NOT NULL,
                       salary_from INTEGER,
                       salary_to INTEGER,
                       salary_avg INTEGER,
                       currency VARCHAR(10),
                       url VARCHAR(255),
                       employer_id INTEGER REFERENCES employers(hh_id) NOT NULL)"""
            )
    conn.close()


def insert_data(db_name: str) -> None:
    """Функция, заполняющая таблицы employers и vacancies из полученной json-строки"""
    conn = psycopg2.connect(
        dbname=db_name,
        user=os.getenv("user"),
        password=os.getenv("password"),
        host=os.getenv("host"),
        port=os.getenv("port"),
        options="-c client_encoding=UTF8",
    )
    with conn:
        with conn.cursor() as cur:
            hh = HHParser()
            employers = hh.get_employers()
            for employer in employers:
                employer_id = employer["id"]
                cur.execute(
                    """INSERT INTO employers (hh_id, employer_name, url) VALUES (%s, %s, %s)""",
                    (employer["id"], employer["name"], employer["alternate_url"]),
                )
                vacancies = hh.get_vacancies(employer_id)
                for vacancy in vacancies:
                    if not vacancy["salary"]:
                        salary_from = 0
                        salary_to = 0
                        salary_avg = 0
                        currency = None
                    elif not vacancy["salary"]["from"]:
                        salary_from = 0
                        salary_to = vacancy["salary"]["to"]
                        salary_avg = vacancy["salary"]["to"]
                        currency = vacancy["salary"]["currency"]
                    elif not vacancy["salary"]["to"]:
                        salary_from = vacancy["salary"]["from"]
                        salary_to = 0
                        salary_avg = vacancy["salary"]["from"]
                        currency = vacancy["salary"]["currency"]
                    else:
                        salary_from = vacancy["salary"]["from"]
                        salary_to = vacancy["salary"]["to"]
                        salary_avg = (
                            vacancy["salary"]["from"] + vacancy["salary"]["to"]
                        ) / 2
                        currency = vacancy["salary"]["currency"]
                    cur.execute(
                        """INSERT INTO vacancies (hh_id, name, salary_from, salary_to,
                                salary_avg, currency, url, employer_id)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                        (
                            vacancy["id"],
                            vacancy["name"],
                            salary_from,
                            salary_to,
                            salary_avg,
                            currency,
                            vacancy["alternate_url"],
                            employer_id,
                        ),
                    )


# create_database("test1")
# create_tables("test1")
# insert_data("test1")
