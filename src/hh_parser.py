from typing import Any

import requests


class HHParser:
    url: str
    params: dict[str, object]

    def __init__(self) -> None:
        self.__url = None
        self.__params = None

    def __get_request(self) -> Any:
        """Функция проверки подключения к API hh.ru"""
        response = requests.get(self.__url, self.__params)
        if response.status_code == 200:
            return response.json()["items"]

    def get_employers(self) -> Any:
        """Функция, которая возвращает json-ответ с списком работодателей"""
        self.__url = "https://api.hh.ru/employers"
        self.__params = {"sort_by": "by_vacancies_open", "per_page": 10}
        return self.__get_request()

    def get_vacancies(self, employer_id: int) -> Any:
        """Функция, которая возвращает json-ответ с списком вакансий по employer_id"""
        self.__url = "https://api.hh.ru/vacancies"
        self.__params = {"employer_id": employer_id, "per_page": 100}
        return self.__get_request()


# hh = HHParser()
# print(hh.get_vacancies(1942330))
