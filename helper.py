from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from decouple import config


class dbhelper:
    __host = config('SQL_HOST')
    __username = config('SQL_USER')
    __password = config('SQL_PASS')
    __database = config('SQL_DATABASE')

    _engine = None
    _Session = None

    def __init__(self) -> None:
        self.__str_connection = f'mysql+pymysql://{self.__username}:{self.__password}@{self.__host}/{self.__database}'

    def Open(self):
        """Tạo kết nối đến database mySQL"""
        self._engine = create_engine(self.__str_connection, echo=True)
        self._Session = sessionmaker(bind=self._engine)
        return True