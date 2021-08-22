import pandas as pd
import numpy as np
import os

class Dataset:
    """ Класс работы с датасетом """
    __filename__ = None
    __df__ = None
    __save_chunk__ = None

    def __init__(self, filename=None, save_chunk=1000):
        self.__filename__ = filename
        self.__save_chunk__ = save_chunk
        try:
            self.__df__ = pd.read_csv(self.__filename__)
            print(f"df shape {self.__df__.shape} <<-- {self.__filename__}")
        except FileNotFoundError:
            print(f"FileNotFoundError {self.__filename__} ")

    def save(self):
        df = self.__df__
        if df is not None:
            df.to_csv(self.__filename__)
            print(f"df shape {self.__df__.shape} --> {self.__filename__}")

    def exist(self, key, key_name):
        df = self.__df__
        return df[df[key_name] == key].empty == False if df is not None else False

    def append(self, df):
        self.__df__ = df if self.__df__ is None else self.__df__.append(df)
        if self.__df__.shape[0] % self.__save_chunk__ == 0:
            self.save()

