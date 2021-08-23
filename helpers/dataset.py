import os
from datetime import datetime
from enum import Enum
import pandas as pd

class Dataset:
    """ Класс работы с датасетом """
    __dataset_dir_path__ = None
    __df__ = None
    __save_chunk__ = None

    class FileExtension(Enum):
        H5 = '.h5'
        CSV = '.csv'
        SQL = '.sql'
        FEATHER = '.f'
        PICKLE = '.pkl'

    def get_file_path(self, file_extension: FileExtension):
        return os.path.join(self.__dataset_dir_path__, 'dataset' + file_extension.value)

    def __init__(self, path=None, save_chunk=1000):
        self.__dataset_dir_path__ = path
        self.__save_chunk__ = save_chunk
        file_path = self.get_file_path(self.FileExtension.PICKLE)
        try:
            self.__df__ = pd.read_pickle(file_path)
            print(f"{datetime.now()}: df shape {self.__df__.shape} <<-- {file_path}")
        except FileNotFoundError:
            print(f"{datetime.now()}: FileNotFoundError {file_path} ")

    def save(self):
        df = self.__df__
        if df is not None:
            file_path = self.get_file_path(self.FileExtension.PICKLE)
            # df.to_sql(self.get_file_path(self.FileExtension.SQL))
            df.to_pickle(file_path)
            df.to_csv(self.get_file_path(self.FileExtension.CSV), )
            print(f"{datetime.now()}: df shape {self.__df__.shape} --> {file_path}")

    def exist(self, key, key_name):
        df = self.__df__
        return df[df[key_name] == key].empty == False if df is not None else False

    def append(self, df):
        self.__df__ = df if self.__df__ is None else self.__df__.append(df)
        if self.__df__.shape[0] % self.__save_chunk__ == 0:
            self.save()

    def get_df(self):
        return self.__df__