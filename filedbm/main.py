#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

"""
import os
import io
import pathlib
from collections.abc import Mapping, MutableMapping
from typing import Any, Generic, Iterator, Union, Dict, List
import shutil
from time import time
# from hashlib import blake2b

# import utils
from . import utils


#######################################################
### Classes


class FileDBM(MutableMapping):
    """

    """
    def __init__(self, db_path: str, flag: str = "r", buffer_size: int=512000, n_bytes_key: int=2, n_bytes_value: int=4, ttl: int=None):
        """

        """
        fp = pathlib.Path(db_path)
        fp_exists = fp.exists()

        if flag == "r":  # Open existing database for reading only (default)
            if not fp_exists:
                raise FileNotFoundError(db_path + ' not found.')
            write = False
            ttl = None
        elif flag == "w":  # Open existing database for reading and writing
            if not fp_exists:
                raise FileNotFoundError(db_path + ' not found.')
            write = True
        elif flag == "c":  # Open database for reading and writing, creating it if it doesn't exist
            write = True
        elif flag == "n":  # Always create a new, empty database, open for reading and writing
            write = True
            if fp_exists:
                shutil.rmtree(fp, ignore_errors=True)
            fp_exists = False
        else:
            raise ValueError("Invalid flag")

        self._write = write
        self._buffer_size = buffer_size
        self._n_bytes_key = n_bytes_key
        self._n_bytes_value = n_bytes_value
        self._ttl = ttl

        ## Load or assign encodings and attributes
        if not fp_exists:
            fp.mkdir(parents=True)

        self.db_path = fp


    def keys(self):
        for key in utils.iter_keys_values(self.db_path, True, False, self._n_bytes_key, self._n_bytes_value, self._ttl):
            yield key

    def items(self, keys: List[str]=None):
        if keys is None:
            for key, value in utils.iter_keys_values(self.db_path, True, True, self._n_bytes_key, self._n_bytes_value, self._ttl):
                yield key, value
        else:
            for key in keys:
                value = utils.get_value(self.db_path, key.encode(), self._n_bytes_key, self._n_bytes_value, self._ttl)
                yield key, value

    def values(self, keys: List[str]=None):
        if keys is None:
            for value in utils.iter_keys_values(self.db_path, False, True, self._n_bytes_key, self._n_bytes_value, self._ttl):
                yield value
        else:
            for key in keys:
                value = utils.get_value(self.db_path, key.encode(), self._n_bytes_key, self._n_bytes_value, self._ttl)
                yield value

    def __iter__(self):
        return self.keys()

    def __len__(self):
        count = 0

        for file_path in self.db_path.iterdir():
            if file_path.is_file():
                if self._ttl is not None:
                    if (time() - os.path.getmtime(file_path)) > self._ttl:
                        file_path.unlink()
                        continue
                count += 1

        return count

    def __contains__(self, key: str):
        key_hash_hex = utils.hash_key(key.encode())
        file_path = self.db_path.joinpath(key_hash_hex)

        if file_path.is_file():
            if self._ttl is not None:
                if (time() - os.path.getmtime(file_path)) > self._ttl:
                    file_path.unlink()
                    return False
            return True
        else:
            return False

    def get(self, key: str, default=None):
        value = utils.get_value(self.db_path, key.encode(), self._n_bytes_key, self._n_bytes_value, self._ttl)

        if value is None:
            return default
        else:
            return value

    def update(self, key_value_dict: Union[Dict[str, bytes], Dict[str, io.IOBase]]):
        """

        """
        if self._write:
            for key, value in key_value_dict.items():
                self[key.encode()] = value
        else:
            raise ValueError('File is open for read only.')


    def __getitem__(self, key: str):
        value = utils.get_value(self.db_path, key.encode(), self._n_bytes_key, self._n_bytes_value, self._ttl)

        if value is None:
            raise KeyError(key)
        else:
            return value


    def __setitem__(self, key: str, value: Union[bytes, io.IOBase]):
        if self._write:
            utils.write_data_block(self.db_path, key.encode(), value, self._n_bytes_key, self._n_bytes_value, self._buffer_size)
        else:
            raise ValueError('File is open for read only.')

    def __delitem__(self, key: str):
        if self._write:
            key_hash = utils.hash_key(key.encode())
            file_path = self.db_path.joinpath(key_hash)

            if not file_path.exists():
                raise KeyError(key)

            file_path.unlink()
        else:
            raise ValueError('File is open for read only.')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def clear(self):
        if self._write:
            for file in self.db_path.iterdir():
                if file.is_file():
                    file.unlink()
        else:
            raise ValueError('File is open for read only.')

    def close(self):
        pass

    # def __del__(self):
    #     self.close()



def open(
    db_path: str, flag: str = "r", buffer_size: int=512000, n_bytes_key: int=2, n_bytes_value: int=4, ttl: int=None):
    """
    Open a persistent dictionary for reading and writing. All keys and values are stored in individual files within the db_path. Keys must be strings and values must be either bytes or file-objects. In the future, I might add more flexibility for inputs and outputs.

    Parameters
    -----------
    db_path : str or pathlib.Path
        It must be a path to a folder. If the folder doesn't exist, it will be created if flags 'c' or 'n' are passed.

    flag : str
        Flag associated with how the file is opened according to the dbm style. See below for details.

    buffer_size : int
        The buffer memory size used for reading and writing. Defaults to 512000.

    n_bytes_key : int
        The number of bytes to represent an integer of the max length of each key.

    n_bytes_value : int
        The number of bytes to represent an integer of the max length of each value.

    ttl : int or None
        Give the database a Time To Live (ttl) lifetime in seconds. All objects will persist in the database for at least this length. The objects will be removed when any query is performed on the database. The default None will not assign a ttl. The ttl will only be used if the flag parameter is set to anything but "r".

    Returns
    -------
    FileDBM

    The optional *flag* argument can be:

    +---------+-------------------------------------------+
    | Value   | Meaning                                   |
    +=========+===========================================+
    | ``'r'`` | Open existing database for reading only   |
    |         | (default)                                 |
    +---------+-------------------------------------------+
    | ``'w'`` | Open existing database for reading and    |
    |         | writing                                   |
    +---------+-------------------------------------------+
    | ``'c'`` | Open database for reading and writing,    |
    |         | creating it if it doesn't exist           |
    +---------+-------------------------------------------+
    | ``'n'`` | Always create a new, empty database, open |
    |         | for reading and writing                   |
    +---------+-------------------------------------------+
    """
    return FileDBM(db_path, flag, buffer_size, n_bytes_key, n_bytes_value, ttl)
