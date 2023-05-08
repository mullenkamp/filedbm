#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

"""
# import os
import io
import pathlib
from collections.abc import Mapping, MutableMapping
from typing import Any, Generic, Iterator, Union, Dict
import shutil
# from hashlib import blake2b

# import utils
from . import utils


#######################################################
### Classes


class FileDBM(MutableMapping):
    """

    """
    def __init__(self, db_path: str, flag: str = "r", n_bytes_key=2, n_bytes_value=4):
        """

        """
        fp = pathlib.Path(db_path)
        fp_exists = fp.exists()

        if flag == "r":  # Open existing database for reading only (default)
            write = False
            if not fp_exists:
                raise FileNotFoundError(db_path + ' not found.')
        elif flag == "w":  # Open existing database for reading and writing
            write = True
            if not fp_exists:
                raise FileNotFoundError(db_path + ' not found.')
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
        self._n_bytes_key = n_bytes_key
        self._n_bytes_value = n_bytes_value

        ## Load or assign encodings and attributes
        if not fp_exists:
            fp.mkdir(parents=True)

        self.db_path = fp


    def keys(self):
        for key in utils.iter_keys_values(self.db_path, True, False, self._n_bytes_key, self._n_bytes_value):
            yield key

    def items(self):
        for key, value in utils.iter_keys_values(self.db_path, True, True, self._n_bytes_key, self._n_bytes_value):
            yield key, value

    def values(self):
        for value in utils.iter_keys_values(self.db_path, False, True, self._n_bytes_key, self._n_bytes_value):
            yield value

    def __iter__(self):
        return self.keys()

    def __len__(self):
        return len([file for file in self.db_path.iterdir() if file.is_file()])

    def __contains__(self, key: str):
        file_hashes = set(file.name for file in self.db_path.iterdir() if file.is_file())
        key_hash = utils.hash_key(key.encode())
        return key_hash.hex() in file_hashes

    def get(self, key: str, default=None):
        value = utils.get_value(self.db_path, key.encode(), self._n_bytes_key, self._n_bytes_value)

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
        value = utils.get_value(self.db_path, key.encode(), self._n_bytes_key, self._n_bytes_value)

        if value is None:
            raise KeyError(key)
        else:
            return value


    def __setitem__(self, key: str, value: Union[bytes, io.IOBase]):
        if self._write:
            utils.write_data_block(self.db_path, key.encode(), value, self._n_bytes_key, self._n_bytes_value)
        else:
            raise ValueError('File is open for read only.')

    def __delitem__(self, key: str):
        if self._write:
            key_hash = utils.hash_key(key.encode())
            file_path = self.db_path.joinpath(key_hash.hex())

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

    # def sync(self):
    #     if self._write:
    #         if self._buffer_index:
    #             utils.flush_write_buffer(self._mm, self._write_buffer)
    #             self._sync_index()
    #         self._mm.seek(n_keys_pos)
    #         self._mm.write(utils.int_to_bytes(self._n_keys, 4))
    #         self._mm.flush()
    #         self._file.flush()



def open(
    db_path: str, flag: str = "r", n_bytes_key=2, n_bytes_value=4):
    """
    Open a persistent dictionary for reading and writing. All keys and values are stored in individual files within the db_path. Keys must be strings and values must be either bytes or file-objects.

    Parameters
    -----------
    db_path : str or pathlib.Path
        It must be a path to a folder. If the folder doesn't exist, it will be created if flags 'c' or 'n' are passed.

    flag : str
        Flag associated with how the file is opened according to the dbm style. See below for details.

    n_bytes_key : int
        The number of bytes to represent an integer of the max length of each key.

    n_bytes_value : int
        The number of bytes to represent an integer of the max length of each value.

    Returns
    -------
    Booklet

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
    return FileDBM(db_path, flag, n_bytes_key, n_bytes_value)
