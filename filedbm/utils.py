#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 11:04:13 2023

@author: mike
"""
import pathlib
import os
import io
from hashlib import blake2b, blake2s
# from time import time
from typing import Any, Generic, Iterator, Union
import mmap
from time import time

############################################
### Parameters

key_hash_len = 13


############################################
### Classes


class FileObjectReadSlice(io.IOBase):
    def __init__(self, file_path: Union[pathlib.Path, str], offset: int, length: int):
        self.f = file_path
        self.f_offset = offset
        self.offset = 0
        self.length = length

    def seek(self, offset, whence=0):
        if whence == os.SEEK_SET:
            self.offset = offset
        elif whence == os.SEEK_CUR:
            self.offset += offset
        elif whence == os.SEEK_END:
            self.offset = self.length + offset
        else:
            # Other values of whence should raise an IOError
            raise IOError(offset, whence)
        return self.offset
        # return self.f.seek(self.offset + self.f_offset, os.SEEK_SET)

    def tell(self):
        return self.offset

    def read(self, size=-1):
        if size < 0:
            size = self.length - self.offset
        size = max(0, min(size, self.length - self.offset))
        with io.open(self.f, 'rb', buffering=0) as file:
            with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                mm.seek(self.offset + self.f_offset)
                b1 = mm.read(size)
                mm.close()
                file.close()
                self.offset += size
                return b1


############################################
### Functions


def bytes_to_int(b, signed=False):
    """
    Remember for a single byte, I only need to do b[0] to get the int. And it's really fast as compared to the function here. This is only needed for bytes > 1.
    """
    return int.from_bytes(b, 'little', signed=signed)


def int_to_bytes(i, byte_len, signed=False):
    """

    """
    return i.to_bytes(byte_len, 'little', signed=signed)


def hash_key(key):
    """

    """
    return blake2s(key, digest_size=key_hash_len).digest().hex()


def determine_obj_size(file_obj):
    """

    """
    pos = file_obj.tell()
    size = file_obj.seek(0, io.SEEK_END)
    file_obj.seek(pos)

    return size


def get_data_block(file_path, key, value, n_bytes_key, n_bytes_value):
    """
    Function to get either the key or the value or both from a data block.
    """
    with io.open(file_path, 'rb', buffering=0) as file:
        with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            if key and value:
                key_len_value_len = mm.read(n_bytes_key + n_bytes_value)
                key_len = bytes_to_int(key_len_value_len[:n_bytes_key])
                value_len = bytes_to_int(key_len_value_len[n_bytes_key:])
                key_value = mm.read(key_len + value_len)
                key = key_value[:key_len]
                value = FileObjectReadSlice(file_path, n_bytes_key + n_bytes_value + key_len, value_len)

                return key.decode(), value

            elif key:
                key_len = bytes_to_int(mm.read(n_bytes_key))
                mm.seek(n_bytes_value, 1)
                key = mm.read(key_len)

                return key.decode()

            elif value:
                key_len_value_len = mm.read(n_bytes_key + n_bytes_value)
                key_len = bytes_to_int(key_len_value_len[:n_bytes_key])
                value_len = bytes_to_int(key_len_value_len[n_bytes_key:])
                value = FileObjectReadSlice(file_path, n_bytes_key + n_bytes_value + key_len, value_len)

                return value
            else:
                raise ValueError('One or both key and value must be True.')


def get_value(db_path, key, n_bytes_key, n_bytes_value, ttl=None):
    """

    """
    key_bytes_len = len(key)
    key_hash = hash_key(key)
    file_path = db_path.joinpath(key_hash)
    if file_path.exists():
        if ttl is not None:
            if (time() - os.path.getmtime(file_path)) > ttl:
                file_path.unlink()
                return None

        file_len = file_path.stat().st_size

        value_pos = n_bytes_key + n_bytes_value + key_bytes_len

        out = FileObjectReadSlice(file_path, value_pos, file_len - value_pos)

        return out
    else:
        return None


def iter_keys_values(db_path, key=False, value=False, n_bytes_key=2, n_bytes_value=4, ttl=None):
    """

    """
    for file_path in db_path.iterdir():
        if ttl is not None:
            if (time() - os.path.getmtime(file_path)) > ttl:
                file_path.unlink()
                continue

        yield get_data_block(file_path, key, value, n_bytes_key, n_bytes_value)


def write_data_block(db_path, key, value, n_bytes_key, n_bytes_value, buffer_size):
    """

    """
    key_bytes_len = len(key)
    key_hash = hash_key(key)

    if isinstance(value, bytes):
        value = io.BytesIO(value)

    value_bytes_len = determine_obj_size(value)

    write_init_bytes = int_to_bytes(key_bytes_len, n_bytes_key) + int_to_bytes(value_bytes_len, n_bytes_value) + key

    file_path = db_path.joinpath(key_hash)

    with io.open(file_path, 'w+b') as file:
        _ = file.write(write_init_bytes)

        if hasattr(value, '_buffer_size'):
            buffer_size = value._buffer_size

        chunk = value.read(buffer_size)
        while chunk:
            file.write(chunk)
            chunk = value.read(buffer_size)











































































