#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 19 04:11:33 2022

@author: andrefelix
"""

import os


def convert_special_chars(df, col):
    """
    Convert special characters in a DataFrame column to their Latin counterparts.

    Args:
        df (pandas.DataFrame): DataFrame containing the column to be processed.
        col (str): Name of the column containing the text to be processed.

    This function converts special characters in the specified column of the DataFrame to their
    Latin counterparts. It iterates through a list of regular expressions for Latin characters
    and their corresponding replacements, and applies these replacements to the column.

    Example:
        To use this function, you can call it with a DataFrame and the name of the column
        containing text to be processed:

        ```python
        convert_special_chars(data_frame, 'column_name')
        ```

    Note:
        This function modifies the DataFrame in place.
        It requires the pandas library to be imported.
    """
    lst_regex_latin_chars = [[r'[aàáâäãåæ]', 'a'],
                         [r'[eèéêë]', 'e'],
                         [r'[iìíîï]', 'i'],
                         [r'[oòóôöõø]', 'o'],
                         [r'[uùúûü]', 'u'],
                         [r'[ñ]', 'n'],
                         [r'[ç]', 'c']
                         ]

    for regex_latin in lst_regex_latin_chars:
        df[col] = df[col].str.lower().replace(regex_latin[0],
                                              regex_latin[1],
                                              regex=True)


def format_path(str_path):
    """
    Formats the path by adding "../data/" as prefix
    and a trailing slash "/", if needed.
    
    Args:
    str_path (str): The path to be formatted.
    
    Returns:
    str: The formatted path.
    """
    if not str_path.startswith("/") or not str_path.startswith("."):
        str_path = os.path.join("..", "data", str_path)
    
    if not str_path.endswith("/"):
        str_path += "/"
    
    return str_path
