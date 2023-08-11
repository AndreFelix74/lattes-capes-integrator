#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 19 04:11:33 2022

@author: andrefelix
"""

def convert_special_chars(df, col):
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
