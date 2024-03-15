#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 16 20:38:14 2022

@author: andrefelix
"""

import argparse
import pandas as pd
from fuzzywuzzy import fuzz
import utils_lattes_cnpq as util


def get_args():
    """
    Parse command-line arguments for processing XML files related to CAPES and Lattes data.

    Returns:
        argparse.Namespace: An object containing parsed arguments.

    This function sets up an argument parser using the argparse module to handle command-line
    arguments. It defines two required arguments:
    - capes_file: Path to the CAPES file to be processed. It should point to an Excel file
      downloaded from the CAPES website. The file must contain variables ID_PESSOA and NM_DOCENTE.
    - lattes_file: Path to the Lattes file to be processed. It should point to a CSV file generated
      by the script parse_xml_lattes.

    Example:
        To use this function, you can call it and access the parsed arguments like so:

        ```python
        import argparse

        args = get_args()
        capes_file_path = args.capes_file
        lattes_file_path = args.lattes_file
        ```

    Note:
        This function requires the argparse module to be imported.
    """
    parser = argparse.ArgumentParser(description='Pasta com arquivos XML para '
                                     'processar.')
    parser.add_argument('capes_file', metavar='input_capes', type=str,
                        help='caminho do arquivo CAPES ser combinado. Deve apontar '
                        'para um arquivo Excel baixado do site da Capes. O '
                        'arquivo deve conter as variaves ID_PESSOA, NM_DOCENTE, '
                        'NM_IES_TITULACAO e AN_TITULACAO')

    parser.add_argument('lattes_file', metavar='input_lattes', type=str,
                        help='caminho do arquivo CAPES ser combinado. Deve apontar '
                        'para um arquivo csv (desambiguate) gerado pelo script '
                        'parse_xml_lattes')

    return parser.parse_args()


def get_df_capes(str_capes_file_path):
    """
    Read and preprocess CAPES data from an Excel file.

    Args:
        str_capes_file_path (str): Path to the Excel file containing CAPES data.

    Returns:
        pandas.DataFrame: A DataFrame containing processed CAPES data.

    This function reads CAPES data from the specified Excel file, performs preprocessing,
    and returns a DataFrame containing the processed data. The preprocessing steps include:
    - Reading the Excel file using pandas.read_excel().
    - Converting the 'AN_TITULACAO' column to string type.
    - Selecting specific columns ('NM_DOCENTE', 'NM_IES_TITULACAO', 'AN_TITULACAO', 'ID_PESSOA').
    - Converting 'AN_TITULACAO' to numeric type and filling NaN values with 0.
    - Converting special characters in 'NM_DOCENTE' and 'NM_IES_TITULACAO' columns.
    - Replacing hyphens with spaces in 'NM_DOCENTE' column.
    - Extracting the first name from the 'NM_DOCENTE' column.
    - Dropping duplicate rows, keeping the last occurrence.
    - Adding an 'id_capes' column with unique IDs starting from 1.

    Example:
        To use this function, you can call it with the path to the CAPES Excel file and receive
        a DataFrame containing processed CAPES data:

        ```python
        import pandas as pd

        df_capes = get_df_capes('path/to/capes_file.xlsx')
        ```

    Note:
        This function requires the pandas library to be imported.
    """
    df_capes = pd.read_excel(str_capes_file_path,
                             converters={'AN_TITULACAO':str})

    df_capes = df_capes.astype({'AN_TITULACAO': str})

    df_capes = df_capes.loc[:, ['NM_DOCENTE', 'NM_IES_TITULACAO',
                                'AN_TITULACAO', 'ID_PESSOA']]
    df_capes['AN_TITULACAO'] = pd.to_numeric(df_capes['AN_TITULACAO'], errors='coerce')
    df_capes['AN_TITULACAO'] = df_capes['AN_TITULACAO'].fillna(0)
    df_capes = df_capes.astype({'AN_TITULACAO': int})

    util.convert_special_chars(df_capes, 'NM_DOCENTE')
    util.convert_special_chars(df_capes, 'NM_IES_TITULACAO')

    df_capes['NM_DOCENTE'] = df_capes['NM_DOCENTE'].str.replace('-', ' ')

    df_capes['prim_nome'] = df_capes['NM_DOCENTE'].str.split(' ', expand=True)[0]

    df_capes = df_capes.drop_duplicates(keep='last')

    df_capes['id_capes'] = range(1, len(df_capes) + 1)

    return df_capes


def get_df_lattes(str_lattes_file_path):
    """
    Read and preprocess Lattes data from a CSV file.

    Args:
        str_lattes_file_path (str): Path to the CSV file containing Lattes data.

    Returns:
        pandas.DataFrame: A DataFrame containing processed Lattes data.

    This function reads Lattes data from the specified CSV file, performs preprocessing,
    and returns a DataFrame containing the processed data. The preprocessing steps include:
    - Reading the CSV file using pandas.read_csv().
    - Converting specific columns ('FILE-NAME', 'NOME-COMPLETO', 'ANO-DE-OBTENCAO-DO-TITULO',
      'NOME-INSTITUICAO') to string type.
    - Converting 'ANO-DE-OBTENCAO-DO-TITULO' to numeric type and filling NaN values with 0.
    - Filtering out rows where 'NOME-COMPLETO' is NaN.
    - Converting special characters in 'NOME-COMPLETO' and 'NOME-INSTITUICAO' columns.
    - Replacing hyphens with spaces in 'NOME-COMPLETO' column.
    - Extracting the first name from the 'NOME-COMPLETO' column.

    Example:
        To use this function, you can call it with the path to the Lattes CSV file and receive
        a DataFrame containing processed Lattes data:

        ```python
        import pandas as pd

        df_lattes = get_df_lattes('path/to/lattes_file.csv')
        ```

    Note:
        This function requires the pandas library to be imported.
    """
    df_lattes = pd.read_csv(str_lattes_file_path,
                            converters={'FILE-NAME':str, 'NOME-COMPLETO':str,
                                        'ANO-DE-OBTENCAO-DO-TITULO':str,
                                        'NOME-INSTITUICAO':str})

    df_lattes['AnoTitulacao'] = pd.to_numeric(df_lattes['ANO-DE-OBTENCAO-DO-TITULO'],
                                              errors='coerce')
    df_lattes['AnoTitulacao'] = df_lattes['AnoTitulacao'].fillna(0)
    df_lattes = df_lattes.astype({'AnoTitulacao': int})

    df_lattes = df_lattes[df_lattes['NOME-COMPLETO'].notna()]

    util.convert_special_chars(df_lattes, 'NOME-COMPLETO')
    util.convert_special_chars(df_lattes, 'NOME-INSTITUICAO')

    df_lattes['NOME-COMPLETO'] = df_lattes['NOME-COMPLETO'].str.replace('-', ' ')

    df_lattes['prim_nome'] = df_lattes['NOME-COMPLETO'].str.split(' ', expand=True)[0]

    return df_lattes


def get_lst_key_merge():
    """
    Get a list of key merge configurations for merging DataFrames.

    Returns:
        list: A list of key merge configurations.

    This function returns a list of key merge configurations, where each configuration is a list
    containing two sublists. The first sublist represents the key columns from the first DataFrame
    to be merged, and the second sublist represents the corresponding key columns from the second
    DataFrame to be merged. These configurations are used as input to merge DataFrames.

    Example:
        To use this function, you can call it to obtain a list of key merge configurations:

        ```python
        key_merge_configurations = get_lst_key_merge()
        ```

        Each element in the list will be a key merge configuration, such as:
        [['NM_DOCENTE', 'NM_IES_TITULACAO', 'AN_TITULACAO'],
         ['NOME-COMPLETO', 'NOME-INSTITUICAO', 'AnoTitulacao']]
    """
    return [[['NM_DOCENTE', 'NM_IES_TITULACAO', 'AN_TITULACAO'],
             ['NOME-COMPLETO', 'NOME-INSTITUICAO', 'AnoTitulacao']],
            [['NM_DOCENTE', 'NM_IES_TITULACAO'],
             ['NOME-COMPLETO', 'NOME-INSTITUICAO']],
            [['NM_DOCENTE', 'AN_TITULACAO'],
             ['NOME-COMPLETO', 'AnoTitulacao']],
            [['NM_DOCENTE'],
             ['NOME-COMPLETO']],
            [['prim_nome', 'NM_IES_TITULACAO', 'AN_TITULACAO'],
             ['prim_nome', 'NOME-INSTITUICAO', 'AnoTitulacao']],
            [['prim_nome', 'NM_IES_TITULACAO'],
             ['prim_nome', 'NOME-INSTITUICAO']],
            [['prim_nome', 'AN_TITULACAO'],
             ['prim_nome', 'AnoTitulacao']],
            [['prim_nome'],
             ['prim_nome']]
           ]


def merge_capes_lattes(str_capes_file_name, str_lattes_file_name):
    """
    Merge CAPES and Lattes DataFrames based on various key configurations.

    Args:
        str_capes_file_name (str): Path to the CAPES Excel file.
        str_lattes_file_name (str): Path to the Lattes CSV file.

    Returns:
        list: A list containing the merged DataFrame and the updated CAPES DataFrame.

    This function merges CAPES and Lattes DataFrames using different key configurations. It iterates
    through each key merge configuration obtained from get_lst_key_merge(), merges the DataFrames
    accordingly, calculates matching scores, and filters matches based on specified thresholds.
    It then removes matched rows from both CAPES and Lattes DataFrames and returns the merged
    DataFrame along with the updated CAPES DataFrame.

    Example:
        To use this function, you can call it with paths to the CAPES and Lattes files:

        ```python
        merged_data, updated_capes = merge_capes_lattes('capes_data.xlsx', 'lattes_data.csv')
        ```

    Note:
        This function relies on the get_lst_key_merge() function to obtain key merge configurations.
        It also uses the remove_rows_by_key() function to remove matched rows from DataFrames.
        Additionally, it requires the pandas library and the fuzzywuzzy module to be imported.
    """
    lst_key_merge = get_lst_key_merge()
    df_capes = get_df_capes(str_capes_file_name)
    df_lattes = get_df_lattes(str_lattes_file_name)

    df_match = None
    str_progress = 'i:{}, count:{}, low_match:{}, key: {}'
    i = 0

    for key_merge in lst_key_merge:
        df_merge = df_capes.merge(df_lattes,
                                  left_on=key_merge[0],
                                  right_on=key_merge[1])

        if 'NM_DOCENTE' in key_merge[0]:
            df_merge['match_nome'] = 100
        else:
            df_merge['match_nome'] = [fuzz.token_sort_ratio(x, y) for (x, y) in
                                      zip(df_merge['NM_DOCENTE'],
                                          df_merge['NOME-COMPLETO'])]
            df_merge = df_merge[df_merge.match_nome >= 75]

        if 'NM_IES_TITULACAO' in key_merge[0]:
            df_merge['match_instit'] = 100
        else:
            df_merge['match_instit'] = [fuzz.token_sort_ratio(x, y) for (x, y) in
                                        zip(df_merge['NM_IES_TITULACAO'],
                                            df_merge['NOME-INSTITUICAO'])]

        df_merge['match_ano'] = [abs(int(x)-int(y)) for (x, y) in
                                 zip(df_merge['AN_TITULACAO'], df_merge['AnoTitulacao'])]

        df_merge['match'] = [((x / 100) * (y / 100)) * 100 for (x, y) in
                             zip(df_merge['match_nome'],
                                 df_merge['match_instit'])]

        if i < 1:
            df_merge = df_merge.drop_duplicates(['id_capes'], keep='last')
        else:
            df_merge = df_merge.sort_values(by=['id_capes', 'match_nome',
                                                'match_ano', 'match_instit'],
                                            ascending=[True, True, False, True])
            df_merge = df_merge.drop_duplicates(['id_capes'], keep='last')


        df_merge['key_match'] = ' - '.join(key_merge[0])
        df_merge['index_match'] = i

        df_match = pd.concat([df_match, df_merge],
                             ignore_index=True, sort=False)

        df_capes = remove_rows_by_key(df_match, df_capes, 'id_capes')
        df_lattes = remove_rows_by_key(df_match, df_lattes, 'FILE-NAME')

        print(str_progress.format(str(i), len(df_merge),
                                  len(df_merge[df_merge.match < 50]),
                                  '+'.join(key_merge[0])))
        i += 1

    df_match['duplicado'] = df_match.duplicated(['id_capes'], keep=False)

    return [df_match, df_capes]


def remove_rows_by_key(df_base, df_to_clean, str_key):
    """
    Remove rows from a DataFrame based on keys present in another DataFrame.

    Args:
        df_base (pandas.DataFrame): DataFrame containing keys to be removed.
        df_to_clean (pandas.DataFrame): DataFrame from which rows will be removed.
        str_key (str): Name of the key column.

    Returns:
        pandas.DataFrame: DataFrame with rows removed based on keys.

    This function removes rows from the df_to_clean DataFrame where the keys in the str_key column
    match keys present in the df_base DataFrame.

    Example:
        To use this function, you can call it with the DataFrame to clean and the base DataFrame
        containing keys to be removed:

        ```python
        cleaned_df = remove_rows_by_key(base_df, df_to_clean, 'key_column')
        ```

    Note:
        This function requires the pandas library to be imported.
    """
    df_unique = df_base.loc[:, [str_key]]
    df_unique = df_unique.apply(lambda col: col.drop_duplicates().
                                reset_index(drop=True))

    return df_to_clean[~df_to_clean[str_key].isin(df_unique[str_key])]

def main():
    """
    Perform the main processing tasks.

    This function serves as the entry point for the main processing tasks. It parses command-line
    arguments, performs CAPES and Lattes data merging, and writes the merged and cleaned data to
    CSV files.

    Example:
        To run the main function, you can call it directly:

        ```python
        main()
        ```

    Note:
        This function relies on the get_args() function to parse command-line arguments,
        format_path() function to format file paths, and merge_capes_lattes() function to merge
        CAPES and Lattes data.
    """
    args = get_args()
    str_capes_file_name = args.capes_file
    str_lattes_file_name = args.lattes_file

    str_capes_file_name = '../data/capes-2020.xlsx'
    str_lattes_file_name = '../data/id_lattes_to_disambiguate.csv'

    df_match, df_capes = merge_capes_lattes(str_capes_file_name, str_lattes_file_name)

    df_match.to_csv(f"../data/match_capes_x_lattes.csv",
                    index=False)

    df_capes.to_csv(f"../data/capes_not_found_in_lattes.csv",
                    index=False)

if __name__ == "__main__":
    main()
