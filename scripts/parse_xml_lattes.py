#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Aug 21 19:27:07 2022

@author: andrefelix
"""

import argparse
import xml.etree.ElementTree as ET
import glob
import time
import multiprocessing
import zipfile
import os
import pandas as pd
import utils_lattes_cnpq as util

COUNT_PARSE = multiprocessing.Value('i', 0)

def convert_lst_prod_to_dataframe(lst_prod):
    """
    Convert a list of dictionaries representing production information to a pandas DataFrame.

    Args:
        lst_prod (list): A list of dictionaries containing production information.

    Returns:
        pandas.DataFrame: A DataFrame containing the production information.

    This function converts a list of dictionaries representing production
    information into a pandas DataFrame.
    It performs various data manipulation operations such as combining columns,
    deleting unnecessary columns,
    filling missing values, and formatting column values. The resulting DataFrame is returned.

    Example:
        If lst_prod is a list of dictionaries representing production information,
        the function will return
        a pandas DataFrame containing the production information in tabular format.
    """
    df_producao = pd.DataFrame(lst_prod)

    columns_to_remove = [
        'NOME-PARA-CITACAO', 'ORDEM-DE-AUTORIA',
        'NRO-ID-CNPQ',
        'PALAVRA-CHAVE-1', 'PALAVRA-CHAVE-2', 'PALAVRA-CHAVE-3',
        'PALAVRA-CHAVE-4', 'PALAVRA-CHAVE-5', 'PALAVRA-CHAVE-6',
        'SETOR-DE-ATIVIDADE-1', 'SETOR-DE-ATIVIDADE-2', 'SETOR-DE-ATIVIDADE-3',
        'NOME-COMPLETO-DO-AUTOR'
    ]
    df_producao.drop(columns=columns_to_remove, inplace=True)

    df_producao.fillna('', inplace=True)

    df_producao['TITULO'] = (df_producao['TITULO'] +
                             df_producao['TITULO-DO-ARTIGO'] +
                             df_producao['TITULO-DO-LIVRO'] +
                             df_producao['TITULO-DO-CAPITULO-DO-LIVRO'] +
                             df_producao['TITULO-DO-TEXTO']
                            )

    df_producao['TITULO-INGLES'] = (df_producao['TITULO-INGLES'] +
                                    df_producao['TITULO-DO-ARTIGO-INGLES'] +
                                    df_producao['TITULO-DO-LIVRO-INGLES'] +
                                    df_producao['TITULO-DO-CAPITULO-DO-LIVRO-INGLES'] +
                                    df_producao['TITULO-DO-TEXTO-INGLES']
                                    )

    df_producao['NUM-CLASSIFICACAO'] = df_producao['ISSN'] + df_producao['ISBN']

    df_producao['ANO'] = (df_producao['ANO'] +
                          df_producao['ANO-DO-TEXTO'] +
                          df_producao['ANO-DO-ARTIGO']
                          )

    df_producao['PAIS'] = (df_producao['PAIS'] + df_producao['PAIS-DE-PUBLICACAO'])

    df_producao['TITULO-DO-PERIODICO-OU-JORNAL-OU-REVISTA'] = (
        df_producao['TITULO-DO-JORNAL-OU-REVISTA'] +
        df_producao['TITULO-DO-PERIODICO-OU-REVISTA']
        )

    redundant_columns = ['TITULO-DO-ARTIGO', 'TITULO-DO-LIVRO',
                         'TITULO-DO-CAPITULO-DO-LIVRO', 'TITULO-DO-TEXTO',
                         'TITULO-DO-ARTIGO-INGLES', 'TITULO-DO-LIVRO-INGLES',
                         'TITULO-DO-CAPITULO-DO-LIVRO-INGLES', 'TITULO-DO-TEXTO-INGLES',
                         'ISSN', 'ISBN',
                         'ANO-DO-TEXTO', 'ANO-DO-ARTIGO',
                         'PAIS-DE-PUBLICACAO',
                         'TITULO-DO-JORNAL-OU-REVISTA', 'TITULO-DO-PERIODICO-OU-REVISTA']

    df_producao.drop(columns=redundant_columns, inplace=True)

    return df_producao


def dictify_flat(str_id, node):
    """
    Convert XML node and its children attributes to a dictionary.

    Args:
        str_id (str): The identifier associated with the XML node.
        node (xml.etree.ElementTree.Element): The XML node to be converted.

    Returns:
        dict: A dictionary containing the attributes of the XML node and its children.

    This function takes an XML node and its associated identifier as input and converts
    the attributes of the node and its children into a dictionary. It adds the identifier
    as a key-value pair with the key 'Identificador' in the dictionary.

    Example:
        If str_id is '123' and node has children with attributes {'name': 'John', 'age': '30'},
        the function will return {'Identificador': '123', 'name': 'John', 'age': '30'}.
    """
    dict_return = dict()
    dict_return['Identificador'] = str_id
    for node_child in node.findall("./*"):
        for att in node_child.attrib:
            dict_return[att] = node_child.attrib[att]

    return dict_return


def dictify_xml_node_att(node):
    """
    Convert XML node attributes to a dictionary.

    Args:
        node (xml.etree.ElementTree.Element): The XML node to be converted.

    Returns:
        dict: A dictionary containing the attributes of the XML node.

    This function takes an XML node as input and converts its attributes into a dictionary.
    If the input node is not None, it iterates over its attributes and adds them to the dictionary.

    Example:
        If node is an XML node with attributes {'id': '123', 'name': 'John'},
        the function will return
        {'id': '123', 'name': 'John'}.
    """
    dict_return = dict()
    if node is not None:
        for att in node.attrib:
            dict_return[att] = node.attrib[att]

    return dict_return


def extract_zip(str_folder_ori, str_folder_dest):
    """
    Extract XML files from ZIP archives in the source folder to the destination folder.

    This function searches for ZIP files in the source folder, extracts the 'curriculo.xml' file from each ZIP archive,
    and saves it in the destination folder. It ensures that only files not already extracted are processed.

    Args:
        str_folder_ori (str): The path to the folder containing ZIP files.
        str_folder_dest (str): The path to the folder where XML files will be extracted.

    Returns:
        None

    Example:
        extract_zip('data/zips/', 'data/xmls/')
    """
    lst_zip_files = sorted(glob.glob(f"{str_folder_ori}*.zip"))
    lst_zip_files = [os.path.basename(x).split('.')[0] for x in lst_zip_files]

    lst_unziped_files = sorted(glob.glob(f"{str_folder_dest}*.xml"))
    lst_unziped_files = [os.path.basename(x).split('.')[0] for x in lst_unziped_files]
    
    lst_zip_files = list(set(lst_zip_files) - set(lst_unziped_files))

    for file_name_zip in lst_zip_files:
        str_file_name = f"{str_folder_dest}{file_name_zip}.xml"
        file_name_zip = f"{str_folder_ori}{file_name_zip}.zip"

        if zipfile.is_zipfile(file_name_zip):
            lattes_zip = zipfile.ZipFile(file_name_zip)
            lattes_zip.extract('curriculo.xml', str_folder_dest)
            lattes_zip.close()
            os.rename(f"{str_folder_dest}curriculo.xml", str_file_name)
        else:
            print('Erro no arquivo: {}'.format(file_name_zip))
            os.remove(file_name_zip)


def get_args():
    """
    Parse command-line arguments using argparse.

    Returns:
        argparse.Namespace: An object containing the parsed arguments.

    The function sets up an ArgumentParser to handle command-line arguments.
    It expects two positional arguments:

    - input_folder (str): The path to the folder containing zip files to be processed.
      These files should be downloaded from the CNPq website.

    - output_folder (str): The path where CSV files will be saved.

    Returns the parsed arguments as an argparse.Namespace object.

    Example:
        To use this function in a script, call it as follows:

        ```python
        import argparse

        # Define the function get_args here...

        if __name__ == "__main__":
            args = get_args()
            print(args.input_folder)
            print(args.output_folder)
        ```

        Then run the script from the command line, providing the required arguments:

        ```
        python script.py /path/to/input_folder /path/to/output_folder
        ```

    """
    parser = argparse.ArgumentParser(description='Pasta com arquivos XML para '
                                     'processar.')
    parser.add_argument('input_folder', metavar='content_folder', type=str,
                        help='caminho dos arquivos a serem processados. Deve '
                        'conter arquivos xml baixados do site do CNPq')

    parser.add_argument('output_folder', metavar='output_path', type=str,
                        help='caminho onde serao gravados os arquivos csv')

    return parser.parse_args()


def get_lst_node_path(node_name):
    """
    Get the XPath of a specified type of node.

    Args:
        node_name (str): The name of the node type.

    Returns:
        str: The XPath corresponding to the specified type of node.

    This function returns the XPath corresponding to a specified type of node.
    It uses a dictionary mapping node names to their respective XPaths.

    Example:
        If node_name is 'artigo', the function will return
        './/PRODUCAO-BIBLIOGRAFICA/ARTIGOS-PUBLICADOS/ARTIGO-PUBLICADO'.
    """
    dct_node = {'artigo': './/PRODUCAO-BIBLIOGRAFICA/ARTIGOS-PUBLICADOS/ARTIGO-PUBLICADO',
                'livro': './/LIVROS-E-CAPITULOS/LIVROS-PUBLICADOS-OU-ORGANIZADOS/',
                'lvr_cap': './/LIVROS-E-CAPITULOS/CAPITULOS-DE-LIVROS-PUBLICADOS/',
                'jor_rev': './/TEXTOS-EM-JORNAIS-OU-REVISTAS/',
                'evt_org': './/PRODUCAO-TECNICA/DEMAIS-TIPOS-DE-PRODUCAO-TECNICA/ORGANIZACAO-DE-EVENTO',
                'evt_part': './/PARTICIPACAO-EM-EVENTOS-CONGRESSOS/PARTICIPACAO-EM-CONGRESSO',
                'premio': './/PREMIOS-TITULOS/PREMIO-TITULO'}
    return dct_node[node_name]


def parse_files(str_file_name):
    """
    Parse XML files to extract various types of information.

    Args:
        str_file_name (str): The name of the XML file to be parsed.

    Returns:
        list: A list containing dictionaries representing different types of
        information extracted from the XML file.

    This function parses XML files to extract various types of information,
    including general attributes,
    education details, and different types of productions (articles, books,
    chapters, etc.). It returns
    a list containing dictionaries representing the extracted information.

    Example:
        If str_file_name is 'file.xml' and the XML file contains information
        about a researcher's CV,
        the function will return a list with dictionaries representing different
        aspects of the researcher's CV.
    """
    global COUNT_PARSE
    with COUNT_PARSE.get_lock():
        COUNT_PARSE.value += 1
        if COUNT_PARSE.value % 100 == 0:
            print(f"Parsing {COUNT_PARSE.value}th file")

    dict_aux = dict()
    str_id = str_file_name[-20:-4]
    dict_aux['FILE-NAME'] = str_id
    root = None

    try:
        root = ET.parse(str_file_name).getroot()
    except KeyboardInterrupt:
        return []
    except Exception as excpt:
        print(excpt)

    if not root:
        return [dict_aux]

    dict_aux.update(dictify_xml_node_att(root))
    dict_aux.update(dictify_xml_node_att(root.find('./DADOS-GERAIS')))
    dict_aux.update(dictify_xml_node_att(root.find('./DADOS-GERAIS/RESUMO-CV')))

    return [dict_aux,
            parse_files_get_formacao_doutorado(str_id, root),
            parse_files_get_lst_node(str_id, root, 'artigo'),
            parse_files_get_lst_node(str_id, root, 'livro'),
            parse_files_get_lst_node(str_id, root, 'lvr_cap'),
            parse_files_get_lst_node(str_id, root, 'jor_rev'),
            parse_files_get_lst_node(str_id, root, 'evt_org'),
            parse_files_get_lst_node(str_id, root, 'evt_part'),
            parse_files_get_lst_node(str_id, root, 'premio')]


def parse_files_get_area_atuacao(str_id, root):
    """
    Parse XML files to extract areas of expertise information.

    Args:
        str_id (str): The identifier associated with the XML data.
        root (xml.etree.ElementTree.Element): The root element of the XML data.

    Returns:
        list: A list of dictionaries containing areas of expertise information.

    This function parses XML data to extract information about areas of expertise.
    It searches for the relevant nodes containing area of expertise data and
    converts the attributes of those nodes into dictionaries. The dictionaries
    are then added to a list.

    Example:
        If str_id is '123' and the XML data contains information about areas of expertise,
        the function will return a list with dictionaries representing the areas of expertise data.
    """
    lst_return = []
    for node_area in root.findall('.//AREAS-DE-ATUACAO/AREA-DE-ATUACAO'):
        dict_aux = dict()
        dict_aux['Identificador'] = str_id
        dict_aux.update(dictify_xml_node_att(node_area))
        lst_return.append(dict_aux)

    return lst_return


def parse_files_get_formacao_doutorado(str_id, root):
    """
    Parse XML files to extract doctoral education information.

    Args:
        str_id (str): The identifier associated with the XML data.
        root (xml.etree.ElementTree.Element): The root element of the XML data.

    Returns:
        list: A list of dictionaries containing doctoral education information.

    This function parses XML data to extract information about doctoral education.
    It searches for the relevant nodes containing doctoral education data and
    converts the attributes of those nodes into dictionaries. The dictionaries
    are then added to a list.

    Example:
        If str_id is '123' and the XML data contains information about doctoral
        education, the function will return a list with dictionaries representing
        the doctoral education data.
    """
    lst_return = []
    node_formacao = root.find('./DADOS-GERAIS/FORMACAO-ACADEMICA-TITULACAO')

    if node_formacao:
        dict_aux = dict()
        dict_aux['Identificador'] = str_id
        dict_aux.update(dictify_xml_node_att(node_formacao.find('./DOUTORADO')))
        dict_aux.update(dictify_xml_node_att(node_formacao.find('./DOUTORADO/PALAVRAS-CHAVE')))
        lst_return.append(dict_aux)

    return lst_return


def parse_files_get_lst_node(str_id, root, node_name):
    """
    Parse XML files to extract information from a specified type of node.

    Args:
        str_id (str): The identifier associated with the XML data.
        root (xml.etree.ElementTree.Element): The root element of the XML data.
        node_name (str): The name of the node type to extract information from.

    Returns:
        list: A list of dictionaries containing information extracted from the
        specified type of node.

    This function parses XML data to extract information from nodes of a specified type.
    It searches for nodes with the specified name and converts the attributes of those
    nodes into dictionaries. The dictionaries are then added to a list, with an additional
    key-value pair indicating the type of node.

    Example:
        If str_id is '123', node_name is 'PRODUCAO-TECNICA', and the XML data contains information
        about technical production, the function will return a list with dictionaries representing
        the technical production data, with each dictionary containing an additional key 'tipo_prod'
        with the value 'PRODUCAO-TECNICA'.
    """

    str_node_path = get_lst_node_path(node_name)
    lst_return = []
    for node in root.findall(str_node_path):
        lst_return.append(dictify_flat(str_id, node))

    for dic in lst_return:
        dic['tipo_prod'] = node_name

    return lst_return


def main():
    """
    Perform data processing tasks on XML files containing researcher information.

    This function orchestrates the entire process of parsing XML files,
    extracting relevant information,
    and performing data processing tasks. It consists of the following steps:
    1. Unzips XML files located in the input folder.
    2. Parses each XML file using multiprocessing to speed up the process.
    3. Converts the parsed information into pandas DataFrames.
    4. Merges DataFrames to create a unified dataset.
    5. Selects relevant columns for further analysis.

    Returns:
        None

    Example:
        main()
    """
    args = get_args()
    str_path_zip_files = util.format_path(args.input_folder)
    str_path_xml_files = f"{os.path.dirname(str_path_zip_files)}_extracted/"

    if not os.path.exists(str_path_xml_files):
        os.makedirs(str_path_xml_files)

    extract_zip(str_path_zip_files, str_path_xml_files)

    lst_files = sorted(glob.glob(f"{str_path_xml_files}/*.xml"))

    pool = multiprocessing.Pool()
    time_start = time.time()
    lst_lattes = pool.map(parse_files, lst_files)
    print(str(time.time() - time_start))

    lst_dados_gerais = []
    lst_formacao = []
    lst_producao = []
    for lattes in lst_lattes:
        if len(lattes) > 1:
            lst_dados_gerais.append(lattes[0])
            lst_formacao.extend(lattes[1])
            lst_producao.extend(sum(lattes[2:], []))

    df_producao = convert_lst_prod_to_dataframe(lst_producao)
    df_producao.to_csv(f"../data/lattes_producao.csv",
                       index=False)

    df_dados_gerais = pd.DataFrame(lst_dados_gerais)
    df_dados_gerais.to_csv(f"../data/lattes_dados_gerais.csv",
                           index=False)

    df_formacao = pd.DataFrame(lst_formacao)
    df_formacao.to_csv(f"../data/lattes_formacao.csv",
                       index=False)

    df_disambiguate = df_dados_gerais.merge(df_formacao,
                                            how='left',
                                            left_on='FILE-NAME',
                                            right_on='Identificador')

    df_disambiguate = df_disambiguate.loc[:, ['FILE-NAME', 'NOME-COMPLETO',
                                              'ANO-DE-OBTENCAO-DO-TITULO',
                                              'NOME-INSTITUICAO']]

    df_disambiguate.to_csv(f"../data/id_lattes_to_disambiguate.csv",
                           index=False)

if __name__ == "__main__":
    main()
