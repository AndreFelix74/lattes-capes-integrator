#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Aug 21 06:26:54 2022

@author: andrefelix
"""

import argparse
import glob
import json
import os
import time
import zipfile
from threading import Thread
import requests
from dbc_api_python3 import deathbycaptcha
import config_dbc_credentials as cfg
import utils_lattes_cnpq as util


def download_xml(str_id_lattes, str_captcha_valido, str_download_folder_path):
    """
    Download XML file for a given ID.

    Args:
        str_id_lattes (str): The CNPq ID for which the XML file needs to be downloaded.
        str_captcha_valido (str): The solved CAPTCHA string.
        str_download_folder_path (str): The path of the download folder.

    This function constructs HTTP requests to download the XML file associated with the
    provided CNPq ID. It uses a session to maintain the connection and sets appropriate
    headers for the requests. Upon successful download, the file is saved in the specified
    download folder. If the downloaded file is not a valid zip file, it is removed.

    """

    dict_headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.72 Safari/537.36',
                    'Host': 'buscatextual.cnpq.br',
                    'Origin': 'http://buscatextual.cnpq.br',
                    'Referer': f"""http://buscatextual.cnpq.br/buscatextual/download.do?metodo=apresentar&idcnpq={str_id_lattes}"""
                    }

    str_url_apresentacao = f"""http://buscatextual.cnpq.br/buscatextual/download.do?metodo=apresentar&idcnpq={str_captcha_valido}"""

    payload = {'metodo': 'executarDownload',
               'tokenCaptchar': str_captcha_valido,
               'idcnpq': str_id_lattes,
               'g-recaptcha-response': str_captcha_valido
              }

    session = requests.Session()

    response = session.get(str_url_apresentacao, headers=dict_headers)

    response = session.post('http://buscatextual.cnpq.br/buscatextual/download.do',
                            data=payload,
                            headers=dict_headers)

    str_file_name = '{}/{}.zip'.format(str_download_folder_path, str_id_lattes)
    with open(str_file_name, mode='wb') as file:
        file.write(response.content)

    if not zipfile.is_zipfile(str_file_name):
        print('Erro no arquivo baixado: {}'.format(str_file_name))
        os.remove(str_file_name)


def download_xml_lst_ids(str_thread_index, str_download_folder_path, lst_ids, dbc_client):
    """
    Download XML files for a list of IDs.

    Args:
        str_thread_index (str): Identifier for the current thread.
        str_download_folder_path (str): The path of the download folder.
        lst_ids (list): A list of IDs for which XML files need to be downloaded.
        dbc_client: An instance of the DeathByCaptcha client.

    This function iterates over the list of IDs, attempts to solve the CAPTCHA for each ID,
    and if successful, initiates a thread to download the XML file associated with the ID.
    It handles exceptions and retries in case of errors.

    Note:
        This function uses multithreading to download XML files concurrently.

    """
    n_error_count = 0

    while lst_ids:
        try:
            str_id_lattes = lst_ids.pop(0)

            str_captcha_valido = solve_captcha(str_id_lattes, dbc_client)
            if str_captcha_valido:
                trd = Thread(target=download_xml, args=(str_id_lattes,
                                                        str_captcha_valido,
                                                        str_download_folder_path))
                trd.start()
                time.sleep(1)
        except KeyboardInterrupt:
            return
        except Exception as excpt:
            n_error_count += 1
            print('')
            print('Error count thread {}: {}'.format(str_thread_index, str(n_error_count)))
            print(excpt)
            time.sleep(30)


def get_args():
    """
    Parse command-line arguments.

    Returns:
        argparse.Namespace: An object containing the parsed arguments.

    This function parses the command-line arguments using the argparse module.
    It expects two positional arguments:
        - input_file (str): The name of the file to be processed. It should
        contain a list of 16-digit IDs.
        - output_path (str): The path where the zip files will be saved.

    Returns the parsed arguments as a namespace object.
    """

    parser = argparse.ArgumentParser(description='Arquivo de IDs para processar.')
    parser.add_argument('input_file', metavar='lista_ids', type=str,
                        help='nome do arquivo a ser processado. Deve conter uma'
                        'lista de IDs de 16 digitos')
    parser.add_argument('output_path', metavar='output_path', type=str,
                        help='caminho onde serao gravados os arquivos zip')
    return parser.parse_args()


def get_lst_downloaded_files(str_download_folder_path):
    """
    Get a list of downloaded files.

    Args:
        str_download_folder_path (str): The path of the download folder.

    Returns:
        list: A list of file paths of downloaded zip files in the specified folder.

    This function utilizes the glob module to retrieve a list of file paths
    of zip files in the specified download folder.

    Example:
        If str_download_folder_path is '/path/to/downloads', and the folder contains
        files like 'file1.zip', 'file2.zip', and 'file3.txt', the function will return
        ['/path/to/downloads/file1.zip', '/path/to/downloads/file2.zip'].
    """

    return glob.glob('{}/*.zip'.format(str_download_folder_path))


def get_lst_ids_to_download(str_list_ids_path, str_download_folder_path):
    """
    Get a list of IDs to download.

    Args:
        str_list_ids_path (str): The path of the file containing the list of IDs.
        str_download_folder_path (str): The path of the download folder.

    Returns:
        list: A list of IDs that are not yet downloaded.

    This function reads the list of IDs from the specified file, compares them with
    the list of already downloaded files in the download folder, and returns the IDs
    that are yet to be downloaded.

    Example:
        If str_list_ids_path points to a file containing IDs ['ID1', 'ID2', 'ID3'],
        and str_download_folder_path contains downloaded files 'ID1.zip' and 'ID3.zip',
        the function will return ['ID2'].
    """
    lst_ids = open(str_list_ids_path).read().splitlines()

    lst_downloaded_files = get_lst_downloaded_files(str_download_folder_path)
    lst_downloaded_files = [os.path.basename(x).split('.')[0] for x in lst_downloaded_files]

    lst_return = list(set(lst_ids) - set(lst_downloaded_files))

    lst_return.sort()

    return lst_return


def solve_captcha(str_idcnpq, dbc_client):
    """
    Solve CAPTCHA for a given CNPq ID.

    Args:
        str_idcnpq (str): The CNPq ID for which the CAPTCHA needs to be solved.
        dbc_client: An instance of the DeathByCaptcha client.

    Returns:
        str or None: The solved CAPTCHA text if successful, None otherwise.

    This function attempts to solve the CAPTCHA required for downloading a file
    associated with the provided CNPq ID. It constructs the CAPTCHA solving request
    using the DeathByCaptcha client. If the CAPTCHA is successfully solved, it returns
    the solved text; otherwise, it returns None.

    If an AccessDeniedException is encountered during the process, it indicates that
    there is an issue with access to the DeathByCaptcha API, and an appropriate error
    message is printed along with the current balance.
    """
    try:
        str_pageurl = f"""http://buscatextual.cnpq.br/buscatextual/download.do?metodo=apresentar&idcnpq={str_idcnpq}"""

        captcha_dict = {
            'proxytype': 'HTTP',
            'googlekey': '6LeDv6QUAAAAAP_ZK5AXrsNRQjxfbjgZLV5_YHyy',
            'pageurl': str_pageurl
            }

        json_captcha = json.dumps(captcha_dict)

        captcha = dbc_client.decode(type=4, token_params=json_captcha)
        if captcha:
            if captcha['is_correct']:
                return captcha['text']

            dbc_client.report(captcha["captcha"])
            print('CAPTCHA was incorrectly solved\n')

        return None
    except deathbycaptcha.AccessDeniedException:
        print("error: Access to DBC API denied," +
              "check your credentials and/or balance\n")
        balance = dbc_client.get_balance()
        print(balance)


def main():
    """
    Main function to orchestrate the download process.

    This function serves as the entry point for the download process.
    It retrieves command-line arguments, initializes necessary resources,
    creates download threads, and starts the download process.

    """
    args = get_args()
    str_list_ids_path = args.input_file
    str_download_folder_path = util.format_path(args.output_path)

    if not os.path.exists(str_download_folder_path):
        os.makedirs(str_download_folder_path)

    n_threads_count = 2

    dbc_client = deathbycaptcha.SocketClient(cfg.username, cfg.password, cfg.authtoken)

    lst_ids = get_lst_ids_to_download(str_list_ids_path, str_download_folder_path)

    if lst_ids:
        for i in range(0, n_threads_count):
            trd = Thread(target=download_xml_lst_ids, args=(i,
                                                            str_download_folder_path,
                                                            lst_ids,
                                                            dbc_client))
            trd.start()
            time.sleep(1)

if __name__ == "__main__":
    main()
