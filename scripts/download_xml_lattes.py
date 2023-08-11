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

def download_xml(str_id_lattes, str_captcha_valido, str_download_folder_path):
    """Baixa o arquivo XML do curriculo Lattes referente ao Id informado."""
    dict_headers = {'User-Agent': """Mozilla/5.0 (X11; Linux x86_64)
                                     AppleWebKit/537.36 (KHTML, like Gecko)
                                     Chrome/90.0.4430.72 Safari/537.36""",
                    'Host': 'buscatextual.cnpq.br',
                    'Origin': 'http://buscatextual.cnpq.br',
                    'Referer': f"""http://buscatextual.cnpq.br/buscatextual/
                    download.do?metodo=apresentar&idcnpq={str_id_lattes}"""
                    }
    str_url_apresentacao = f"""http://buscatextual.cnpq.br/buscatextual/download.do
    ?metodo=apresentar&idcnpq={str_captcha_valido}"""

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

def download_xml_lst_ids(str_thread_index, str_download_folder_path, lst_ids,
                         dbc_client):
    """Processa a lista de Ids, baixando os arquivos XMLs de cada curriculo Lattes."""
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
    """Retorna os argumentos passados na linha de comando."""
    parser = argparse.ArgumentParser(description='Arquivo de IDs para processar.')
    parser.add_argument('input_file', metavar='lista_ids', type=str,
                        help='nome do arquivo a ser processado. Deve conter uma'
                        'lista de IDs de 16 digitos')
    parser.add_argument('output_path', metavar='output_path', type=str,
                        help='caminho onde serao gravados os arquivos zip')
    return parser.parse_args()

def get_lst_downloaded_files(str_download_folder_path):
    """Retorna uma lista com os nomes dos arquivos zip existentes na pasta de download."""
    return glob.glob('{}/*.zip'.format(str_download_folder_path))

def get_lst_ids_to_download(str_list_ids_path, str_download_folder_path):
    """Compara a lista de Ids a serem baixados com os arquivos baixados e
    Retorna uma lista com Ids que ainda não foram baixados."""
    lst_ids = open(str_list_ids_path).read().splitlines()

    lst_downloaded_files = get_lst_downloaded_files(str_download_folder_path)
    lst_downloaded_files = [os.path.basename(x).split('.')[0] for x in lst_downloaded_files]

    lst_return = list(set(lst_ids) - set(lst_downloaded_files))

    lst_return.sort()

    return lst_return

def solve_captcha(str_idcnpq, dbc_client):
    """Chama a API do servico Dead by Captcha para resolver o captcha.
    Retorna o valor do captcha solucionado ou None em caso de falha."""
    try:
        str_pageurl = """http://buscatextual.cnpq.br/buscatextual/download.do?
        metodo=apresentar&idcnpq={str_idcnpq}"""

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
    """Funcao principal que executa o script."""
    args = get_args()
    str_list_ids_path = args.input_file
    str_download_folder_path = args.output_path

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
