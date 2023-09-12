#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 17 08:50:07 2022

@author: andrefelix
"""

import argparse
import datetime
import glob
from threading import Thread
import re
import time
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
import pandas as pd
import utils_capes_lattes as util

B_HEADLESS = True

def download_idcnpq_by_lst_id_names(str_thread_index, str_download_folder_path,
                                    lst_id_names):
    browser = None
    n_error_count = 0

    while lst_id_names:
        try:
            if browser:
                del browser

            browser = get_browser()
            browser.maximize_window()
            wait_browser = WebDriverWait(browser, 60)

            while lst_id_names:
                str_id, str_name = lst_id_names.pop(0)

                lst_idcnpq = get_lst_idcnpq_by_name(browser, wait_browser, str_name)

                if not lst_idcnpq:
                    continue

                with open('{}/{}.txt'.format(str_download_folder_path, str(str_id)),
                          'w') as file:
                    file.write('\n'.join(lst_idcnpq))
        except KeyboardInterrupt:
            return
        except Exception as excpt:
            n_error_count += 1
            print('')
            print('Error count thread {}: {}'.format(str_thread_index, str(n_error_count)))
            print(excpt)
            time.sleep(30)

    if browser:
        browser.close()
        del browser

def get_args():
    parser = argparse.ArgumentParser(description='Arquivo de IDs para processar')

    parser.add_argument('input_file', metavar='content_file', type=str,
                        help='nome do arquivo a ser processado. Deve apontar '
                        'para um arquivo Excel baixado do site da Capes. O '
                        'arquivo deve conter as variaves ID_PESSOA e NM_DOCENTE')

    parser.add_argument('output_folder', metavar='output_path', type=str,
                        help='caminho onde serao gravados os arquivos txt')

    return parser.parse_args()

def get_browser():
    str_chromedriver_path = './chromedriver'

    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('disable-blink-features=AutomationControlled')
    chrome_options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.72 Safari/537.36')
    if B_HEADLESS:
        chrome_options.add_argument('headless')
    return webdriver.Chrome(str_chromedriver_path,
                            chrome_options=chrome_options)

def get_lst_capes_to_download(df_capes,
                              str_download_folder_path):

    lst_id_pessoa = list(df_capes['ID_PESSOA'])

    lst_downloaded_files = get_lst_downloaded_files(str_download_folder_path)
    lst_downloaded_files = [os.path.basename(x).split('.')[0] for x in lst_downloaded_files]

    lst_diff = list(set(lst_id_pessoa) - set(lst_downloaded_files))
    df_capes = df_capes[df_capes['ID_PESSOA'].isin(lst_diff)]

    lst_return = df_capes.to_records(index=False).tolist()
    lst_return.sort()

    return lst_return

def get_lst_downloaded_files(str_download_folder_path):
    return glob.glob('{}/*.txt'.format(str_download_folder_path))

def get_lst_idcnpq_by_name(browser, wait_browser, str_name):
    str_url_search = 'http://buscatextual.cnpq.br/buscatextual/busca.do'
    str_url_preview = 'http://buscatextual.cnpq.br/buscatextual/preview.do?metodo=apresentar&id={}'

#    regex_id_k = r"javascript:abreDetalhe\('(.*?)','.*?',.*?,\)\""
    regex_id_k = r"<li>[\s\S]*?javascript:abreDetalhe\('(.*?)','.*?',.*?,\)\"[\s\S]+?\"><br>(?:<span.*?(Bolsista de Produtividade.*?)<\/span>)?[\s\S]*?<\/li>"
    regex_idcnpq_first_page = r"abrirLink\('http:.*?(\d{16})'\)"
    regex_idcnpq_det_page = r'">(\d{16})</span>'

    lst_idcnpq = []

    browser.get(str_url_search)

    browser.find_element_by_id('textoBusca').send_keys(str_name)
    browser.find_element_by_id('botaoBuscaFiltros').click()

    wait_browser.until(ec.element_to_be_clickable((By.CLASS_NAME,
                                                   'paginacao')))

    obj_match = re.findall(regex_id_k, browser.page_source)
    for match_id_k in obj_match:
        str_k_cnpq = match_id_k[0]
        str_bolsista = match_id_k[1]
        browser.get(str_url_preview.format(str_k_cnpq))

        obj_match_idcnpq = re.findall(regex_idcnpq_first_page,
                                      browser.page_source)

        if not obj_match_idcnpq:
            browser.find_element_by_class_name('m-logo').click()
            wait_browser.until(ec.number_of_windows_to_be(2))
            browser.switch_to.window(browser.window_handles[1])

            obj_match_idcnpq = re.findall(regex_idcnpq_det_page,
                                          browser.page_source)
            browser.close()
            browser.switch_to.window(browser.window_handles[0])

        lst_idcnpq.append('{},{}'.format(str(obj_match_idcnpq[0]),
                                         str_bolsista))
    return lst_idcnpq

def get_df_capes(str_path_file_capes):
    lst_cols = ['ID_PESSOA', 'NM_DOCENTE']

    df_capes = pd.read_excel(str_path_file_capes, usecols=lst_cols)

    util.convert_special_chars(df_capes, 'NM_DOCENTE')
    df_capes['ID_PESSOA'] = df_capes['ID_PESSOA'].astype('string')

    return df_capes.loc[~df_capes.duplicated(),]

def get_df_capes_lattes(str_download_folder_path):
    lst_capes_lattes = get_lst_downloaded_files(str_download_folder_path)

    lst_result = []
    for file_path in lst_capes_lattes:
        id_file = os.path.basename(file_path).split('.')[0]
        if os.path.getsize(file_path):
            with open(file_path, 'r') as file:
                lst_content_file = [line.strip().split(',') + [id_file] for line in file]
        else:
            print('empty file: {}'.format(file_path))
            lst_content_file = [['', '', id_file]]

        lst_result.extend(lst_content_file)

    df_result = pd.DataFrame(lst_result)
    df_result.columns = ['ID_CNPQ', 'Bolsista', 'ID_PESSOA']

    df_result['Homonimo'] = df_result['ID_PESSOA'].duplicated(keep=False)
    df_result['ID_CNPQ'] = df_result['ID_CNPQ'].astype(str)

    return df_result

def show_download_progress(lst_id_names):
    n_start_size = len(lst_id_names)
    time_start = time.time()
    lst_count = [n_start_size] * 16

    estimate = 0
    str_msg_status = 'processing average: {}, {}, {} - missing: {} - estimate: {} - passed: {}'
    while lst_id_names:
        lst_count.insert(0, len(lst_id_names))
        lst_count.pop()

        if (lst_count[15] - lst_count[0]) > 0:
            estimate = int(lst_count[0] / (lst_count[15] - lst_count[0])) * 15

        print(' ' * 100, end='\r')
        print(str_msg_status.format(lst_count[1] - lst_count[0],
                                    lst_count[5] - lst_count[0],
                                    lst_count[15] - lst_count[0],
                                    lst_count[0],
                                    str(datetime.timedelta(seconds=estimate)),
                                    str(datetime.timedelta(seconds=int(time.time() - time_start)))),
              end='\r')
        time.sleep(1)
    print('\nfim')

def start_threads_download_idcnpq(n_threads_count, lst_id_names,
                                  str_download_folder_path):
    print('{} names to search'.format(len(lst_id_names)))

    for i in range(0, n_threads_count):
        t_down = Thread(target=download_idcnpq_by_lst_id_names,
                        args=(i,
                              str_download_folder_path,
                              lst_id_names))
        t_down.start()
        time.sleep(1)

    t_progress = Thread(target=show_download_progress, args=(lst_id_names,))
    t_progress.start()
    t_progress.join()

def main():
    args = get_args()
    str_path_file_capes= args.input_file
    str_download_folder_path = args.output_folder

    str_download_folder_path = str_download_folder_path.rstrip('/')

    df_capes = get_df_capes(str_path_file_capes)

    n_attempts = 3
    n_threads_count = 3
    n_count = 0
    while n_count < n_attempts:
        n_count += 1
        print('attempt:{}'.format(n_count))

        lst_id_names = get_lst_capes_to_download(df_capes,
                                                 str_download_folder_path)

        start_threads_download_idcnpq(n_threads_count, lst_id_names,
                                      str_download_folder_path)


    lst_id_names = get_lst_capes_to_download(df_capes,
                                             str_download_folder_path)

    df_missing = df_capes[df_capes['ID_PESSOA'].isin([x[0] for x in lst_id_names])]
    df_missing.to_csv(f"{str_download_folder_path}/missing_lattes.csv", index=False)

    df_capes_lattes = get_df_capes_lattes(str_download_folder_path)
    df_capes_lattes.to_csv(f"{str_download_folder_path}/capes-x-lattes.csv",
                           index=False)

    df_capes_lattes['ID_CNPQ'].to_csv(f"{str_download_folder_path}/idlattes_to_download.csv",
                                      index=False, header=False)

main()
