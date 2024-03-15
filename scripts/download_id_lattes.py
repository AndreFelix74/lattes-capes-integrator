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
import utils_lattes_cnpq as util


B_HEADLESS = True


def download_idcnpq_by_lst_id_names(str_thread_index, str_download_folder_path,
                                    lst_id_names):
    """
    Downloads CNPQ IDs by names from the CNPQ website and saves them to text files.

    This function iterates through a list of ID-name pairs, searches for each
    name on the CNPQ website,
    retrieves CNPQ IDs associated with the names, and saves them to text files
    in the specified folder.

    Args:
        str_thread_index (str): The index of the current thread.
        str_download_folder_path (str): Path to the folder where files will be downloaded.
        lst_id_names (list): A list of tuples containing CNPQ ID and name pairs.

    Returns:
        None

    Example:
        >>> download_idcnpq_by_lst_id_names('1', '/path/to/downloads',
        [('123', 'John Doe'), ('456', 'Jane Smith')])
    """

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
    """
    Parses command-line arguments and returns them.

    This function uses the argparse module to define and parse command-line arguments.
    It expects two arguments:
    - input_file: The name of the Excel file to be processed. It should point to an Excel
      file downloaded from the CAPES website, containing the variables ID_PESSOA and NM_DOCENTE.
    - output_folder: The path where the resulting text files will be saved.

    Returns:
        argparse.Namespace: An object containing the parsed command-line arguments.

    Example:
        Suppose the script is invoked with the following command:
        $ python script.py input.xlsx /path/to/output_folder

        >>> args = get_args()
        >>> print(args.input_file)
        'input.xlsx'
        >>> print(args.output_folder)
        '/path/to/output_folder'
    """
    parser = argparse.ArgumentParser(description='Arquivo de IDs para processar')

    parser.add_argument('input_file', metavar='content_file', type=str,
                        help='nome do arquivo a ser processado. Deve apontar '
                        'para um arquivo Excel baixado do site da Capes. O '
                        'arquivo deve conter as variaves ID_PESSOA e NM_DOCENTE')

    parser.add_argument('output_folder', metavar='output_path', type=str,
                        help='caminho onde serao gravados os arquivos txt')

    return parser.parse_args()


def get_browser():
    """
    Creates and returns a Chrome webdriver instance with specified options.

    This function initializes a ChromeOptions object and configures it with arguments:
    --no-sandbox: Disables the Chrome sandbox.
    --disable-gpu: Disables the GPU usage.
    disable-blink-features=AutomationControlled: Disables automation control features.
    user-agent: Sets the user agent to mimic Mozilla Firefox on Linux.

    If the global constant B_HEADLESS is True, 'headless' argument is added to
    run Chrome in headless mode.

    Returns:
        selenium.webdriver.Chrome: A Chrome webdriver instance configured with
        the specified options.

    Example:
        >>> browser = get_browser()
    """

    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('disable-blink-features=AutomationControlled')
    chrome_options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.72 Safari/537.36')

    if B_HEADLESS:
        chrome_options.add_argument('headless')

    return webdriver.Chrome(options=chrome_options)


def get_lst_capes_to_download(df_capes,
                              str_download_folder_path):
    """
    Retrieves a list of CAPES IDs to download based on the provided DataFrame
    and download folder path.

    This function takes a DataFrame containing CAPES data and a path to the folder
    where files are downloaded.
    It extracts the 'ID_PESSOA' column from the DataFrame and compares it with
    the list of downloaded files.
    It then returns a list of CAPES IDs that have not been downloaded yet,
    sorted in ascending order.

    Args:
        df_capes (pandas.DataFrame): DataFrame containing CAPES data with 'ID_PESSOA' column.
        str_download_folder_path (str): Path to the folder where files are downloaded.

    Returns:
        list: A sorted list of CAPES IDs to download.

    Example:
        >>> df_capes = pd.DataFrame({'ID_PESSOA': [123, 456, 789]})
        >>> downloaded_files = ['/path/to/downloads/123.txt', '/path/to/downloads/789.txt']
        >>> lst_to_download = get_lst_capes_to_download(df_capes, '/path/to/downloads')
        >>> print(lst_to_download)
        [(123,), (456,)]
    """

    lst_id_pessoa = list(df_capes['ID_PESSOA'])

    lst_downloaded_files = get_lst_downloaded_files(str_download_folder_path)
    lst_downloaded_files = [os.path.basename(x).split('.')[0] for x in lst_downloaded_files]

    lst_diff = list(set(lst_id_pessoa) - set(lst_downloaded_files))
    df_capes = df_capes[df_capes['ID_PESSOA'].isin(lst_diff)]

    lst_return = df_capes.to_records(index=False).tolist()
    lst_return.sort()

    return lst_return


def get_lst_downloaded_files(str_download_folder_path):
    """
    Retrieves a list of downloaded files with a .txt extension from the specified folder.

    Args:
        str_download_folder_path (str): The path to the folder where downloaded files are stored.

    Returns:
        list: A list of file paths of the downloaded files with a .txt extension.

    Example:
        >>> downloaded_files = get_lst_downloaded_files('/path/to/downloads')
        >>> print(downloaded_files)
        ['/path/to/downloads/file1.txt', '/path/to/downloads/file2.txt']
    """

    return glob.glob('{}/*.txt'.format(str_download_folder_path))


def get_lst_idcnpq_by_name(browser, wait_browser, str_name):
    """
    Retrieves a list of CNPQ IDs by searching for a given name on the CNPQ website.

    This function uses a Selenium WebDriver instance to search for a given name on the CNPQ website.
    It extracts CNPQ IDs and whether the person is a 'Bolsista' from the search results.

    Args:
        browser (selenium.webdriver): WebDriver instance for browser automation.
        wait_browser (selenium.webdriver.support.ui.WebDriverWait): WebDriverWait
        instance for waiting in the browser.
        str_name (str): The name to search for on the CNPQ website.

    Returns:
        list: A list of CNPQ IDs and their corresponding 'Bolsista' status.

    Example:
        >>> from selenium import webdriver
        >>> from selenium.webdriver.support.ui import WebDriverWait
        >>> browser = webdriver.Chrome()
        >>> wait_browser = WebDriverWait(browser, 10)
        >>> lst_idcnpq = get_lst_idcnpq_by_name(browser, wait_browser, 'John Doe')
        >>> print(lst_idcnpq)
        ['1234567890123456,Bolsista de Produtividade', '9876543210987654,']
    """

    str_url_search = 'http://buscatextual.cnpq.br/buscatextual/busca.do'
    str_url_preview = 'http://buscatextual.cnpq.br/buscatextual/preview.do?metodo=apresentar&id={}'

    regex_id_k = r"<li>[\s\S]*?javascript:abreDetalhe\('(.*?)','.*?',.*?,\)\"[\s\S]+?\"><br>(?:<span.*?(Bolsista de Produtividade.*?)<\/span>)?[\s\S]*?<\/li>"
    regex_idcnpq_first_page = r"abrirLink\('http:.*?(\d{16})'\)"
    regex_idcnpq_det_page = r'">(\d{16})</span>'

    lst_idcnpq = []

    browser.get(str_url_search)

    browser.find_element(By.ID, 'textoBusca').send_keys(str_name)

    browser.find_element(By.ID, 'botaoBuscaFiltros').click()

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
            browser.find_element(By.CLASS_NAME, 'm-logo').click()
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
    """
    Reads an Excel file containing CAPES data and returns a DataFrame with selected columns.

    This function reads the specified Excel file using pandas and selects only the columns
    'ID_PESSOA' and 'NM_DOCENTE'. It then applies a utility function to convert special characters
    in the 'NM_DOCENTE' column, and converts the 'ID_PESSOA' column to string type.
    Finally, it removes duplicate rows based on 'ID_PESSOA' and returns the resulting DataFrame.

    Args:
        str_path_file_capes (str): The path to the CAPES Excel file.

    Returns:
        pandas.DataFrame: A DataFrame containing selected columns from the CAPES data.

    Example:
        >>> df_capes = get_df_capes('/path/to/capes_data.xlsx')
        >>> print(df_capes.head())
           ID_PESSOA         NM_DOCENTE
        0     123456  John Doe
        1     789012  Jane Smith
    """
    lst_cols = ['ID_PESSOA', 'NM_DOCENTE']

    df_capes = pd.read_excel(str_path_file_capes, usecols=lst_cols)

    util.convert_special_chars(df_capes, 'NM_DOCENTE')
    df_capes['ID_PESSOA'] = df_capes['ID_PESSOA'].astype('string')

    return df_capes.loc[~df_capes.duplicated(),]


def get_df_capes_lattes(str_download_folder_path):
    """
    Reads CAPES Lattes files from the specified folder and returns a DataFrame.

    This function reads CAPES Lattes files from the specified folder and processes their content.
    It extracts data such as 'ID_CNPQ', 'Bolsista', and 'ID_PESSOA' from each
    file and creates a DataFrame.
    Additionally, it identifies homonymous entries based on 'ID_PESSOA'.

    Args:
        str_download_folder_path (str): Path to the folder containing CAPES Lattes files.

    Returns:
        pandas.DataFrame: A DataFrame containing extracted data from CAPES Lattes files.

    Example:
        >>> df_capes_lattes = get_df_capes_lattes('/path/to/downloads')
        >>> print(df_capes_lattes.head())
           ID_CNPQ  Bolsista ID_PESSOA  Homonimo
        0  123456789     True    123456      False
        1  987654321    False    789012      False
    """
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
    """
    Displays the progress of CNPQ ID download.

    This function continuously updates and displays the progress of CNPQ ID download
    based on the remaining number of names to search.

    Args:
        lst_id_names (list): A list of tuples containing CNPQ ID and name pairs.

    Returns:
        None

    Example:
        >>> show_download_progress([('123', 'John Doe'), ('456', 'Jane Smith')])
    """

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
    """
    Starts multiple threads for downloading CNPQ IDs by names.

    This function starts multiple threads for downloading CNPQ IDs by names
    from the CNPQ website
    and saves them to text files in the specified folder. It also starts a
    thread to show download progress.

    Args:
        n_threads_count (int): Number of threads to start for downloading.
        lst_id_names (list): A list of tuples containing CNPQ ID and name pairs.
        str_download_folder_path (str): Path to the folder where files will be downloaded.

    Returns:
        None

    Example:
        >>> start_threads_download_idcnpq(3,
        [('123', 'John Doe'), ('456', 'Jane Smith')], '/path/to/downloads')
    """

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
    """
    Main function to orchestrate the entire process of downloading CNPQ IDs and
    processing CAPES data.

    This function serves as the entry point for the entire process. It handles
    command-line arguments,
    initializes necessary variables, downloads CNPQ IDs, processes CAPES data,
    and saves the results to files.

    Returns:
        None
    """
    args = get_args()
    str_path_file_capes = args.input_file
    str_download_folder_path = util.format_path(args.output_folder)

    str_download_folder_path = str_download_folder_path.rstrip('/')

    if not os.path.exists(str_download_folder_path):
        os.makedirs(str_download_folder_path)

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

if __name__ == "__main__":
    main()
