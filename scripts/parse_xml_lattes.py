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
import pandas as pd

def convert_lst_prod_to_dataframe(lst_prod):
    df_producao = pd.DataFrame(lst_prod)

    del (df_producao['NOME-PARA-CITACAO'], df_producao['ORDEM-DE-AUTORIA'],
         df_producao['NRO-ID-CNPQ'],
         df_producao['PALAVRA-CHAVE-1'], df_producao['PALAVRA-CHAVE-2'],
         df_producao['PALAVRA-CHAVE-3'], df_producao['PALAVRA-CHAVE-4'],
         df_producao['PALAVRA-CHAVE-5'], df_producao['PALAVRA-CHAVE-6'],
         df_producao['SETOR-DE-ATIVIDADE-1'], df_producao['SETOR-DE-ATIVIDADE-2'],
         df_producao['SETOR-DE-ATIVIDADE-3'], df_producao['NOME-COMPLETO-DO-AUTOR']
         )

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
    del (df_producao['TITULO-DO-ARTIGO'], df_producao['TITULO-DO-LIVRO'],
         df_producao['TITULO-DO-CAPITULO-DO-LIVRO'], df_producao['TITULO-DO-TEXTO'],
         df_producao['TITULO-DO-ARTIGO-INGLES'],
         df_producao['TITULO-DO-LIVRO-INGLES'],
         df_producao['TITULO-DO-CAPITULO-DO-LIVRO-INGLES'],
         df_producao['TITULO-DO-TEXTO-INGLES']
         )

    df_producao['NUM-CLASSIFICACAO'] = df_producao['ISSN'] + df_producao['ISBN']
    del (df_producao['ISSN'], df_producao['ISBN'])

    df_producao['ANO'] = (df_producao['ANO'] +
                          df_producao['ANO-DO-TEXTO'] +
                          df_producao['ANO-DO-ARTIGO']
                          )
    del(df_producao['ANO-DO-TEXTO'], df_producao['ANO-DO-ARTIGO'])

    df_producao['PAIS'] = (df_producao['PAIS'] + df_producao['PAIS-DE-PUBLICACAO'])
    del df_producao['PAIS-DE-PUBLICACAO']

    df_producao['TITULO-DO-PERIODICO-OU-JORNAL-OU-REVISTA'] = (
        df_producao['TITULO-DO-JORNAL-OU-REVISTA'] +
        df_producao['TITULO-DO-PERIODICO-OU-REVISTA']
        )
    del(df_producao['TITULO-DO-JORNAL-OU-REVISTA'],
        df_producao['TITULO-DO-PERIODICO-OU-REVISTA']
        )

    return df_producao

def dictify_flat(str_id, node):
    dict_return = dict()
    dict_return['Identificador'] = str_id
    for node_child in node.findall("./*"):
        for att in node_child.attrib:
            dict_return[att] = node_child.attrib[att]

    return dict_return

def dictify_xml_node_att(node):
    dict_return = dict()
    if node is not None:
        for att in node.attrib:
            dict_return[att] = node.attrib[att]

    return dict_return

def get_args():
    parser = argparse.ArgumentParser(description='Pasta com arquivos XML para '
                                     'processar.')
    parser.add_argument('input_folder', metavar='content_folder', type=str,
                        help='caminho dos arquivos a serem processados. Deve '
                        'conter arquivos xml baixados do site do CNPq')

    parser.add_argument('output_folder', metavar='output_path', type=str,
                        help='caminho onde serao gravados os arquivos csv')

    return parser.parse_args()

def get_lst_node_path(node_name):
    dct_node = {'artigo': './/PRODUCAO-BIBLIOGRAFICA/ARTIGOS-PUBLICADOS/ARTIGO-PUBLICADO',
                'livro': './/LIVROS-E-CAPITULOS/LIVROS-PUBLICADOS-OU-ORGANIZADOS/',
                'lvr_cap': './/LIVROS-E-CAPITULOS/CAPITULOS-DE-LIVROS-PUBLICADOS/',
                'jor_rev': './/TEXTOS-EM-JORNAIS-OU-REVISTAS/',
                'evt_org': './/PRODUCAO-TECNICA/DEMAIS-TIPOS-DE-PRODUCAO-TECNICA/ORGANIZACAO-DE-EVENTO',
                'evt_part': './/PARTICIPACAO-EM-EVENTOS-CONGRESSOS/PARTICIPACAO-EM-CONGRESSO',
                'premio': './/PREMIOS-TITULOS/PREMIO-TITULO'}
    return dct_node[node_name]

def parse_files(str_file_name):
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
    lst_return = []
    for node_area in root.findall('.//AREAS-DE-ATUACAO/AREA-DE-ATUACAO'):
        dict_aux = dict()
        dict_aux['Identificador'] = str_id
        dict_aux.update(dictify_xml_node_att(node_area))
        lst_return.append(dict_aux)

    return lst_return

def parse_files_get_formacao_doutorado(str_id, root):
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
    str_node_path = get_lst_node_path(node_name)
    lst_return = []
    for node in root.findall(str_node_path):
        lst_return.append(dictify_flat(str_id, node))

    for dic in lst_return: dic['tipo_prod'] = node_name

    return lst_return


def main():
    args = get_args()
    str_path_xml_files = args.input_folder

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

    df_dados_gerais = pd.DataFrame(lst_dados_gerais)
    df_formacao = pd.DataFrame(lst_formacao)

    df_desambiguate = df_dados_gerais.merge(df_formacao,
                                            how='left',
                                            left_on='FILE-NAME',
                                            right_on='Identificador')

    df_desambiguate = df_desambiguate.loc[:, ['FILE-NAME', 'NOME-COMPLETO',
                                              'ANO-DE-OBTENCAO-DO-TITULO',
                                              'NOME-INSTITUICAO']]
