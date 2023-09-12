# Projeto Diversidade na Ciência - Serrapilheira

Scripts para Baixar e Tratar Currículos da Plataforma Lattes do CNPq

Este repositório contém um conjunto de scripts desenvolvidos no escopo do projeto Diversidade na Ciẽncia do Instituto Serrapilheria para estabelecer uma relação entre as informações da Plataforma Sucupira da Capes e os currículos na Plataforma Lattes do CNPq. O processo é dividido em uma etapa manual e quatro etapas automatizadas. A seguir, são detalhados os passos necessários para utilizar os scripts e realizar o relacionamento entre as bases de dados.

## Pré-requisitos

Certifique-se de ter os seguintes requisitos antes de executar o projeto:

- Cadastro e créditos no serviço Dead By Captcha 
- Python instalado
- Chromedriver compatível com a versão do sistema operacional e do Google Chrome

## Passo 1: Baixar Dados da Plataforma Sucupira da Capes (Manual)

Antes de iniciar o processo automatizado, é necessário baixar os dados da Plataforma Sucupira da Capes, que contêm informações dos docentes. Esses dados devem ser salvos em um arquivo no formato de planilha eletrônica (por exemplo, Excel), contendo pelo menos uma coluna com identificadores e outra com os nomes dos docentes.

## Passo 2: Baixar Identificadores Lattes

O primeiro passo automatizado envolve a obtenção dos identificadores Lattes a partir dos nomes dos docentes baixados da plataforma Sucupira. O script `download_id_lattes.py` executa esse processo. Ele acessa a Plataforma Lattes, realiza consultas por nome e grava arquivos TXT com os identificadores Lattes correspondentes. Esse processo é repetido três vezes para lidar com possíveis problemas de conexão. O resultado é consolidado no arquivo `capes-x-lattes.csv`, os nomes não encontrados são listados no arquivo `missing_lattes.csv` e os IDS para download no arquivo `idlattes_to_download.csv`.

Exemplo de uso:
```
python3 download_id_lattes.py input/capes-2020.xlsx capes_x_lattes
```

## Passo 3: Baixar Currículos Lattes (XML)

O próximo passo é baixar os currículos Lattes na forma de arquivos XML. O script `download_xml_lattes.py` realiza essa tarefa. Ele utiliza os identificadores Lattes obtidos no passo anterior para fazer o download dos respectivos currículos. Além disso, para superar desafios de CAPTCHA, o script usa o serviço Dead by Captcha. É necessário se cadastrar no serviço e fornecer as credenciais de acesso no arquivo `config_dbc_credentials.py`.

Exemplo de uso:
```
python3 download_xml_lattes.py capes_x_lattes/idlattes_to_download.csv xml_lattes
```


## Passo 4: Interpretar Currículos Lattes (XML)

Com os currículos Lattes em formato XML baixados, o script `parse_xml_lattes.py` interpreta esses arquivos e gera três arquivos CSV consolidando os dados das seções de dados gerais, formação e produção dos currículos.

Exemplo de uso:
```
python3 parse_xml_lattes.py xml_lattes
```


## Passo 5: Combinar Dados Capes-Lattes

O último passo automatizado consiste em combinar os dados obtidos da Capes com os dados consolidados do currículo Lattes. O script `merge_capes_x_lattes.py` realiza essa tarefa utilizando variáveis como nome do docente, instituição de titulação e ano de titulação para realizar a combinação.

Exemplo de uso:
```
python3 merge_capes_x_lattes.py xml_lattes
```


## Observações Importantes

- Certifique-se de ter as dependências necessárias instaladas antes de executar os scripts. Para instalá-las, execute o seguinte comando:
`pip install -r requirements.txt`
- O script `download_id_lattes.py` utiliza o ChromeDriver. Descompacte a versão compatível com seu sistema operaciona e versão do Chrome no diretório scripts.
- Para usar o script `download_xml_lattes.py`, você precisa se cadastrar no serviço Dead by Captcha. Após o cadastro, descompacte o arquivo zip da API em Python no diretório scripts e informe username e password no arquivo `config_dbc_credentials.py`.
- Os scripts `download_id_lattes.py` e `download_xml_lattes.py` têm mecanismos de tolerância a falhas e evitam duplicações de download.

Através da execução sequencial desses scripts, é possível relacionar as informações da Plataforma Capes com os currículos da Plataforma Lattes, fornecendo uma visão mais completa dos dados dos docentes e facilitando análises e pesquisas futuras.
