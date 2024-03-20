# Integrador Lattes-Capes

O Integrador Lattes-Capes é uma ferramenta em Python desenvolvida para relacionar os dados de docentes da Coordenação de Aperfeiçoamento de Pessoal de Nível Superior (Capes) com seus respectivos perfis no Lattes, uma plataforma do Conselho Nacional de Desenvolvimento Científico e Tecnológico (CNPq). No Brasil, essas duas entidades são fundamentais para o monitoramento e avaliação da produção científica acadêmica.

## Objetivo
O objetivo deste projeto é facilitar a integração dos dados da Capes com os perfis dos pesquisadores no Lattes, permitindo uma análise mais abrangente e precisa da produção científica brasileira. Ao relacionar os identificadores únicos dos docentes da Capes com seus respectivos perfis no Lattes, é possível enriquecer as análises sobre a produção acadêmica, proporcionando insights valiosos para pesquisadores, instituições de ensino e órgãos governamentais.


O processo é dividido em uma etapa manual e quatro etapas automatizadas. A seguir, são detalhados os passos necessários para utilizar os scripts e realizar o relacionamento entre as bases de dados.

## Funcionalidades
- Descarga dos currículos Lattes.
- Extração dos dados de produção científica dos perfis no Lattes.
- Geração de tabelas de relação entre os identificadores da Capes e do Lattes.
- Relacionamento dos dados da Capes com os identificadores do Lattes.

## Como Usar
Para utilizar o Integrador Lattes-Capes, siga as instruções abaixo:

1. Clone este repositório em sua máquina local.
1. Instale as dependências necessárias especificadas no arquivo requirements.txt.
1. Execute os scripts Python conforme instruções abaixo.

## Pré-requisitos

Certifique-se de ter os seguintes requisitos antes de executar o projeto:

- Cadastro e créditos no serviço Dead By Captcha 
- Python instalado
- Chromedriver compatível com a versão do sistema operacional e do Google Chrome

## Passo 1: Baixar Dados da Plataforma Sucupira da Capes (Manual)

Antes de iniciar o processo automatizado, é necessário baixar os dados da Plataforma Sucupira da Capes, que contêm informações dos docentes. Esses dados devem ser salvos em um arquivo no formato de planilha eletrônica (por exemplo, Excel), contendo pelo menos uma coluna com identificadores e outra com os nomes dos docentes. Para baixar dados de áreas de pesquisa ou instituições específicas, filtre a planilha pela opção negativa e exclua as linhas.

**Resultado esperado:**
Um arquivo no formato de planilha eletrônica contendo as linhas referentes à área de pesquisa ou instituição que se deseja analisar.

## Passo 2: Baixar Identificadores Lattes

O primeiro passo automatizado consiste na obtenção dos identificadores Lattes a partir dos nomes dos docentes baixados da plataforma Sucupira. O script `download_id_lattes.py` executa esse processo. Utiliza técnicas de web scraping com Selenium para recuperar IDs CNPQ e pandas para o processamento de dados. Realiza consultas por nome do site da Plataforma Lattes e grava arquivos TXT com os identificadores Lattes encontrados. Esse processo é repetido três vezes para lidar com possíveis problemas de conexão. O resultado é consolidado no arquivo `capes-x-lattes.csv`, os nomes não encontrados são listados no arquivo `missing_lattes.csv` e os IDS para download no arquivo `idlattes_to_download.csv`.

Sintaxe:
```
python3 download_id_lattes.py <arquivo_entrada.xlsx> <pasta_saida>
```
Substitua `<arquivo_entrada.xlsx>` pelo caminho para o seu arquivo Excel de dados CAPES e `<pasta_saida>` pelo caminho onde você deseja salvar os resultados.
Exemplo:
```
python3 ./scripts/download_id_lattes.py ./data/capes-2020.xlsx capes_x_lattes
```

**Resultado esperado:**
O diretório informado no argumento `<pasta_saida>` deverá conter um arquivo txt para cada nome no `<arquivo_entrada.xlsx>` e os arquivos: `capes-x-lattes.csv`, `missing_lattes.csv` e `idlattes_to_download.csv` que serão utilizados nos próximos passos.

## Passo 3: Baixar Currículos Lattes (XML)

O próximo passo descarrega os currículos Lattes na forma de arquivos XML. O script `download_xml_lattes.py` realiza essa tarefa. Ele utiliza os identificadores Lattes obtidos no passo anterior para fazer o download dos respectivos currículos. Além disso, para superar desafios de CAPTCHA, o script usa o serviço Dead by Captcha. É necessário se cadastrar no serviço e fornecer as credenciais de acesso no arquivo `config_dbc_credentials.py`.

Sintaxe:
```
python download_xml_lattes.py <arquivo_entrada.csv> <pasta_saida>
```
Substitua `<arquivo_entrada.csv>` pelo caminho para o arquivo gerado no passo anterior e `<pasta_saida>` pelo caminho onde você deseja salvar os resultados.

Exemplo:
```
python3 ./scripts/download_xml_lattes.py capes_x_lattes/idlattes_to_download.csv xml_lattes
```

**Resultado esperado:**
O diretório informado no argumento `<pasta_saida>` deverá conter um arquivo zip para cada nome no `<arquivo_entrada.csv>`.

## Passo 4: Interpretar Currículos Lattes (XML)

Com os currículos Lattes em formato XML baixados, o script `parse_xml_lattes.py` interpreta esses arquivos e gera três arquivos CSV consolidando os dados das seções de dados gerais, formação e produção dos currículos. Primeiro os arquivos zip baixados no passo anterior são descompactados, em seguida os arquivos XML são interpretados e, por fim, são gravados os arquivos: `lattes_producao.csv`, `lattes_dados_gerais.csv`, `lattes_formacao.csv` e `id_lattes_to_disambiguate.csv`. Os três primeiros poderão ser usados depois para os detalhes do currículo Lattes com os dados da CAPES. O último arquivo será usado no passo seguinte para combinar as bases Capes e Lattes.

Sintaxe:
```
python3 parse_xml_lattes.py <pasta_entrada>
```
Substitua <pasta_entrada> pelo caminho onde os arquivos foram baixados no passo anterior.
Exemplo:
```
python3 ./scripts/parse_xml_lattes.py xml_lattes
```

## Passo 5: Combinar Dados Capes-Lattes

O último passo automatizado consiste em combinar os dados obtidos da Capes com os dados consolidados do currículo Lattes. O script `merge_capes_x_lattes.py` executa essa combinação utilizando as variáveis nome do docente, instituição de titulação e ano de titulação. O processo consiste em comparar as informações das duas bases partindo do critério mais rígido em direção ao critério mais folgado. Na primeira combinação, são usadas as três variáveis. Na segunda iteração, são usados nome do docente e instituição de titulação. Na terceira, nome do docente e ano de titulação. Depois, apenas o nome do docente. Por fim, são feitas três tentativas de combinação usando somente o primeiro nome do docente em conjunto com as outras variáveis da mesma maneira que nas interações com o nome completo. Nestas combinações que usam apenas o primeiro nome, o resultado é filtrado pela semelhança entre os nomes completos calculado pela biblioteca `fuzzywuzzy`.

Sintaxe:
```
python merge_capes_x_lattes.py <arquivo_entrada_capes.xlsx> <arquivo_entrada_lattes.csv>
```
Substitua `<arquivo_entrada.xlsx>` pelo caminho para o seu arquivo Excel de dados CAPES e `<arquivo_entrada_lattes.csv>` pelo caminho para o arquivo criado no passo anterior.
Exemplo:
```
python3 ./scripts/merge_capes_x_lattes.py ./data/capes-2020.xlsx ./data/id_lattes_to_disambiguate.csv
```

**Resultado esperado:**
Serão gravados os arquivos `match_capes_x_lattes.csv` e `capes_not_found_in_lattes.csv` no diretório `./data`. No primeiro arquivo, as variáveis de interesse são ID_PESSOA da Capes e FILE-NAME do Lattes, sendo essa última o identificador único na plataforma do CNPq. Com essa informação é possível combinar as duas bases. Por exemplo, o arquivo `lattes_producao.csv`, gerado no passo 4, contém a variável FILE-NAME (id Lattes) que, agora, tem uma relação estabelecida com id_pessoa da Capes.

Exemplo de saída no terminal:
```
i:0, count:11401, low_match:0, key:NM_DOCENTE+NM_IES_TITULACAO+AN_TITULACAO
i:1, count:707, low_match:0, key:NM_DOCENTE+NM_IES_TITULACAO
i:2, count:4025, low_match:714, key:NM_DOCENTE+AN_TITULACAO
i:3, count:660, low_match:278, key:NM_DOCENTE
i:4, count:32, low_match:0, key:prim_nome+NM_IES_TITULACAO+AN_TITULACAO
i:5, count:10, low_match:0, key:prim_nome+NM_IES_TITULACAO
i:6, count:20, low_match:11, key:prim_nome+AN_TITULACAO
i:7, count:36, low_match:21, key:prim_nome
```

**Falsos positivos:**
É possível que o processo gere falsos positivos nas etapas mais frouxas da combinação. Na saída acima, a informação low_match indica a quantidade de registros nos quais a semelhança entre as informações é baixa (nome, instituição e ano de titulação). O arquivo `match_capes_x_lattes.csv` contém esses detalhes e fica a critério do usuário excluir tais registros.

**Casos faltantes:**
Nem todos os nomes constantes na base da Capes são encontrados no Lattes; essa é a informação gravada no arquivo `capes_not_found_in_lattes.csv`. Segundo a nossa experiência, a maioria desses casos ocorre porque a pessoa usa um nome mais curto no Lattes. Pode-se baixar manualmente os currículos Lattes dessas pessoas na pasta onde os demais arquivos foram descarregados. Também é possível criar uma segunda lista de nomes e repetir o processo.

## Observações Importantes

- Certifique-se de ter as dependências necessárias instaladas antes de executar os scripts. Para instalá-las, execute o seguinte comando:
`pip install -r requirements.txt`
- O script `download_id_lattes.py` utiliza o ChromeDriver. Descompacte a versão compatível com seu sistema operacional e versão do Chrome no diretório scripts.
- Para usar o script `download_xml_lattes.py`, você precisa se cadastrar no serviço Dead by Captcha. Após o cadastro, descompacte o arquivo zip da API em Python no diretório scripts e informe o username e a password no arquivo `config_dbc_credentials.py`.
- Os scripts `download_id_lattes.py` e `download_xml_lattes.py` têm mecanismos de tolerância a falhas e evitam duplicações de download.

Através da execução sequencial desses scripts, é possível relacionar as informações da Plataforma Capes com os currículos da Plataforma Lattes, fornecendo uma visão mais completa dos dados dos docentes e facilitando análises e pesquisas futuras.

## Licença
Este projeto é licenciado sob a [MIT License](https://opensource.org/licenses/MIT).
