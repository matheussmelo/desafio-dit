# Desafio técnico DIT para Cientista de Dados

## Bem-vindo!

Neste repositório é apresentado todo o pipeline de desenvolvimento do desafio técnico proposto para a vaga de Cientista de Dados DIT. 

Sendo assim, o desenvolvimento consistiu em:

## 1. Configuração do DBT local com VSCode

O primeiro passo do projeto foi realizar a configuração do DBT (Data Build Tool) localmente seguindo um passo a passo para futuramente poder desenvolver o modelo que tratou os dados brutos em uma nova tabela transformada. Foi criado um ambiente virtual chamado dbt_venv para poder inicializar a configuração do dbt e utilizá-lo quando necessário (como na hora de realizar a transformação pelo modelo dbt, por exemplo).

O projeto DBT pode ser encontrado na pasta **desafio_dit**.

## 2. Análise Exploratória dos Dados

Essa etapa consistiu em analisar o conjunto de dados brutos disponibilizado no arquivo **dados_ficha_a_desafio.csv** com informações descritivas presentes em **descricao_de_campos.xlsx**. O objetivo foi entender a estrutura dos dados, identificar possíveis inconsistências e sugerir melhorias na qualidade da informação. Além disso, foi necessário interpretar o contexto de geração dos dados, levantar questionamentos e apontar problemas que poderiam impactar análises futuras.

Todo o processo pode ser visto no arquivo **eda_pré.ipynb**, o qual fiz a análise exploratória dos dados em um formato de relatório.

O que foi encontrado: 

- **obito, luz_eletrica, em_situacao_de_rua, possui_plano_saude, vulnerabilidade_social, familia_beneficiaria_auxilio_brasil, crianca_matriculada_creche_pre_escola**: Essas colunas possuem valores misturados entre 0, 1, False e True. Logo, irei transforma-las para apenas 0 (False) e 1 (True).

- **sexo**: Será necessário traduzir para o português. Sugiro que no ato da coleta, as opções no campo estejam em português.

- **religiao**: Essa coluna apresenta algumas religiões possíveis, mas também há valores inconsistentes, como 'ESB ALMIRANTE', '10 EAP 01', 'Acomp. Cresc. e Desenv. da Criança' que são valores que não fazems sentido para essa parte. 

    Além disso, 'Sim' e 'Não' não agregam valor, considerando que é redundante responder 'Sim' sendo que foi perguntado a religião, enquanto que 'Não' também é redudante já que há a opção 'Sem religião'. Recomendo remover a opção de 'Sim' e 'Não' dos questionários.

- **renda_familiar**: Alguns valores não deveriam estar presentes como 'Manhã' e 'Internet'.

- **identidade_genero**: Essa variável possui informações coletadas que são delicadas e apresentam alguns erros relevantes. Primeiramente, Heterosesexual, Homossexual (gay / lésbica)' e 'Bissexual' se refere à orientação sexual e não identidade de gênero, então seria interessante remover essas opções desse campo. 

    Além disso, as cláusulas 'Sim' e 'Não' se comportam da mesma forma que no campo de religião, então recomendo fazer os mesmos tratamentos.

    O valor nulo pode ser justificado pelo fato do campo não ter sido preenchido, então para evitá-lo seria ético considerar o preenchimento obrigatório mas com a opção de não querer responder ou não saber informar. Por fim, como já existe um campo para dizer o sexo, seria interessante considerar nesse campo apenas as opções 'Cis', 'Trans', 'Outro' e 'Prefere não responder/Não sabe informar', assim evitando redundâncias como 'Homem transexual' e 'Mulher transexual'. 

- **situacao_profissional**: Esse campo possui valores corretos, com exceção de informações que não pertencem como 'SMS CAPS DIRCINHA E LINDA BATISTA AP 33' e 'Médico Urologista'. Além disso, 'Não trabalha'/'Desempregado'/'Não se aplica' são redundantes, podendo ser apenas um dos três.

- **meios_transporte, doencas_condicoes, meios_comunicacao, em_caso_doenca_procura**: Os dados fazem parte de listas que sugerem ter vindo de checkboxs de marcação múltipla, então existem várias possibilidades para cada. Porém, para cada um deles será necessário tratá-los removendo-os de dentro de possíveis "[ ]" e também remover erros de parser.

    Especificamente, os campos **meios_comunicacao** e **em_caso_doenca_procura** além de erros de parser, também apresentam valores que não deveriam ser desse campo.

## 3. Criação do modelo

Por fim, nessa etapa envolveu o tratamento dos dados brutos analisados acima utilizando um modelo DBT feito em SQL e gerando uma nova tabela tratada (utilizei o PostgreSQL). A proposta foi desenvolver um modelo que padronizasse e estruturasse o dataset, garantindo maior qualidade e consistência. O foco esteve na criação de um pipeline modularizado e legível, facilitando a manutenção e escalabilidade do processo.

O modelo pode ser visto em **/desafio-dit/models/dados_ficha_tratados.sql**, onde fiz uma sequência estruturada de CTE's para ter um pipeline fluido e legível.
