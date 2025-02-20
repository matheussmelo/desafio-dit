{{config(materialized='table') }}

-- Nesse script serão utilizadas Common Table Expressions (CTE's) para realizar a transformação dos dados de forma organizada

-- CTE para criar a tabela original e adicionar um identificador incremental na primeira coluna (Será útil para unir todos os CTE's ao final e gerar a tabela transformada)
WITH cte_dados_ficha_com_id AS (
    SELECT ROW_NUMBER() OVER () AS linha_id, *
	
    FROM public.dados_ficha
),

-- CTE para filtrar os dados que não terão nenhuma transformação
cte_dados_sem_transformacao AS (
	SELECT linha_id, id_paciente, bairro, raca_cor, ocupacao, data_cadastro, escolaridade, nacionalidade, data_nascimento, 
		   frequenta_escola, orientacao_sexual, data_atualizacao_cadastro, altura, peso, pressao_sistolica, 
		   pressao_diastolica, n_atendimentos_atencao_primaria, n_atendimentos_hospital, updated_at, tipo
		   
	FROM cte_dados_ficha_com_id
),

-- CTE para normalizar as colunas que possuem 0/1/True/False apenas para binário 0/1
cte_bool_bin_transformado AS(
SELECT  linha_id,

CASE WHEN obito IN ('True', '1') THEN 1 
     WHEN obito IN ('False', '0') THEN 0
	 END AS obito,

CASE WHEN luz_eletrica IN ('True', '1') THEN 1
     WHEN luz_eletrica IN ('False', '0') THEN 0
	 END AS luz_eletrica,

CASE WHEN em_situacao_de_rua IN ('True', '1') THEN 1
     WHEN em_situacao_de_rua IN ('False', '0') THEN 0
	 END AS em_situacao_de_rua,

CASE WHEN possui_plano_saude IN ('True', '1') THEN 1
     WHEN possui_plano_saude IN ('False', '0') THEN 0
	 END AS possui_plano_saude,

CASE WHEN vulnerabilidade_social IN ('True', '1') THEN 1
     WHEN vulnerabilidade_social IN ('False', '0') THEN 0
	 END AS vulnerabilidade_social,

CASE WHEN familia_beneficiaria_auxilio_brasil IN ('True', '1') THEN 1
     WHEN familia_beneficiaria_auxilio_brasil IN ('False', '0') THEN 0
	 END AS familia_beneficiaria_auxilio_brasil,

CASE WHEN crianca_matriculada_creche_pre_escola IN ('True', '1') THEN 1
     WHEN crianca_matriculada_creche_pre_escola IN ('False', '0') THEN 0
	 END AS crianca_matriculada_creche_pre_escola

FROM cte_dados_ficha_com_id
),

-- CTE para trocar o sexo de inglês para português
cte_sexo_transformado AS(
SELECT linha_id,

CASE WHEN sexo = 'male' THEN 'Masculino'
	 WHEN sexo = 'female' THEN 'Feminino'
	 ELSE sexo
	 END AS sexo
	 
FROM cte_dados_ficha_com_id
),

-- CTE para manter na coluna de religião apenas valores que pertencem a essa categoria, removendo informações como 'ESB ALMIRANTE'/'10 EAP 01'/'Acomp. Cresc. e Desenv. da Criança'/'ORQUIDEA'
cte_religiao_transformado AS(
SELECT linha_id,
       CASE WHEN religiao IN ('Sem religião', 'Evangélica', 'Católica', 'Outra', 'Espírita',
							 'Religião de matriz africana', 'Não', 'Budismo', 'Judaísmo', 
							 'Candomblé', 'Islamismo', 'Sim') 				
		THEN religiao
		ELSE ''
		END AS religiao
	 
FROM cte_dados_ficha_com_id
),

-- CTE para manter na coluna de renda familiar apenas valores que pertencem à essa categoria, removendo informações como 'Manhã'/'Internet'
cte_renda_familiar_transformado AS(
SELECT linha_id, 

CASE WHEN renda_familiar IN ('1/2 Salário Mínimo', '2 Salários Mínimos', '1/4 Salário Mínimo', 
							'3 Salários Mínimos', 'Mais de 4 Salários Mínimos', '4 Salários Mínimos', '1 Salário Mínimo') 
		THEN renda_familiar
	 	ELSE ''
	 	END AS renda_familiar

FROM cte_dados_ficha_com_id
),

-- CTE para manter na coluna de identidade de genero apenas valores que pertencem à essa categoria, removendo informações relacionadas à orientação sexual
-- Também trocar 'Homem Transexual' e 'Mulher Transexual' para 'Trans' para evitar redundância já que existe uma coluna 'sexo'
cte_identidade_genero_transformado AS(
SELECT linha_id, 
		CASE WHEN identidade_genero IN ('Travesti', 'Não', 'Sim', 'Outro', 'Cis') THEN identidade_genero
		WHEN identidade_genero IN ('Mulher transexual', 'Homem transexual') THEN 'Trans'				     
		ELSE ''
		END AS identidade_genero

FROM cte_dados_ficha_com_id
),

-- CTE para manter na coluna de situação profissional apenas valores que pertencem à essa categoria, removendo informações como 'SMS CAPS DIRCINHA E LINDA BATISTA AP 33'/'Médico Urologista
cte_situacao_profissional_transformado AS(
SELECT linha_id, 

		CASE WHEN situacao_profissional IN ('Não se aplica', 'Autônomo com previdência social', 'Emprego Informal', 'Não trabalha', 'Autônomo', 'Pensionista / Aposentado',
											'Outro', 'Desempregado', 'Autônomo sem previdência social', 'Empregador', 'Emprego Formal') 
		THEN situacao_profissional
		ELSE ''
		END AS situacao_profissional

FROM cte_dados_ficha_com_id
),

-- CTE para editar os erros de parser vindos da coleta do campo de meios de transporte e também tirar colchetes e aspas
-- \u00d4 = Ô, \u00f4 = ô, \u00e7 = ç, \u00e3 = ã, \u00ed = í
cte_meios_transporte_transformado AS(
SELECT linha_id, REPLACE(
					REPLACE(
						REPLACE(
							REPLACE(
								REPLACE(
									REPLACE(
										REPLACE(
											REPLACE(meios_transporte,'\u00d4', 'Ô'), 
										'\u00f4', 'ô'), 
									'\u00e7', 'ç'), 
								'\u00e3', 'ã'), 
							'\u00ed', 'í'), 
						'[', ''), 
					']', ''), 
				'''', '') AS meios_transporte

FROM cte_dados_ficha_com_id
),

-- CTE para editar os erros de parser vindos da coleta do campo de doencas_condicoes e também tirar colchetes e aspas
-- \u00e3 = ã, \u00e1 = á, \u00ed = í, \u00f3 = ó, \u00e2 = â, '\u00e9' = 'é'
cte_doencas_condicoes_transformado AS (
SELECT linha_id, REPLACE(
					REPLACE(
						REPLACE(
							REPLACE(
								REPLACE(
									REPLACE(
										REPLACE(
											REPLACE(
												REPLACE(
													REPLACE(doencas_condicoes,'\u00e3', 'ã'), 
												'\u00e1', 'á'), 
											'\u00ed', 'í'), 
										'\u00f3', 'ó'), 
									'\u00e2', 'â'), 
								'\u00e9', 'é'), 
							'\u00ea', 'ê'), 
						'[', ''), 
					']', ''), 
				'''', '') AS doencas_condicoes

FROM cte_dados_ficha_com_id 
),

-- CTE para editar os erros de parser vindos da coleta do campo de meios_comunicacao e também tirar colchetes e aspas
-- \u00e3 = ã e \u00e1 = á 
cte_meios_comunicacao_transformado1 AS(
SELECT	linha_id, REPLACE(
						REPLACE(
							REPLACE(
								REPLACE(
									REPLACE(meios_comunicacao,'\u00e3', 'ã'), 
								'\u00e1', 'á'), 
							'[', ''), 
						']', ''), 
					'''', '') AS meios_comunicacao
									
FROM cte_dados_ficha_com_id 
),

-- CTE com uma linha para cada meio de comunicação válido
cte_meios_comunicao_validos AS (
	SELECT UNNEST(ARRAY['Internet', 'Televisão', 'Rádio', 'Jornal', 'Revista', 'Outros']) AS meio_comunicao_valido
),

-- CTE para usar a coluna de meio de comunicação sem erros de parser e remover apenas valores das listas que não sejam dessa categoria 
cte_meios_comunicacao_transformado2 AS (
	SELECT linha_id, array_to_string(
									ARRAY(
										SELECT categoria
										FROM UNNEST(string_to_array(meios_comunicacao, ',')) WITH ORDINALITY AS t(categoria, ordem_original)
										WHERE TRIM(categoria) IN (SELECT meio_comunicao_valido FROM cte_meios_comunicao_validos)
										ORDER BY ordem_original
									), ', '
						) AS meios_comunicacao

FROM cte_meios_comunicacao_transformado1
),

-- CTE para editar os erros de parser vindos da coleta do campo de em_caso_doenca_procura
-- \u00fa = ú, \u00e1 = á, \u00ed = í
cte_em_caso_doenca_procura_transformado1 AS (
SELECT linha_id, REPLACE(
						REPLACE(
							REPLACE(
								REPLACE(
									REPLACE(
										REPLACE(em_caso_doenca_procura, '\u00fa', 'ú'), 
									'\u00e1', 'á'), 
								'\u00ed', 'í'), 
							'[', ''), 
						']', ''), 
					'''', '') AS em_caso_doenca_procura
					
FROM cte_dados_ficha_com_id 
),

-- CTE com uma linha para cada local válido de procura para caso de doença 
cte_em_caso_doenca_procura_validos AS(
	SELECT UNNEST(ARRAY['Unidade de Saúde', 'Hospital Público', 'Rede Privada', 'Farmácia', 'Outros', 'Auxílio Espiritual']) AS em_caso_doenca_procura_valido
),

-- CTE para usar a coluna de local de procura para caso de doença sem erros de parser e remover apenas valores das listas que não sejam dessa categoria 
cte_em_caso_doenca_procura_transformado2 AS (
	SELECT linha_id, array_to_string(
									ARRAY(
										SELECT categoria
										FROM UNNEST(string_to_array(em_caso_doenca_procura, ',')) WITH ORDINALITY AS t(categoria, ordem_original)
										WHERE TRIM(categoria) IN (SELECT em_caso_doenca_procura_valido FROM cte_em_caso_doenca_procura_validos)
										ORDER BY ordem_original
									), ', '
						) AS em_caso_doenca_procura

FROM cte_em_caso_doenca_procura_transformado1
)

SELECT cte0.id_paciente, cte2.sexo, cte1.obito, cte0.bairro, cte0.raca_cor, cte0.ocupacao, cte3.religiao, cte1.luz_eletrica, cte0.data_cadastro, cte0.escolaridade, cte0.nacionalidade, cte4.renda_familiar,
	   cte0.data_nascimento, cte1.em_situacao_de_rua, cte0.frequenta_escola, cte7.meios_transporte, cte8.doencas_condicoes, cte5.identidade_genero, cte9.meios_comunicacao, cte0.orientacao_sexual, cte1.possui_plano_saude,
	   cte10.em_caso_doenca_procura, cte6.situacao_profissional, cte1.vulnerabilidade_social, cte0.data_atualizacao_cadastro, cte1.familia_beneficiaria_auxilio_brasil, cte1.crianca_matriculada_creche_pre_escola, 
	   cte0.altura, cte0.peso, cte0.pressao_sistolica, cte0.pressao_diastolica, cte0.n_atendimentos_atencao_primaria, cte0.n_atendimentos_hospital, cte0.updated_at, cte0.tipo

-- cte0 -> cte1 -> cte2 -> cte3 -> cte4 -> cte5 -> cte6 -> cte7 -> cte8 -> cte9 -> cte10
FROM cte_dados_sem_transformacao cte0
JOIN cte_bool_bin_transformado cte1 ON cte0.linha_id = cte1.linha_id
JOIN cte_sexo_transformado cte2 ON cte1.linha_id = cte2.linha_id
JOIN cte_religiao_transformado cte3 ON cte2.linha_id = cte3.linha_id
JOIN cte_renda_familiar_transformado cte4 ON cte3.linha_id = cte4.linha_id
JOIN cte_identidade_genero_transformado cte5 ON cte4.linha_id = cte5.linha_id
JOIN cte_situacao_profissional_transformado cte6 ON cte5.linha_id = cte6.linha_id
JOIN cte_meios_transporte_transformado cte7 ON cte6.linha_id = cte7.linha_id
JOIN cte_doencas_condicoes_transformado cte8 ON cte7.linha_id = cte8.linha_id
JOIN cte_meios_comunicacao_transformado2 cte9 ON cte8.linha_id = cte9.linha_id
JOIN cte_em_caso_doenca_procura_transformado2 cte10 ON cte9.linha_id = cte10.linha_id