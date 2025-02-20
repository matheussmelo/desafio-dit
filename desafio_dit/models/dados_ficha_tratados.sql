{{config(materialized='table') }}

-- Nesse script serão utilizadas Common Table Expressions (CTE's) para realizar a transformação dos dados de forma organizada

-- Nesse script serão utilizadas Common Table Expressions (CTE's) para realizar a transformação dos dados de forma organizada

-- CTE para criar a tabela original e adicionar um identificador incremental na primeira coluna (Será útil para unir todos os CTE's e geral a tabela transformada)
WITH cte_dados_ficha_com_id AS (
    SELECT ROW_NUMBER() OVER () AS linha_id, *
	
    FROM public.dados_ficha
),

-- CTE para filtrar os dados que não terão nenhuma transformação
cte_not_transformed_data AS (
	SELECT linha_id, id_paciente, bairro, raca_cor, ocupacao, data_cadastro, escolaridade, nacionalidade, data_nascimento, 
		   frequenta_escola, orientacao_sexual, data_atualizacao_cadastro, altura, peso, pressao_sistolica, 
		   pressao_diastolica, n_atendimentos_atencao_primaria, n_atendimentos_hospital, updated_at, tipo
		   
	FROM cte_dados_ficha_com_id
),

-- CTE para normalizar as colunas que possuem 0/1/True/False apenas para binário 0/1
cte_binary_only AS(
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
cte_sexo AS(
SELECT linha_id,

CASE WHEN sexo = 'male' THEN 'Masculino'
     WHEN sexo = 'female' THEN 'Feminino'
	 ELSE sexo
	 END AS sexo
	 
FROM cte_dados_ficha_com_id
),

-- CTE para transformar no campo de religião 'Não' para 'Sem religião' e valores que não agregam informação 
-- como 'Sim'/'ESB ALMIRANTE'/'10 EAP 01'/'Acomp. Cresc. e Desenv. da Criança'/'ORQUIDEA'/'Sem resposta'
cte_religiao AS(
SELECT linha_id, 

CASE WHEN religiao = 'Não' THEN 'Sem religião'
	 WHEN religiao IN ('Sim', 'ESB ALMIRANTE', '10 EAP 01', 'Acomp. Cresc. e Desenv. da Criança', 'ORQUIDEA') THEN 'Sem resposta'
	 ELSE religiao
	 END AS religiao
	 
FROM cte_dados_ficha_com_id
),

-- CTE para transformar no campo de renda familiar valores que não pertencem à essa categoria em 'Sem resposta' 
cte_renda_familiar AS(
SELECT linha_id, 

CASE WHEN renda_familiar IN ('Manhã', 'Internet') THEN 'Sem resposta'
	 ELSE renda_familiar
	 END AS renda_familiar

FROM cte_dados_ficha_com_id
),

-- CTE para transformar no campo de identidade de gênero valores nulo e que não pertencem à essa categoria em 'Sem resposta',
-- além de reduzir para apenas as respostas 'Cis', 'Trans' 'Travesti' e 'Outro'
cte_identidade_genero AS(
SELECT linha_id, 

CASE WHEN identidade_genero IN ('Bissexual', 'Homossexual (gay / lésbica)', 'Heterossexual', 'Não', 'Sim') 
						     OR identidade_genero IS NULL THEN 'Sem resposta'
	 WHEN identidade_genero IN ('Mulher transexual', 'Homem transexual') THEN 'Trans'
	 ELSE identidade_genero
	 END AS identidade_genero

FROM cte_dados_ficha_com_id
),

-- CTE para transformar no campo de situação profissional valores que não pertencem à essa categoria em 'Sem resposta' 
cte_situacao_profissional AS(
SELECT linha_id, 

CASE WHEN situacao_profissional IN ('SMS CAPS DIRCINHA E LINDA BATISTA AP 33', 'Médico Urologista') THEN 'Sem resposta'
	 ELSE situacao_profissional
	 END AS situacao_profissional

FROM cte_dados_ficha_com_id
),

-- CTE para editar os erros de parser vindos da coleta do campo de meios de transporte
-- \u00d4 = Ô, \u00f4 = ô, \u00e7 = ç, \u00e3 = ã, \u00ed = í
cte_meios_transporte AS(
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

-- CTE para editar os erros de parser vindos da coleta do campo de doencas_condicoes
-- \u00e3 = ã, \u00e1 = á, \u00ed = í, \u00f3 = ó, \u00e2 = â, '\u00e9' = 'é'
cte_doencas_condicoes AS (
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

-- CTE para editar os erros de parser vindos da coleta do campo de meios_comunicacao
-- \u00e3 = ã e \u00e1 = á 
cte_meios_comunicacao AS (
SELECT	linha_id, LTRIM(REPLACE(
							REPLACE(
								REPLACE(
									REPLACE(
										REPLACE(
											REPLACE(
												REPLACE(
													REPLACE(
														REPLACE(
															REPLACE(meios_comunicacao, 'Mais de 4 Salários Mínimos,', ''), 
														'4 Salários Mínimos,', ''), 
													'Manhã', ''), 
												'Grupos Religiosos', ''), 
											'3 Salários Mínimos', ''), 
										'\u00e3', 'ã'), 
									'\u00e1', 'á'), 
								'[', ''), 
							']', ''), 
						'''', '')) AS meios_comunicacao
									
FROM cte_dados_ficha_com_id 
),

-- CTE para editar os erros de parser vindos da coleta do campo de em_caso_doenca_procura
-- \u00fa = ú, \u00e1 = á, \u00ed = í,  
cte_em_caso_doenca_procura AS (
SELECT linha_id, REPLACE(
					REPLACE(
						REPLACE(
							REPLACE(
								REPLACE(
									REPLACE(
										REPLACE(em_caso_doenca_procura, '1 Salário Mínimo', ''),
										'\u00fa', 'ú'), 
									'\u00e1', 'á'), 
								'\u00ed', 'í'), 
							'[', ''), 
						']', ''), 
					'''', '') AS em_caso_doenca_procura
					
FROM cte_dados_ficha_com_id 
)

-- SELECT DISTINCT em_caso_doenca_procura FROM cte_em_caso_doenca_procura

SELECT cte0.id_paciente, cte2.sexo, cte1.obito, cte0.bairro, cte0.raca_cor, cte0.ocupacao, cte3.religiao, cte1.luz_eletrica, cte0.data_cadastro, cte0.escolaridade, cte0.nacionalidade, cte4.renda_familiar,
	   cte0.data_nascimento, cte1.em_situacao_de_rua, cte0.frequenta_escola, cte7.meios_transporte, cte8.doencas_condicoes, cte5.identidade_genero, cte9.meios_comunicacao, cte0.orientacao_sexual, cte1.possui_plano_saude,
	   cte10.em_caso_doenca_procura, cte6.situacao_profissional, cte1.vulnerabilidade_social, cte0.data_atualizacao_cadastro, cte1.familia_beneficiaria_auxilio_brasil, cte1.crianca_matriculada_creche_pre_escola, 
	   cte0.altura, cte0.peso, cte0.pressao_sistolica, cte0.pressao_diastolica, cte0.n_atendimentos_atencao_primaria, cte0.n_atendimentos_hospital, cte0.updated_at, cte0.tipo

-- cte0 -> cte1 -> cte2 -> cte3 -> cte4 -> cte5 -> cte6 -> cte7 -> cte8 -> cte9 -> cte10
FROM cte_not_transformed_data cte0
JOIN cte_binary_only cte1 ON cte0.linha_id = cte1.linha_id
JOIN cte_sexo cte2 ON cte1.linha_id = cte2.linha_id
JOIN cte_religiao cte3 ON cte2.linha_id = cte3.linha_id
JOIN cte_renda_familiar cte4 ON cte3.linha_id = cte4.linha_id
JOIN cte_identidade_genero cte5 ON cte4.linha_id = cte5.linha_id
JOIN cte_situacao_profissional cte6 ON cte5.linha_id = cte6.linha_id
JOIN cte_meios_transporte cte7 ON cte6.linha_id = cte7.linha_id
JOIN cte_doencas_condicoes cte8 ON cte7.linha_id = cte8.linha_id
JOIN cte_meios_comunicacao cte9 ON cte8.linha_id = cte9.linha_id
JOIN cte_em_caso_doenca_procura cte10 ON cte9.linha_id = cte10.linha_id