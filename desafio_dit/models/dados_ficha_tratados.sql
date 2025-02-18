{{config(materialized='table') }}

WITH dados_ficha_tratados AS (
    SELECT UPPER(bairro) as BAIRRO, altura * 2 as altura_quadrado
    FROM public.dados_ficha
)

SELECT * FROM dados_ficha_tratados


-- dá pra criar vários with_as com colunas especificas tratadas e dps usar um select com cada grupo pra juntar!