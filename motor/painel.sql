-- painel_produto | BBB Banheiras x concorrentes (BigQuery / Nekt)
-- Uma linha por produto BBB que tem par no índice hoje.
-- Variante padrão = menor tamanho (menor comprimento x largura; empate ou sem medida: menor preço).
-- Índice principal = preço BBB / mediana do preço ajustado dos pares exato+equivalente DA FAIXA DA BBB x 100
-- (faixa de mercado: entrada, intermediaria, premium; coluna faixa = faixa do concorrente, faixa_bbb = faixa da BBB no dia).
-- Também: índice contra cada faixa (indice_entrada, indice_intermediaria, indice_premium) e contra todas (indice_todas_faixas).
-- Posição (só índice principal): < 95 mais barata | 95 a 105 na média | > 105 mais cara | sem par na faixa da BBB.
-- Menor/mediana/maior preço, concorrente mais barato, variação 1d/7d e dias acima do mercado: só pares da faixa da BBB.
-- Histórico gravado antes da coluna faixa existir (faixa vazia) conta como intermediaria.
-- Fontes: equivalencias (dia atual), comparativo_diario (histórico), precos_coleta (medidas da BBB).
-- SQL portátil (roda também em DuckDB para teste): mediana calculada por posição, sem PERCENTILE_CONT.
WITH
hoje AS (
  SELECT MAX(CAST(data_coleta AS DATE)) AS d FROM `rafaelnasch_bbb_banheiras`.`equivalencias`
),
pares AS (
  SELECT e.*, COALESCE(e.bbb_variante, '') AS var_bbb,
         COALESCE(e.faixa, 'intermediaria') AS fx, COALESCE(e.faixa_bbb, 'intermediaria') AS fx_bbb
  FROM `rafaelnasch_bbb_banheiras`.`equivalencias` e, hoje
  WHERE CAST(e.data_coleta AS DATE) = hoje.d
    AND e.classe IN ('exato', 'equivalente')
    AND e.conc_preco_ajustado > 0
),
-- medidas da variante BBB (último dia, site da BBB; o 2º domínio só completa)
bbb_medidas AS (
  SELECT sku, var_bbb, area
  FROM (
    SELECT p.sku, COALESCE(p.variante, '') AS var_bbb,
           p.comprimento_m * p.largura_m AS area,
           ROW_NUMBER() OVER (PARTITION BY p.sku, COALESCE(p.variante, '')
                              ORDER BY CASE WHEN REPLACE(p.dominio, 'www.', '') = 'banheirasbombanho.com.br' THEN 0 ELSE 1 END,
                                       p.coletado_em DESC) AS rn
    FROM `rafaelnasch_bbb_banheiras`.`precos_coleta` p, hoje
    WHERE p.empresa_slug = 'bbb' AND p.canal = 'site' AND CAST(p.data_coleta AS DATE) = hoje.d
  ) x
  WHERE rn = 1
),
variantes AS (
  SELECT DISTINCT pa.bbb_sku, pa.var_bbb, pa.bbb_nome, pa.bbb_categoria, pa.bbb_preco, pa.url_bbb, m.area
  FROM pares pa
  LEFT JOIN bbb_medidas m ON m.sku = pa.bbb_sku AND m.var_bbb = pa.var_bbb
),
padrao AS (
  SELECT * FROM (
    SELECT v.*, ROW_NUMBER() OVER (PARTITION BY bbb_sku
                                   ORDER BY CASE WHEN area IS NULL THEN 1 ELSE 0 END, area, bbb_preco, var_bbb) AS rn
    FROM variantes v
  ) x WHERE rn = 1
),
-- grupo: 'principal' (faixa da BBB), cada faixa e 'todas'
pares_grupo AS (
  SELECT pa.*, 'principal' AS grupo FROM pares pa WHERE pa.fx = pa.fx_bbb
  UNION ALL SELECT pa.*, pa.fx AS grupo FROM pares pa
  UNION ALL SELECT pa.*, 'todas' AS grupo FROM pares pa
),
pares_padrao AS (
  SELECT pa.*,
         ROW_NUMBER() OVER (PARTITION BY pa.bbb_sku, pa.grupo ORDER BY pa.conc_preco_ajustado, pa.conc_slug, pa.conc_produto_id) AS pos,
         COUNT(*) OVER (PARTITION BY pa.bbb_sku, pa.grupo) AS n
  FROM pares_grupo pa
  JOIN padrao p ON p.bbb_sku = pa.bbb_sku AND p.var_bbb = pa.var_bbb
),
resumo_grupo AS (
  SELECT bbb_sku, grupo,
         MAX(fx_bbb) AS faixa_bbb,
         MAX(n) AS n_pares,
         MIN(conc_preco_ajustado) AS menor_preco_conc,
         AVG(CASE WHEN pos IN (FLOOR((n + 1) / 2.0), CEIL((n + 1) / 2.0))
                  THEN conc_preco_ajustado END) AS mediana_preco_conc,
         MAX(conc_preco_ajustado) AS maior_preco_conc
  FROM pares_padrao
  GROUP BY bbb_sku, grupo
),
resumo AS (
  SELECT bbb_sku,
         MAX(faixa_bbb) AS faixa_bbb,
         MAX(CASE WHEN grupo = 'principal' THEN n_pares END) AS n_pares,
         MAX(CASE WHEN grupo = 'principal' THEN menor_preco_conc END) AS menor_preco_conc,
         MAX(CASE WHEN grupo = 'principal' THEN mediana_preco_conc END) AS mediana_preco_conc,
         MAX(CASE WHEN grupo = 'principal' THEN maior_preco_conc END) AS maior_preco_conc,
         MAX(CASE WHEN grupo = 'entrada' THEN mediana_preco_conc END) AS med_entrada,
         MAX(CASE WHEN grupo = 'entrada' THEN n_pares END) AS n_entrada,
         MAX(CASE WHEN grupo = 'intermediaria' THEN mediana_preco_conc END) AS med_intermediaria,
         MAX(CASE WHEN grupo = 'intermediaria' THEN n_pares END) AS n_intermediaria,
         MAX(CASE WHEN grupo = 'premium' THEN mediana_preco_conc END) AS med_premium,
         MAX(CASE WHEN grupo = 'premium' THEN n_pares END) AS n_premium,
         MAX(CASE WHEN grupo = 'todas' THEN mediana_preco_conc END) AS med_todas,
         MAX(CASE WHEN grupo = 'todas' THEN n_pares END) AS n_todas
  FROM resumo_grupo
  GROUP BY bbb_sku
),
mais_barato AS (
  SELECT bbb_sku, conc_empresa, conc_slug, conc_nome, conc_variante, conc_preco, conc_preco_ajustado, url_conc
  FROM pares_padrao WHERE pos = 1 AND grupo = 'principal'
),
-- histórico: menor preço ajustado e índice por dia, na mesma variante padrão
hist_pares AS (
  SELECT CAST(c.data_coleta AS DATE) AS d, c.bbb_sku, c.bbb_preco, c.conc_preco_ajustado,
         ROW_NUMBER() OVER (PARTITION BY CAST(c.data_coleta AS DATE), c.bbb_sku ORDER BY c.conc_preco_ajustado) AS pos,
         COUNT(*) OVER (PARTITION BY CAST(c.data_coleta AS DATE), c.bbb_sku) AS n
  FROM `rafaelnasch_bbb_banheiras`.`comparativo_diario` c
  JOIN padrao p ON p.bbb_sku = c.bbb_sku AND p.var_bbb = COALESCE(c.bbb_variante, '')
  WHERE c.classe IN ('exato', 'equivalente') AND c.conc_preco_ajustado > 0
    AND COALESCE(c.faixa, 'intermediaria') = COALESCE(c.faixa_bbb, 'intermediaria')
),
hist_dia AS (
  SELECT d, bbb_sku,
         MIN(conc_preco_ajustado) AS menor,
         MAX(bbb_preco) / NULLIF(AVG(CASE WHEN pos IN (FLOOR((n + 1) / 2.0), CEIL((n + 1) / 2.0))
                                     THEN conc_preco_ajustado END), 0) * 100 AS indice
  FROM hist_pares
  GROUP BY d, bbb_sku
),
-- referência de 1 e 7 dias: a coleta mais recente até a data-alvo
ref_1d AS (
  SELECT h.bbb_sku, h.menor FROM (
    SELECT hd.*, ROW_NUMBER() OVER (PARTITION BY hd.bbb_sku ORDER BY hd.d DESC) AS rn
    FROM hist_dia hd, hoje WHERE hd.d <= DATE_ADD(hoje.d, INTERVAL (-1) DAY)
  ) h WHERE rn = 1
),
ref_7d AS (
  SELECT h.bbb_sku, h.menor FROM (
    SELECT hd.*, ROW_NUMBER() OVER (PARTITION BY hd.bbb_sku ORDER BY hd.d DESC) AS rn
    FROM hist_dia hd, hoje WHERE hd.d <= DATE_ADD(hoje.d, INTERVAL (-7) DAY)
  ) h WHERE rn = 1
),
-- dias seguidos acima do mercado (índice > 105), contados de hoje para trás; dia sem coleta do produto quebra a série
dias_coleta AS (
  SELECT DISTINCT CAST(data_coleta AS DATE) AS d FROM `rafaelnasch_bbb_banheiras`.`comparativo_diario`
),
grade AS (
  SELECT p.bbb_sku, dc.d,
         ROW_NUMBER() OVER (PARTITION BY p.bbb_sku ORDER BY dc.d DESC) AS k,
         CASE WHEN hd.indice > 105 THEN 1 ELSE 0 END AS acima
  FROM padrao p
  CROSS JOIN dias_coleta dc
  LEFT JOIN hist_dia hd ON hd.bbb_sku = p.bbb_sku AND hd.d = dc.d
),
quebra AS (
  SELECT bbb_sku, MIN(k) AS primeira_quebra FROM grade WHERE acima = 0 GROUP BY bbb_sku
),
seguidos AS (
  SELECT g.bbb_sku, COALESCE(q.primeira_quebra - 1, MAX(g.k)) AS dias
  FROM grade g LEFT JOIN quebra q ON q.bbb_sku = g.bbb_sku
  GROUP BY g.bbb_sku, q.primeira_quebra
)
SELECT
  hoje.d AS data_coleta,
  p.bbb_sku,
  p.bbb_nome,
  p.bbb_categoria,
  p.var_bbb AS bbb_variante_padrao,
  p.bbb_preco,
  r.faixa_bbb,
  COALESCE(r.n_pares, 0) AS n_pares,
  ROUND(r.menor_preco_conc, 2) AS menor_preco_conc,
  ROUND(r.mediana_preco_conc, 2) AS mediana_preco_conc,
  ROUND(r.maior_preco_conc, 2) AS maior_preco_conc,
  ROUND(p.bbb_preco / NULLIF(r.mediana_preco_conc, 0) * 100, 1) AS indice,
  CASE
    WHEN p.bbb_preco / NULLIF(r.mediana_preco_conc, 0) * 100 < 95 THEN 'mais barata'
    WHEN p.bbb_preco / NULLIF(r.mediana_preco_conc, 0) * 100 <= 105 THEN 'na média'
    WHEN r.mediana_preco_conc > 0 THEN 'mais cara'
    ELSE 'sem par na faixa da BBB'
  END AS posicao,
  ROUND(p.bbb_preco / NULLIF(r.med_entrada, 0) * 100, 1) AS indice_entrada,
  COALESCE(r.n_entrada, 0) AS n_pares_entrada,
  ROUND(p.bbb_preco / NULLIF(r.med_intermediaria, 0) * 100, 1) AS indice_intermediaria,
  COALESCE(r.n_intermediaria, 0) AS n_pares_intermediaria,
  ROUND(p.bbb_preco / NULLIF(r.med_premium, 0) * 100, 1) AS indice_premium,
  COALESCE(r.n_premium, 0) AS n_pares_premium,
  ROUND(p.bbb_preco / NULLIF(r.med_todas, 0) * 100, 1) AS indice_todas_faixas,
  COALESCE(r.n_todas, 0) AS n_pares_todas_faixas,
  mb.conc_empresa AS conc_mais_barato,
  mb.conc_nome AS conc_mais_barato_produto,
  mb.conc_variante AS conc_mais_barato_variante,
  mb.conc_preco AS conc_mais_barato_preco,
  mb.conc_preco_ajustado AS conc_mais_barato_preco_ajustado,
  mb.url_conc AS conc_mais_barato_url,
  ROUND((r.menor_preco_conc / NULLIF(r1.menor, 0) - 1) * 100, 1) AS var_menor_preco_1d_pct,
  ROUND((r.menor_preco_conc / NULLIF(r7.menor, 0) - 1) * 100, 1) AS var_menor_preco_7d_pct,
  COALESCE(s.dias, 0) AS dias_seguidos_acima_mercado,
  p.url_bbb
FROM padrao p
CROSS JOIN hoje
JOIN resumo r ON r.bbb_sku = p.bbb_sku
LEFT JOIN mais_barato mb ON mb.bbb_sku = p.bbb_sku
LEFT JOIN ref_1d r1 ON r1.bbb_sku = p.bbb_sku
LEFT JOIN ref_7d r7 ON r7.bbb_sku = p.bbb_sku
LEFT JOIN seguidos s ON s.bbb_sku = p.bbb_sku
ORDER BY indice DESC NULLS LAST, indice_todas_faixas DESC
