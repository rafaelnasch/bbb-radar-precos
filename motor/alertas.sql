-- alertas_dia | BBB Banheiras x concorrentes (BigQuery / Nekt)
-- Uma linha por alerta do dia da coleta mais recente. Tipos:
--   concorrente_baixou    concorrente baixou >= 5% num produto que está num par exato/equivalente (qualquer faixa;
--                         o detalhe diz a faixa do concorrente e se ela é a da BBB)
--   bbb_acima_mercado     variante BBB passou a ficar > 10% acima da mediana dos pares DA FAIXA DA BBB
--                         (índice principal > 110 hoje, <= 110 na coleta anterior)
--   produto_novo          produto de concorrente que não existia na coleta anterior da mesma empresa
--   produto_retirado      produto que existia na coleta anterior e sumiu hoje
--                         (novo/retirado só para empresa com coleta "ok" nos dois dias: rodízio e corte geram falso alarme)
--   preco_suspeito        linha de hoje com status_preco = 'suspeito' (faixa ou variação > 30%)
--   pix_igual_preco_por   fonte em que o preço "por" gravado é igual ao Pix (diferença <= R$ 0,01): o Pix anunciado
--                         virou o preço de cartão (erro de leitura, como a Aquasol em 31/10/2026) ou o Pix é suposto
--                         (marketplace sem Pix separado). Uma linha por fonte; alta quando >= 50% das linhas com Pix.
--   divergencia_sites_bbb mesmo SKU e variante com preço diferente (> 1%) em banheirasbombanho e bbbbanheiras
--   fonte_falhou          fonte com status 'erro' ou 'bloqueado' no coleta_log de hoje
-- SQL portátil (roda também em DuckDB para teste).
WITH
hoje AS (
  SELECT MAX(CAST(data_coleta AS DATE)) AS d FROM `rafaelnasch_bbb_banheiras`.`precos_coleta`
),
-- precos_coleta sem duplicata (rodar a coleta 2x no mesmo dia faz append): última leitura do dia
pc AS (
  SELECT * FROM (
    SELECT p.*, CAST(p.data_coleta AS DATE) AS d,
           REPLACE(COALESCE(p.dominio, ''), 'www.', '') AS dom,
           COALESCE(p.variante, '') AS var,
           COALESCE(NULLIF(p.produto_id, ''), p.url) AS pid,  -- produto sem id na loja: a URL é a chave
           ROW_NUMBER() OVER (PARTITION BY CAST(p.data_coleta AS DATE), p.empresa_slug, p.canal, p.dominio,
                                           COALESCE(NULLIF(p.produto_id, ''), p.url),
                                           COALESCE(p.variante, '')
                              ORDER BY p.coletado_em DESC) AS rn
    FROM `rafaelnasch_bbb_banheiras`.`precos_coleta` p
  ) x WHERE rn = 1
),
-- coleta anterior de cada empresa (a mais recente antes de hoje)
anterior AS (
  SELECT pc.empresa_slug, MAX(pc.d) AS d_ant
  FROM pc, hoje WHERE pc.d < hoje.d GROUP BY pc.empresa_slug
),
log_dia AS (
  SELECT CAST(data_coleta AS DATE) AS d, empresa_slug,
         SUM(CASE WHEN status = 'ok' THEN 0 ELSE 1 END) AS nao_ok
  FROM `rafaelnasch_bbb_banheiras`.`coleta_log`
  GROUP BY CAST(data_coleta AS DATE), empresa_slug
),
-- ---------------------------------------------------------------- 1) concorrente baixou >= 5%
pares_hoje AS (
  SELECT c.conc_slug, c.conc_empresa, c.conc_produto_id, COALESCE(c.conc_variante, '') AS conc_var,
         MIN(c.conc_nome) AS conc_nome, MIN(c.url_conc) AS url_conc,
         MIN(COALESCE(c.faixa, 'intermediaria')) AS faixa,
         MAX(CASE WHEN COALESCE(c.faixa, 'intermediaria') = COALESCE(c.faixa_bbb, 'intermediaria') THEN 1 ELSE 0 END) AS na_faixa_bbb,
         STRING_AGG(DISTINCT c.bbb_sku, ', ') AS skus_bbb
  FROM `rafaelnasch_bbb_banheiras`.`comparativo_diario` c, hoje
  WHERE CAST(c.data_coleta AS DATE) = hoje.d AND c.classe IN ('exato', 'equivalente')
  GROUP BY c.conc_slug, c.conc_empresa, c.conc_produto_id, COALESCE(c.conc_variante, '')
),
baixou AS (
  SELECT 'concorrente_baixou' AS tipo, 'alta' AS severidade,
         ph.conc_slug AS empresa_slug, ph.conc_empresa AS empresa, ph.skus_bbb AS bbb_sku,
         CONCAT(ph.conc_nome, CASE WHEN ph.conc_var <> '' THEN CONCAT(' (', ph.conc_var, ')') ELSE '' END) AS produto,
         CONCAT('preço caiu de ', CAST(ROUND(a.preco_por, 2) AS STRING), ' para ', CAST(ROUND(t.preco_por, 2) AS STRING),
                ' desde ', CAST(a.d AS STRING), '; faixa ', ph.faixa,
                CASE WHEN ph.na_faixa_bbb = 1 THEN ' (a da BBB)' ELSE ' (fora do índice principal)' END,
                '; pares BBB: ', ph.skus_bbb) AS detalhe,
         a.preco_por AS valor_anterior, t.preco_por AS valor_atual,
         ROUND((t.preco_por / a.preco_por - 1) * 100, 1) AS variacao_pct, ph.url_conc AS url
  FROM pares_hoje ph
  CROSS JOIN hoje
  JOIN pc t ON t.empresa_slug = ph.conc_slug AND t.pid = COALESCE(NULLIF(ph.conc_produto_id, ''), ph.url_conc) AND t.var = ph.conc_var AND t.d = hoje.d
  JOIN anterior an ON an.empresa_slug = ph.conc_slug
  JOIN pc a ON a.empresa_slug = t.empresa_slug AND a.dom = t.dom AND a.pid = t.pid AND a.var = t.var AND a.d = an.d_ant
  WHERE a.preco_por > 0 AND t.preco_por > 0 AND t.preco_por <= a.preco_por * 0.95
    AND a.status_preco = 'ok' AND t.status_preco = 'ok'
),
-- ---------------------------------------------------------------- 2) BBB passou a > 10% acima da mediana
idx_pares AS (
  SELECT CAST(c.data_coleta AS DATE) AS d, c.bbb_sku, COALESCE(c.bbb_variante, '') AS var, c.bbb_nome, c.bbb_preco,
         c.conc_preco_ajustado, c.url_bbb,
         ROW_NUMBER() OVER (PARTITION BY CAST(c.data_coleta AS DATE), c.bbb_sku, COALESCE(c.bbb_variante, '')
                            ORDER BY c.conc_preco_ajustado) AS pos,
         COUNT(*) OVER (PARTITION BY CAST(c.data_coleta AS DATE), c.bbb_sku, COALESCE(c.bbb_variante, '')) AS n
  FROM `rafaelnasch_bbb_banheiras`.`comparativo_diario` c
  WHERE c.classe IN ('exato', 'equivalente') AND c.conc_preco_ajustado > 0
    AND COALESCE(c.faixa, 'intermediaria') = COALESCE(c.faixa_bbb, 'intermediaria')
),
idx_dia AS (
  SELECT d, bbb_sku, var, MIN(bbb_nome) AS bbb_nome, MIN(url_bbb) AS url_bbb, MAX(n) AS n_pares,
         MAX(bbb_preco) AS bbb_preco,
         AVG(CASE WHEN pos IN (FLOOR((n + 1) / 2.0), CEIL((n + 1) / 2.0)) THEN conc_preco_ajustado END) AS mediana
  FROM idx_pares GROUP BY d, bbb_sku, var
),
idx_seq AS (
  SELECT i.*, i.bbb_preco / NULLIF(i.mediana, 0) * 100 AS indice,
         ROW_NUMBER() OVER (PARTITION BY i.bbb_sku, i.var ORDER BY i.d DESC) AS k
  FROM idx_dia i, hoje WHERE i.d <= hoje.d
),
acima AS (
  SELECT 'bbb_acima_mercado' AS tipo, 'alta' AS severidade,
         'bbb' AS empresa_slug, 'BBB' AS empresa, h.bbb_sku AS bbb_sku,
         CONCAT(h.bbb_nome, CASE WHEN h.var <> '' THEN CONCAT(' (', h.var, ')') ELSE '' END) AS produto,
         CONCAT('índice foi de ', CAST(ROUND(a.indice, 1) AS STRING), ' para ', CAST(ROUND(h.indice, 1) AS STRING),
                ' (mediana de ', CAST(h.n_pares AS STRING), ' pares da faixa da BBB: ', CAST(ROUND(h.mediana, 2) AS STRING), ')') AS detalhe,
         ROUND(a.indice, 1) AS valor_anterior, ROUND(h.indice, 1) AS valor_atual,
         ROUND(h.indice - a.indice, 1) AS variacao_pct, h.url_bbb AS url
  FROM idx_seq h
  CROSS JOIN hoje
  JOIN idx_seq a ON a.bbb_sku = h.bbb_sku AND a.var = h.var AND a.k = 2
  WHERE h.k = 1 AND h.d = hoje.d AND h.indice > 110 AND a.indice <= 110
),
-- ---------------------------------------------------------------- 3) produto novo ou retirado
escopo AS (
  SELECT pc.* FROM pc
  WHERE pc.empresa_slug <> 'bbb' AND pc.categoria IN ('banheira', 'spa', 'ofuro', 'infantil')
),
prod_hoje AS (
  SELECT e.empresa_slug, MIN(e.empresa) AS empresa, e.pid AS produto_id, MIN(e.nome) AS nome, MIN(e.url) AS url,
         MIN(e.preco_por) AS preco_por
  FROM escopo e, hoje WHERE e.d = hoje.d
  GROUP BY e.empresa_slug, e.pid
),
prod_ant AS (
  SELECT e.empresa_slug, MIN(e.empresa) AS empresa, e.pid AS produto_id, MIN(e.nome) AS nome, MIN(e.url) AS url,
         MIN(e.preco_por) AS preco_por
  FROM escopo e JOIN anterior an ON an.empresa_slug = e.empresa_slug AND e.d = an.d_ant
  GROUP BY e.empresa_slug, e.pid
),
empresas_estaveis AS (
  SELECT an.empresa_slug
  FROM anterior an
  CROSS JOIN hoje
  JOIN log_dia lh ON lh.empresa_slug = an.empresa_slug AND lh.d = hoje.d AND lh.nao_ok = 0
  JOIN log_dia la ON la.empresa_slug = an.empresa_slug AND la.d = an.d_ant AND la.nao_ok = 0
),
novos AS (
  SELECT 'produto_novo' AS tipo, 'media' AS severidade, h.empresa_slug, h.empresa, CAST(NULL AS STRING) AS bbb_sku,
         h.nome AS produto, 'produto que não estava na coleta anterior' AS detalhe,
         NULL AS valor_anterior, h.preco_por AS valor_atual, NULL AS variacao_pct, h.url
  FROM prod_hoje h
  JOIN empresas_estaveis s ON s.empresa_slug = h.empresa_slug
  LEFT JOIN prod_ant a ON a.empresa_slug = h.empresa_slug AND a.produto_id = h.produto_id
  WHERE a.produto_id IS NULL
),
retirados AS (
  SELECT 'produto_retirado' AS tipo, 'media' AS severidade, a.empresa_slug, a.empresa, CAST(NULL AS STRING) AS bbb_sku,
         a.nome AS produto, 'produto da coleta anterior que não apareceu hoje' AS detalhe,
         a.preco_por AS valor_anterior, NULL AS valor_atual, NULL AS variacao_pct, a.url
  FROM prod_ant a
  JOIN empresas_estaveis s ON s.empresa_slug = a.empresa_slug
  LEFT JOIN prod_hoje h ON h.empresa_slug = a.empresa_slug AND h.produto_id = a.produto_id
  WHERE h.produto_id IS NULL
),
-- ---------------------------------------------------------------- 4) preço suspeito
suspeitos AS (
  SELECT 'preco_suspeito' AS tipo, 'media' AS severidade, pc.empresa_slug, pc.empresa,
         CASE WHEN pc.empresa_slug = 'bbb' THEN pc.sku END AS bbb_sku,
         CONCAT(pc.nome, CASE WHEN pc.var <> '' THEN CONCAT(' (', pc.var, ')') ELSE '' END) AS produto,
         COALESCE(pc.motivo_status, 'suspeito') AS detalhe,
         NULL AS valor_anterior, pc.preco_por AS valor_atual, NULL AS variacao_pct, pc.url
  FROM pc, hoje WHERE pc.d = hoje.d AND pc.status_preco = 'suspeito'
),
-- ---------------------------------------------------------------- 4b) preço por igual ao Pix anunciado
pix_fonte AS (
  SELECT pc.empresa_slug, MIN(pc.empresa) AS empresa, pc.dom,
         SUM(CASE WHEN pc.preco_pix > 0 THEN 1 ELSE 0 END) AS com_pix,
         SUM(CASE WHEN pc.preco_pix > 0 AND pc.preco_por > 0 AND ABS(pc.preco_por - pc.preco_pix) <= 0.01 THEN 1 ELSE 0 END) AS iguais,
         MIN(CASE WHEN pc.preco_pix > 0 AND pc.preco_por > 0 AND ABS(pc.preco_por - pc.preco_pix) <= 0.01 THEN pc.url END) AS url_exemplo
  FROM pc, hoje WHERE pc.d = hoje.d
  GROUP BY pc.empresa_slug, pc.dom
),
pix_igual AS (
  SELECT 'pix_igual_preco_por' AS tipo,
         CASE WHEN f.iguais * 2 >= f.com_pix THEN 'alta' ELSE 'media' END AS severidade,
         f.empresa_slug, f.empresa, CAST(NULL AS STRING) AS bbb_sku,
         CONCAT('fonte ', f.dom) AS produto,
         CONCAT(CAST(f.iguais AS STRING), ' de ', CAST(f.com_pix AS STRING),
                ' linhas com Pix têm preço por igual ao Pix (valor_anterior = linhas com Pix, valor_atual = iguais): conferir se o por gravado é o Pix ou se o Pix é suposto') AS detalhe,
         f.com_pix * 1.0 AS valor_anterior, f.iguais * 1.0 AS valor_atual,
         NULL AS variacao_pct, f.url_exemplo AS url
  FROM pix_fonte f WHERE f.iguais > 0
),
-- ---------------------------------------------------------------- 5) divergência entre os 2 sites da BBB
bbb_sites AS (
  SELECT pc.sku, pc.dom, pc.nome, pc.url, pc.preco_por,
         LOWER(REPLACE(REPLACE(LOWER(pc.var), 'tamanho:', ''), ' ', '')) AS var_n
  FROM pc, hoje
  WHERE pc.d = hoje.d AND pc.empresa_slug = 'bbb' AND pc.canal = 'site' AND pc.status_preco = 'ok'
    AND pc.dom IN ('banheirasbombanho.com.br', 'bbbbanheiras.com.br')
),
divergencia AS (
  SELECT 'divergencia_sites_bbb' AS tipo, 'media' AS severidade, 'bbb' AS empresa_slug, 'BBB' AS empresa, a.sku AS bbb_sku,
         CONCAT(a.nome, CASE WHEN a.var_n <> '' THEN CONCAT(' (', a.var_n, ')') ELSE '' END) AS produto,
         CONCAT('banheirasbombanho ', CAST(ROUND(a.preco_por, 2) AS STRING), ' x bbbbanheiras ',
                CAST(ROUND(b.preco_por, 2) AS STRING), ' (', b.url, ')') AS detalhe,
         a.preco_por AS valor_anterior, b.preco_por AS valor_atual,
         ROUND((b.preco_por / a.preco_por - 1) * 100, 1) AS variacao_pct, a.url
  FROM bbb_sites a
  JOIN bbb_sites b ON b.sku = a.sku AND b.var_n = a.var_n
  WHERE a.dom = 'banheirasbombanho.com.br' AND b.dom = 'bbbbanheiras.com.br'
    AND ABS(b.preco_por / a.preco_por - 1) > 0.01
),
-- ---------------------------------------------------------------- 6) fonte com erro ou bloqueio
falhas AS (
  SELECT 'fonte_falhou' AS tipo, CASE WHEN l.status = 'erro' THEN 'alta' ELSE 'baixa' END AS severidade,
         l.empresa_slug, COALESCE(MIN(c.empresa), l.empresa_slug) AS empresa, CAST(NULL AS STRING) AS bbb_sku,
         CONCAT('fonte ', l.metodo) AS produto, CONCAT(l.status, ': ', COALESCE(l.erro, '')) AS detalhe,
         NULL AS valor_anterior, NULL AS valor_atual, NULL AS variacao_pct,
         MIN(c.endpoint) AS url
  FROM `rafaelnasch_bbb_banheiras`.`coleta_log` l
  CROSS JOIN hoje
  LEFT JOIN `rafaelnasch_bbb_banheiras`.`concorrentes` c ON c.empresa_slug = l.empresa_slug AND c.metodo = l.metodo
  WHERE CAST(l.data_coleta AS DATE) = hoje.d AND l.status IN ('erro', 'bloqueado')
  GROUP BY l.empresa_slug, l.metodo, l.status, l.erro
),
todos AS (
  SELECT * FROM baixou
  UNION ALL SELECT * FROM acima
  UNION ALL SELECT * FROM novos
  UNION ALL SELECT * FROM retirados
  UNION ALL SELECT * FROM suspeitos
  UNION ALL SELECT * FROM pix_igual
  UNION ALL SELECT * FROM divergencia
  UNION ALL SELECT * FROM falhas
)
SELECT hoje.d AS data_coleta, t.*
FROM todos t CROSS JOIN hoje
ORDER BY CASE t.severidade WHEN 'alta' THEN 0 WHEN 'media' THEN 1 ELSE 2 END, t.tipo, t.empresa_slug, t.produto
