# Monitor de preços BBB x concorrentes (Nekt)

Coleta diária dos preços públicos da BBB e dos concorrentes, casamento de produtos equivalentes e painel com alertas. Tudo grava na camada `bbb_banheiras` da Nekt (no BigQuery: dataset `rafaelnasch_bbb_banheiras`).

## Arquivos

| Arquivo | O que faz |
|---|---|
| `coleta_precos.py` | Notebook 1. Lê os sites (lista `CONCORRENTES`), grava `precos_coleta` (append), `coleta_log` (append) e `concorrentes` (overwrite). Limite de 23 min. |
| `equivalencias.py` | Notebook 2. Lê `precos_coleta` e `revisao_pares`, casa cada variante da BBB com o equivalente de cada concorrente e grava `equivalencias` (overwrite), `comparativo_diario` (histórico; rodar de novo no mesmo dia substitui o dia) e `precos_suspeitos` (overwrite). |
| `painel.sql` | Consulta `painel_produto`: uma linha por produto BBB, com o índice principal (só concorrentes da faixa da BBB), o índice contra cada faixa (entrada, intermediaria, premium) e contra todas, posição, concorrente mais barato da faixa, variação de 1 e 7 dias e dias seguidos acima do mercado. |
| `alertas.sql` | Consulta `alertas_dia`: concorrente baixou ≥ 5% (diz a faixa), BBB passou a > 10% acima da mediana da faixa dela, produto novo ou retirado, preço suspeito, preço "por" igual ao Pix (uma linha por fonte), divergência entre os 2 sites da BBB, fonte com erro ou bloqueio. |
| `test_extracao.py` | 41 testes da leitura de texto do coletor (jatos, opcionais, lugares, Pix etc.) com páginas salvas em `test_fixtures/`. |
| `test_equivalencias.py` | Confere o casamento contra os pares conferidos à mão. `--v2` usa a coleta real (`PRECOS_V2=<csv>`) e o `testes_pares_v2.csv` (45 pares); `TESTES_PARES=testes_pares.csv` volta aos 25 originais. |
| `test_sql.py` | Testa as duas consultas no DuckDB com um histórico sintético de 9 dias montado a partir da coleta real: `python test_sql.py <pasta_da_coleta>`. Confere o painel por faixa contra pandas e os alertas plantados (inclusive Pix igual ao preço). |
| `converter_legado.py` | Só local: converte a coleta antiga para o formato novo (usado pelo teste sem `--v2`). |

## Ordem de execução (diária)

1. `coleta_precos.py` (cerca de 17 min).
2. `equivalencias.py` (segundos). Só depois que a coleta terminar.
3. `painel.sql` e `alertas.sql` como transformações/consultas sobre as tabelas acima.

Teste local: `NEKT_LOCAL=1 NEKT_LOCAL_DIR=<pasta>` troca o SDK da Nekt por CSV na pasta.

## Tabelas

| Tabela | Modo | Grão |
|---|---|---|
| `precos_coleta` | append | produto x variante x dia (contrato completo no cabeçalho de `coleta_precos.py`; coluna extra `fonte_id` separa os sites da mesma empresa) |
| `coleta_log` | append | fonte x dia (coluna extra `fonte_id` e `requisicoes`) |
| `concorrentes` | overwrite | fonte cadastrada |
| `equivalencias` | overwrite | par BBB x concorrente do dia (exato, equivalente, referência); `faixa` = faixa do concorrente, `faixa_bbb` = faixa da BBB no dia |
| `comparativo_diario` | histórico | só pares exato e equivalente, um bloco por dia |
| `precos_suspeitos` | overwrite | linhas suspeitas do dia (faixa de preço ou variação > 30%) |
| `revisao_pares` | overwrite (manual) | decisão humana sobre um par |

Base de preço da BBB: `banheirasbombanho.com.br`; o `bbbbanheiras.com.br` só completa SKU que falta no primeiro. A loja de Campinas e o MadeiraMadeira são coletados mas ficam fora do índice.

## Como adicionar um concorrente

- [ ] Ache a plataforma da loja e um método que já existe em `ADAPTADORES` (Woo, Shopify, Tray, VTEX, Magento, Nuvemshop, sitemap com JSON-LD etc.).
- [ ] Acrescente um item na lista `CONCORRENTES` de `coleta_precos.py`: `fonte_id`, `empresa_slug` (minúsculas e hífen), `empresa`, `tipo`, `cidade_uf`, `relevancia`, `plataforma`, `metodo`, `canal` (`site` ou `marketplace:<nome>`), `endpoint`, `cats`, `params`, `tem_preco_publico`, `ativo=True`, `observacao`.
- [ ] Teste só essa fonte: `COLETA_SO=<fonte_id> NEKT_LOCAL=1 NEKT_LOCAL_DIR=<pasta_teste> python coleta_precos.py`.
- [ ] Confira na saída: preço, medidas, lugares, jatos e motores preenchidos; status `ok` no log.
- [ ] Publique o arquivo no notebook da Nekt.

## Como revisar pares

- [ ] Abra `equivalencias` do dia e escolha o par (`bbb_sku`, `conc_slug`, `conc_produto_id`).
- [ ] Acrescente uma linha no CSV de revisão com as colunas `bbb_sku, conc_slug, conc_produto_id, decisao, motivo, revisado_por, revisado_em`. `decisao` é `aprovado` ou `rejeitado`.
- [ ] Suba o CSV inteiro como tabela `revisao_pares` (overwrite) na camada `bbb_banheiras`.
- [ ] No dia seguinte (ou rodando `equivalencias.py` de novo): `rejeitado` nunca mais vira par; `aprovado` vira `exato` se passar nos filtros duros (categoria, hidro, lugares, medida até ±20%/±25%).

## Faixa de mercado

Cada empresa ganha uma faixa (entrada, intermediaria ou premium) pela razão entre o preço típico dela e a mediana do mercado no mesmo grupo (categoria x lugares). Marca premium (Jacuzzi, Doka/Immersi, Victoria+Albert, My Place/Axell) é premium direto. O **índice principal** compara a BBB só com a faixa dela; o painel mostra também o índice contra cada faixa. Histórico gravado antes da coluna existir conta como intermediaria.

## Regras que mudaram nesta versão

- Faixa de mercado (acima). Colunas `faixa` e `faixa_bbb` em `equivalencias` e `comparativo_diario`. O painel e o alerta "BBB acima do mercado" usam só a faixa da BBB.
- Alerta novo `pix_igual_preco_por`: fonte em que o preço "por" gravado é igual ao Pix. Pega o erro da Aquasol (Pix gravado como preço de cartão) se ele voltar, e marca marketplace com Pix suposto (hoje: MadeiraMadeira da BBB, 26 de 27 linhas).
- Coletor: leitura de jatos, opcionais, lugares e Pix corrigida conforme a conferência de 31/10/2026 (`test_extracao.py`).

- Jatos: o par só cai para "referência" quando a razão de jatos é 2 ou mais (antes, 1,6). Mesma régua dos motores. Motivo: o par Karisma x Blumenau (7 x 4 jatos, mesmo motor) foi aceito na conferência manual.
- Corte de 3 pares por concorrente por variante BBB continua. O teste mede a classificação sem o corte e diz se o par ficou no índice final.

## Rodada de referência (31/10/2026, modo local, pasta `local_tables_v3`)

Fonte: `local_tables_v3/coleta_log.csv`, `precos_coleta.csv`, `equivalencias.csv` e `resumo_equivalencias.json`, coleta de 31/10/2026 (17,7 min).

- 9.212 linhas, 66 fontes: 57 ok, 4 parciais por rodízio ou corte (Elevato, Personalizze, Aquasol, Brasil Banheiras), 5 bloqueadas pelo robots.txt (Hidro Forte, Deca, Atacadão da Hidro, Ouro Fino Shop, Leroy Merlin).
- Pares: 1.157 exato, 5.486 equivalente, 29.447 referência.
- BBB na faixa intermediaria. Índice principal 132,0 (banheira 135,6 · spa 119,4 · ofurô 128,4), 253 produtos, 2.892 pares. Contra entrada 148,6; contra premium 56,6; todas as faixas 129,4.
- 290 produtos BBB na base; 21 sem nenhum par exato ou equivalente; 37 sem par na faixa da BBB.

## Pendências de decisão

- [ ] **Teste 107 (Mônaco 1,34 x Canto Búzios, Cariri):** o coletor agora lê "acomoda confortavelmente duas pessoas" e a BBB declara 3 lugares para o mesmo tamanho. A regra de lugares derruba o par, mas a conferência manual aceitou. Decidir: manter a regra (e mudar o esperado do teste) ou aceitar diferença de 1 lugar quando a medida bate em ±5%.
- [ ] **Hidro Fiber:** preço do configurador difere da loja em alguns produtos (ex.: Ofurô Duplo Estrelar). Decidir qual preço vale.
- [ ] **Top de risco:** conferir à mão os pares do topo antes de usar (ex.: Karisma x "Sul de Minas individual" da Minas Banheiras aparece como o mais barato).
