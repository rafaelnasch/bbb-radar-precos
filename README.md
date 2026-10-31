# Radar de Preços BBB

Site que mostra, todo dia, como os preços da BBB Banheiras Bom Banho se comparam com os dos concorrentes que publicam preço na internet. Feito para o vendedor consultar no celular: índice do dia, produtos com mais risco e mais folga, alertas, ficha de cada produto com os equivalentes e um roteiro de negociação montado por regras.

> **Atenção: o site é público.** Quem tiver o link vê tudo. O `robots.txt` e a meta `noindex` pedem aos buscadores que não indexem, mas isso não é proteção. Não coloque aqui custo, margem, preço de tabela interna ou nome de cliente. Para restringir o acesso, o caminho é GitHub Pages privado (plano Enterprise) ou outro hospedeiro com login.

## Como funciona

Todo dia às 6h (horário de Brasília) o GitHub Actions (`.github/workflows/diario.yml`):

1. remonta as tabelas a partir de `historico/` (últimos 30 dias);
2. roda a coleta (`motor/coleta_precos.py`, limite de 25 min) nos sites públicos;
3. roda o casamento de produtos (`motor/equivalencias.py`);
4. roda `motor/painel.sql` e `motor/alertas.sql` no DuckDB e grava os JSON do site (`scripts/gerar_site.py`);
5. guarda o dia em `historico/` e apaga o que tiver mais de 30 dias;
6. faz commit de `historico/` e `site/dados/` ("dados: coleta AAAA-MM-DD") e publica `site/` no GitHub Pages.

Se a coleta cair, o passo segue e o site sai com a última coleta disponível (o Actions mostra um aviso).

## Pastas

| Pasta | O que tem |
|---|---|
| `motor/` | Coletor, casamento e as duas consultas SQL (mesmos arquivos do notebook da Nekt, rodando no modo local com CSV). |
| `scripts/` | `historico.py` (remontar e salvar histórico) e `gerar_site.py` (JSON do site). |
| `historico/` | `precos_coleta`, `comparativo_diario` e `coleta_log` de cada dia, em CSV gzip. |
| `dados_base/` | `cadastro_concorrentes.csv` (1.164 empresas, só as colunas que o site mostra; as anotações de pesquisa ficam fora do repositório público) e `fatos_bbb.json` (fatos públicos e perguntas do bloco "Como negociar"). |
| `site/` | `index.html` (página única) e `dados/` (JSON gerados). |

## Rodar no computador

```bash
python3.12 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python scripts/historico.py remontar --tabelas tabelas
NEKT_LOCAL=1 NEKT_LOCAL_DIR=$PWD/tabelas COLETA_ORCAMENTO_S=1500 python motor/coleta_precos.py
NEKT_LOCAL=1 NEKT_LOCAL_DIR=$PWD/tabelas python motor/equivalencias.py
python scripts/gerar_site.py --tabelas tabelas
python -m http.server 8000 --directory site   # abrir http://localhost:8000
```

Para testar só o site sem coletar de novo, basta remontar o histórico e rodar `gerar_site.py`.

## Publicar pela primeira vez

- [ ] Criar o repositório no GitHub e subir este conteúdo.
- [ ] Em Settings > Pages, escolher **Source: GitHub Actions**.
- [ ] Em Settings > Actions > General, deixar **Workflow permissions: Read and write**.
- [ ] Em Actions, rodar **Coleta diária** à mão (Run workflow) e conferir o link que aparece no passo "Publicar no Pages".

## Como adicionar um concorrente

- [ ] Ache a plataforma da loja e um método que já existe em `ADAPTADORES` dentro de `motor/coleta_precos.py` (WooCommerce, Shopify, Tray, VTEX, Magento, Nuvemshop, sitemap com JSON-LD etc.).
- [ ] Acrescente um item na lista `CONCORRENTES` do mesmo arquivo: `fonte_id`, `empresa_slug` (minúsculas e hífen), `empresa`, `tipo`, `cidade_uf`, `relevancia`, `plataforma`, `metodo`, `canal`, `endpoint`, `cats`, `params`, `tem_preco_publico`, `ativo=True`, `observacao`.
- [ ] Teste só essa fonte: `COLETA_SO=<fonte_id> NEKT_LOCAL=1 NEKT_LOCAL_DIR=/tmp/teste python motor/coleta_precos.py` e confira preço, medidas, lugares, jatos e motores na saída, com status `ok`.
- [ ] Se a empresa for nova também no cadastro, acrescente uma linha em `dados_base/cadastro_concorrentes.csv`.
- [ ] Faça commit. A próxima coleta já inclui a fonte. Mudou o motor também no notebook da Nekt? Copie o arquivo para lá.

## Fatos da BBB e perguntas

`dados_base/fatos_bbb.json` alimenta o bloco "Como negociar". Só entra fato que a BBB publica, sempre com a fonte. Item com `"conferir": true` aparece no site com o aviso "conferir a fonte antes de usar". O bloco nunca sugere desconto nem valor.

## Limites

Preço de vitrine, sem frete. Mercado Livre fica fora (exige aplicativo autenticado). Sites que bloqueiam robô aparecem como fonte bloqueada. Lojas grandes são lidas em rodízio ao longo de alguns dias. O IP do GitHub Actions é de datacenter e alguns sites podem responder diferente do que respondem num computador comum: confira a aba "Fontes" depois das primeiras execuções.
