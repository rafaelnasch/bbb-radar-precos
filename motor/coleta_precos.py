# -*- coding: utf-8 -*-
"""
Coletor diário de preços: BBB Banheiras Bom Banho e concorrentes com preço público.

Roda no notebook "python-base" da Nekt (Python 3, pandas + requests; sem navegador).
Lê/grava tabelas na camada "bbb_banheiras" pelo SDK da Nekt:
  entradas : precos_coleta (histórico, para variação dia a dia)
  saídas   : precos_coleta (append), coleta_log (append), concorrentes (overwrite)

Modo local (teste): NEKT_LOCAL=1 troca o SDK por um stub que lê/grava CSV em local_tables/.

Regras de coleta educada: User-Agent de navegador, pausa por domínio (respeita Crawl-delay),
robots.txt respeitado (grupo "*"), sem login, sem contornar bloqueio. Fonte que bloqueia
vira status "bloqueado" no coleta_log e a coleta segue.

Colunas extras além do contrato: fonte_id (identifica o canal da mesma empresa, ex.: os
dois sites da BBB) em precos_coleta, coleta_log e concorrentes; canal e dominio em concorrentes.
"""
import os
import re
import io
import json
import time
import gzip
import html
import math
import hashlib
import datetime
import threading
import traceback
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse, urlencode, quote

import socket
import requests
import pandas as pd
import urllib3.util.connection as _u3conn

# Força IPv4: sem rota IPv6, cada conexão esperava o timeout do endereço IPv6 antes de tentar o IPv4.
if os.environ.get("COLETA_IPV6") != "1":
    _u3conn.allowed_gai_family = lambda: socket.AF_INET

LOCAL = os.environ.get("NEKT_LOCAL") == "1"
LAYER = "bbb_banheiras"
LOCAL_DIR = os.environ.get(
    "NEKT_LOCAL_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "local_tables"),
)

if LOCAL:
    class _NektLocalStub(object):
        """Mesmas funções do SDK da Nekt, lendo e gravando CSV em LOCAL_DIR/<tabela>.csv."""

        def load_table(self, layer_name, table_name):
            p = os.path.join(LOCAL_DIR, table_name + ".csv")
            if not os.path.exists(p):
                raise FileNotFoundError(p)
            return pd.read_csv(p, dtype=str, keep_default_na=False, na_values=[""])

        def save_table(self, df, layer_name, table_name, mode="append"):
            os.makedirs(LOCAL_DIR, exist_ok=True)
            p = os.path.join(LOCAL_DIR, table_name + ".csv")
            if mode == "append" and os.path.exists(p):
                old = pd.read_csv(p, dtype=str, keep_default_na=False, na_values=[""])
                df = pd.concat([old, df.astype(object)], ignore_index=True)
            df.to_csv(p, index=False)
            return p

    nekt = _NektLocalStub()
else:
    import nekt  # SDK disponível no notebook da Nekt

# ---------------------------------------------------------------------------
# Parâmetros gerais
# ---------------------------------------------------------------------------
TZ_SP = datetime.timezone(datetime.timedelta(hours=-3))  # America/Sao_Paulo (sem horário de verão desde 2019)
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")
ORCAMENTO_TOTAL_S = int(os.environ.get("COLETA_ORCAMENTO_S", str(23 * 60)))  # limite duro < 25 min
MAX_WORKERS = int(os.environ.get("COLETA_WORKERS", "32"))
SO_FONTES = [s for s in os.environ.get("COLETA_SO", "").split(",") if s]  # teste: só estas fonte_id
CHECAR_VARIACAO = True  # marca "suspeito" se o preço variar > 30% contra a última coleta
CATS_PRINCIPAIS = ("banheira", "spa", "ofuro")
VARIANTE_HIST = {}  # (fonte_id, sku) -> texto da variante na última coleta (Vnda: listagem usa o mesmo rótulo do detalhe)

# ---------------------------------------------------------------------------
# CONCORRENTES (fonte_id único por canal; empresa_slug agrupa a empresa)
#   metodo: adaptador usado; endpoint: URL base do método; params: ajustes do adaptador
#   cats: categorias mantidas (None = todas menos "outro")
# ---------------------------------------------------------------------------
TODAS = ["banheira", "spa", "ofuro", "sauna", "infantil", "pet", "acessorio"]
SO_PRINCIPAIS = ["banheira", "spa", "ofuro", "sauna", "infantil", "pet"]

CONCORRENTES = [
    # ---------------- BBB (canais próprios) ----------------
    dict(fonte_id="bbb-bombanho", empresa_slug="bbb", empresa="BBB", tipo="fabricante (cliente)", cidade_uf="Congonhal-MG",
         relevancia="referencia", plataforma="Magento 2", metodo="magento2_graphql", canal="site",
         endpoint="https://www.banheirasbombanho.com.br/graphql", cats=TODAS, params=dict(garantia_padrao=2.0), tem_preco_publico=True, ativo=True,
         observacao="Domínio de referência de preço da BBB (mix maior, sitemap ok). Opções de tamanho viram variantes; opcionais pagos em atributos_json."),
    dict(fonte_id="bbb-bbbbanheiras", empresa_slug="bbb", empresa="BBB", tipo="fabricante (cliente)", cidade_uf="Congonhal-MG",
         relevancia="referencia", plataforma="Magento 2", metodo="magento2_graphql", canal="site",
         endpoint="https://www.bbbbanheiras.com.br/graphql", cats=TODAS, params=dict(garantia_padrao=2.0), tem_preco_publico=True, ativo=True,
         observacao="Segundo domínio da BBB, cadastro separado (preço, SKU e mix divergem do bombanho)."),
    dict(fonte_id="bbb-madeiramadeira", empresa_slug="bbb", empresa="BBB", tipo="fabricante (cliente)", cidade_uf="Congonhal-MG",
         relevancia="referencia", plataforma="MadeiraMadeira (Next.js + Algolia)", metodo="madeiramadeira_seller",
         canal="marketplace:madeiramadeira", endpoint="https://www.madeiramadeira.com.br/lojista/bbb-banheiras-e-spa",
         cats=TODAS, params=dict(garantia_padrao=2.0), tem_preco_publico=True, ativo=True,
         observacao="Vitrine do lojista BBB BANHEIRAS E SPA (CNPJ 34.098.618/0001-58). preco_pix = preço à vista do marketplace."),
    dict(fonte_id="bbb-campinas", empresa_slug="bbb", empresa="BBB", tipo="loja BBB Campinas", cidade_uf="Campinas-SP",
         relevancia="referencia", plataforma="Tray", metodo="tray_web_api", canal="site",
         endpoint="https://www.bbbcampinas.com.br", cats=TODAS, params=dict(garantia_padrao=2.0), tem_preco_publico=True, ativo=True,
         observacao="bbbcampinas.com.br (BBB Shopping Dom Pedro). Se é loja própria ou licenciada não foi verificado; tabela própria."),
    dict(fonte_id="bbb-mercadolivre", empresa_slug="bbb", empresa="BBB", tipo="fabricante (cliente)", cidade_uf="Congonhal-MG",
         relevancia="referencia", plataforma="Mercado Livre", metodo="mercadolivre_api", canal="marketplace:mercadolivre",
         endpoint="https://api.mercadolibre.com/sites/MLB/search?seller_id=<id>", cats=TODAS, tem_preco_publico=True, ativo=False,
         observacao="DESATIVADO: busca exige app autenticado (OAuth) do Mercado Livre; HTML pede login. Ativar com app da BBB."),

    # ---------------- Fabricantes / lojas com preço público ----------------
    dict(fonte_id="minas-banheiras", empresa_slug="minas-banheiras", empresa="Minas Banheiras", tipo="fabricante", cidade_uf="Pouso Alegre-MG",
         relevancia="alta", plataforma="WooCommerce", metodo="woo_store_api", canal="site",
         endpoint="https://www.minasbanheiras.com.br", cats=TODAS, tem_preco_publico=True, ativo=True, params=dict(pix_pct_html=True),
         observacao="Vizinha da fábrica BBB (~30 km). Pix: % da loja lido na página de um produto e aplicado ao preço (atributos_json.pix_origem)."),
    dict(fonte_id="so-banheiras", empresa_slug="so-banheiras", empresa="Só Banheiras", tipo="fabricante", cidade_uf="Pouso Alegre-MG",
         relevancia="alta", plataforma="Magento 1.9", metodo="magento1_html", canal="site",
         endpoint="https://www.sobanheiras.com.br/media/downloads/sitemap.xml", cats=TODAS, tem_preco_publico=True, ativo=True,
         params=dict(delay=1.2), observacao="Grupo Lineaplás. robots bloqueia URLs com '?': só páginas .html do sitemap."),
    dict(fonte_id="lineaplas", empresa_slug="lineaplas", empresa="Lineaplas", tipo="fabricante", cidade_uf="Pouso Alegre-MG",
         relevancia="alta", plataforma="WooCommerce", metodo="woo_store_api", canal="site",
         endpoint="https://www.lineaplas.com.br", cats=TODAS, tem_preco_publico=True, ativo=True,
         observacao="Páginas HTML atrás de Cloudflare (403); Store API responde."),
    dict(fonte_id="fabrica-das-banheiras", empresa_slug="fabrica-das-banheiras", empresa="Fábrica das Banheiras", tipo="fabricante",
         cidade_uf=None, relevancia="alta", plataforma="Shopify", metodo="shopify_products_json", canal="site",
         endpoint="https://www.fabricadasbanheiras.com.br", cats=TODAS, tem_preco_publico=True, ativo=True,
         observacao="Selo 'até 10% OFF no Pix' sem valor exato: preco_pix fica vazio."),
    dict(fonte_id="splaz-banheiras", empresa_slug="splaz-banheiras", empresa="Splaz Banheiras", tipo="fabricante", cidade_uf=None,
         relevancia="alta", plataforma="Shopify", metodo="shopify_products_json", canal="site",
         endpoint="https://www.splazbanheiras.com.br", cats=TODAS, tem_preco_publico=True, ativo=True, params=dict(pix_pct_html=True),
         observacao="Responde 429 se martelar: 1 retry após espera. Pix e parcelas: regra da loja lida numa página de produto."),
    dict(fonte_id="fabrica-de-spas", empresa_slug="fabrica-de-spas", empresa="Fábrica de Spas / Fábrica de Banheiras", tipo="fabricante",
         cidade_uf="São Paulo-SP", relevancia="media", plataforma="WooCommerce", metodo="woo_store_api", canal="site",
         endpoint="https://fabricadespas.com.br", cats=TODAS, tem_preco_publico=True, ativo=True, observacao=None),
    dict(fonte_id="hidro-fiber-do-brasil", empresa_slug="hidro-fiber-do-brasil", empresa="Hidro Fiber do Brasil", tipo="fabricante",
         cidade_uf=None, relevancia="media", plataforma="WooCommerce", metodo="woo_store_api", canal="site",
         endpoint="https://hidrofiberdobrasil.com.br", cats=TODAS, tem_preco_publico=True, ativo=True, params=dict(config_html=True),
         observacao="Descrição traz várias configurações; o preço é da 'Configuração selecionada' da página (lida no HTML)."),
    dict(fonte_id="hidromundi-banheiras", empresa_slug="hidromundi-banheiras", empresa="Hidromundi Banheiras", tipo="fabricante",
         cidade_uf=None, relevancia="media", plataforma="Tray", metodo="tray_web_api", canal="site",
         endpoint="https://www.hidromundibanheiras.com.br", cats=TODAS, tem_preco_publico=True, ativo=True,
         observacao="Preço base sem adicionais (aquecedor etc. em atributos_json.opcionais)."),
    dict(fonte_id="aqually-banheiras", empresa_slug="aqually-banheiras", empresa="Aqually Banheiras", tipo="fabricante", cidade_uf=None,
         relevancia="media", plataforma="Tray", metodo="tray_html_datalayer", canal="site",
         endpoint="https://www.aquallybanheiras.com.br/loja/arquivos/1018229/sitemaps/sitemap_1.xml", cats=TODAS,
         tem_preco_publico=True, ativo=True,
         params=dict(url_regex=r"/linha-[a-z]+/.+", pagamento_tray=True,
                     forma_por_url={"/linha-canto/": "Canto", "/linha-redonda/": "Redonda", "/linha-retangular/": "Retangular"}),
         observacao="Variantes = kits de configuração (listSku do dataLayer). Parcelas pela aba 'Formas de Pagamento' "
                    "(maior N sem tarifa). Forma pela linha da URL (canto, redonda, retangular); Linha Dupla/Especial "
                    "não dizem a forma."),
    dict(fonte_id="aquaplas", empresa_slug="aquaplas", empresa="Aquaplas", tipo="fabricante", cidade_uf="Campinas-SP",
         relevancia="media", plataforma="Tray", metodo="tray_web_api", canal="site",
         endpoint="https://www.aquaplas.com.br", cats=SO_PRINCIPAIS, tem_preco_publico=True, ativo=True,
         params=dict(itens_serie_html=True, pagamento_tray=True),
         observacao="Grupo Stamplas; linhas Basic/Standard/Premium como variações. Jatos, motor e itens inclusos pela "
                    "tabela 'Itens de Série' da página (coluna da versão); parcelas pela aba 'Formas de Pagamento' "
                    "(hoje 2x-10x com juros: parcelas_sem_juros vazio)."),
    dict(fonte_id="show-banho", empresa_slug="show-banho", empresa="Show Banho", tipo="loja regional / revenda multimarcas",
         cidade_uf="Curitiba-PR", relevancia="media", plataforma="Tray", metodo="tray_web_api", canal="site",
         endpoint="https://www.showbanho.com.br", cats=SO_PRINCIPAIS, tem_preco_publico=True, ativo=True, observacao=None),
    dict(fonte_id="doka-home-spa", empresa_slug="doka-bath-works-immersi", empresa="Doka Bath Works / Immersi", tipo="importador/fabricante",
         cidade_uf="Navegantes-SC", relevancia="baixa", plataforma="Tray", metodo="tray_web_api", canal="site",
         endpoint="https://homespa.dokabathworks.com.br", cats=SO_PRINCIPAIS, tem_preco_publico=True, ativo=True,
         params=dict(so_nome=True, nome_regex=r"banheir|spa\b|ofur|sauna|hidro"),
         observacao="Loja Doka Home Spa (premium). dokabathworks.com.br bloqueia robôs no robots: não usado."),
    dict(fonte_id="jp-banheiras", empresa_slug="jp-banheiras", empresa="JP Banheiras", tipo="fabricante", cidade_uf=None,
         relevancia="media", plataforma="Nuvemshop", metodo="nuvemshop_sitemap_variants", canal="site",
         endpoint="https://dcdn-us.mitiendanube.com/stores/001/542/515/themes/common/sitemap.xml.gz", cats=TODAS,
         tem_preco_publico=True, ativo=True, params=dict(delay=1.5), observacao="Pix publicado por variante (5% no Pix/boleto)."),
    dict(fonte_id="hidrogas-piscinas", empresa_slug="hidrogas-piscinas", empresa="Hidrogás Piscinas", tipo="loja regional",
         cidade_uf="São José dos Campos-SP", relevancia="media", plataforma="Nuvemshop", metodo="nuvemshop_sitemap_variants", canal="site",
         endpoint="https://acdn-us.mitiendanube.com/stores/003/109/242/themes/common/sitemap.xml.gz", cats=SO_PRINCIPAIS,
         tem_preco_publico=False, ativo=True, params=dict(delay=1.5, url_regex=r"banheir|spa|ofur|sauna"),
         observacao="Hoje 'Sob Consulta' (sem_preco). Mantida para alertar quando publicar preço; praça da loja BBB de Taubaté/SJC."),
    dict(fonte_id="brasil-banheiras", empresa_slug="brasil-banheiras", empresa="Brasil Banheiras", tipo="fabricante", cidade_uf=None,
         relevancia="alta", plataforma="Vnda", metodo="vnda_data_variant", canal="site",
         endpoint="https://www.brasilbanheiras.com.br", cats=TODAS, tem_preco_publico=True, ativo=True,
         params=dict(listagens=["/banheiras", "/spas", "/ofuros", "/acessorios", "/lancamentos"], max_detalhes=85),
         observacao="robots: Crawl-delay 10 s. Listagens trazem o preço da variante padrão de todos; variantes completas em rodízio diário (log informa o corte)."),
    dict(fonte_id="banheiras-acqualine", empresa_slug="banheiras-acqualine", empresa="Banheiras Acqualine", tipo="fabricante", cidade_uf=None,
         relevancia="media", plataforma="WooCommerce (Store API desativada)", metodo="sitemap_jsonld", canal="site",
         endpoint="https://banheirasacqualine.com.br/product-sitemap.xml", cats=TODAS, tem_preco_publico=True, ativo=True,
         params=dict(url_regex=r"/produto/"), observacao="JSON-LD Product + data-product_variations (WooCommerce)."),
    dict(fonte_id="cariri-banheiras", empresa_slug="cariri-banheiras", empresa="Cariri Banheiras", tipo="fabricante/loja",
         cidade_uf="Juazeiro do Norte-CE", relevancia="baixa", plataforma="WooCommerce", metodo="woo_store_api", canal="site",
         endpoint="https://cariribanheiras.com.br", cats=TODAS, tem_preco_publico=True, ativo=True, observacao=None),
    dict(fonte_id="wt-fibras", empresa_slug="wt-fibras", empresa="WT Fibras Spas e Banheiras", tipo="fabricante", cidade_uf="São Paulo-SP",
         relevancia="media", plataforma="WooCommerce", metodo="woo_store_api", canal="site",
         endpoint="https://loja.wtfibras.com.br", cats=TODAS, tem_preco_publico=True, ativo=True,
         params=dict(ficha_html=True, garantia_padrao=3.0),
         observacao="Medidas e lugares no bloco de ícones da página (Dimensões/Acomodações), lido por produto. "
                    "Garantia: '3 anos de garantia' na faixa do site (regra da loja)."),
    dict(fonte_id="rc-banheiras", empresa_slug="rc-banheiras", empresa="RC Banheiras", tipo="fabricante", cidade_uf=None,
         relevancia="alta", plataforma="WooCommerce", metodo="woo_store_api", canal="site",
         endpoint="https://rcbanheiras.com.br", cats=TODAS, tem_preco_publico=True, ativo=True,
         observacao="Lidera busca no ML; inclui aquecedor e cromoterapia no preço em vários modelos."),
    dict(fonte_id="aurum-banheiras-e-spa", empresa_slug="aurum-banheiras-e-spa", empresa="Aurum Banheiras e Spa", tipo="fabricante",
         cidade_uf="Cascavel-PR", relevancia="media", plataforma="WooCommerce", metodo="woo_store_api", canal="site",
         endpoint="https://aurumbanheirasespa.com.br", cats=TODAS, tem_preco_publico=True, ativo=True,
         params=dict(lugares_por_travesseiro=True),
         observacao="Mesmo catálogo da Gold. Lugares: nome, imagem ('8 PESSOAS') ou nº de travesseiros do que acompanha."),
    dict(fonte_id="gold-banheiras", empresa_slug="gold-banheiras", empresa="Gold Banheiras", tipo="fabricante", cidade_uf="Cascavel-PR",
         relevancia="media", plataforma="WooCommerce", metodo="woo_store_api", canal="site",
         endpoint="https://goldbanheiras.com.br", cats=TODAS, tem_preco_publico=True, ativo=True,
         params=dict(lugares_por_travesseiro=True),
         observacao="Lugares: nome, imagem ('SPA ISLA ... 8 PESSOAS') ou nº de travesseiros do que acompanha."),
    dict(fonte_id="amazon-spa", empresa_slug="amazon-spa", empresa="Amazon Spa (rede)", tipo="loja regional / marca própria",
         cidade_uf="Salvador-BA (rede BA/PR/SP)", relevancia="media", plataforma="WooCommerce", metodo="woo_store_api", canal="site",
         endpoint="https://amazonspasalvador.com.br", cats=TODAS, tem_preco_publico=True, ativo=True,
         observacao="Mesmo catálogo nas unidades; Salvador é a referência."),
    dict(fonte_id="amazon-spa-ribeirao", empresa_slug="amazon-spa", empresa="Amazon Spa (rede)", tipo="loja regional / marca própria",
         cidade_uf="Ribeirão Preto-SP", relevancia="baixa", plataforma="WooCommerce", metodo="woo_store_api", canal="site",
         endpoint="https://amazonsparibeirao.com.br", cats=TODAS, tem_preco_publico=True, ativo=True,
         observacao="Unidade Ribeirão Preto (preço pode diferir da rede)."),
    dict(fonte_id="casa-do-spa", empresa_slug="casa-do-spa", empresa="Casa do Spa", tipo="revenda multimarca (Jacuzzi)",
         cidade_uf="Curitiba-PR", relevancia="media", plataforma="WooCommerce (Store API antiga)", metodo="woo_store_api", canal="site",
         endpoint="https://casadospashop.com.br", cats=SO_PRINCIPAIS, tem_preco_publico=True, ativo=True,
         params=dict(so_nome=True, api_path="/wp-json/wc/store/products", minor_unit=0),
         observacao="Store API antiga: valor em reais inteiros (sem centavos), conferido com a página (J220 L = R$ 77.335). Preço = 'à vista por'. Cartão 10x no HTML (não coletado)."),
    dict(fonte_id="versatil-banheiras", empresa_slug="versatil-banheiras", empresa="Versátil Banheiras", tipo="fabricante",
         cidade_uf="Senador Canedo-GO", relevancia="alta", plataforma="WordPress (CPT produtos)", metodo="wp_rest_html", canal="site",
         endpoint="https://versatilbanheiras.com.br/wp-json/wp/v2/produtos", cats=TODAS, tem_preco_publico=True, ativo=True,
         params=dict(delay=1.0),
         observacao="Praça de Goiânia (showroom BBB). Preço 'à vista' vai em atributos_json; preco_por = total no cartão. Inclui instalação em parte dos modelos."),
    dict(fonte_id="veneza-piscinas", empresa_slug="veneza-piscinas", empresa="Veneza Piscinas", tipo="loja de piscina (revenda Albacete)",
         cidade_uf="Recife-PE", relevancia="baixa", plataforma="WooCommerce antigo", metodo="sitemap_jsonld", canal="site",
         endpoint="https://loja.venezapiscinas.com.br/product-sitemap.xml", cats=SO_PRINCIPAIS, tem_preco_publico=True, ativo=True,
         params=dict(url_regex=r"/produto/"), observacao=None),
    dict(fonte_id="cooper-fibra-banheiras", empresa_slug="cooper-fibra-banheiras", empresa="Cooper Fibra Banheiras", tipo="fabricante",
         cidade_uf="São Paulo-SP", relevancia="baixa", plataforma="OpenCart", metodo="opencart_html", canal="site",
         endpoint="https://www.cooperfibrabanheiras.com.br", cats=TODAS, tem_preco_publico=True, ativo=True,
         observacao="robots bloqueia ?limit/&page/&sort: só categoria e produto."),
    dict(fonte_id="personalizze-banheiras", empresa_slug="personalizze-banheiras", empresa="Personalizze Banheiras", tipo="revenda multimarcas online",
         cidade_uf="Santana de Parnaíba-SP", relevancia="media", plataforma="Cliquemania", metodo="cliquemania_lista", canal="site",
         endpoint="https://www.personalizzebanheiras.com.br/listadeprodutos.php", cats=SO_PRINCIPAIS,
         params=dict(so_nome=True, detalhe_itens=True, max_detalhes=220, delay=1.0), tem_preco_publico=True, ativo=True,
         observacao="Lista única com preço por; Pix, formato, pessoas e opcionais pagos lidos na página do item (rodízio diário)."),
    dict(fonte_id="floripa-norte-spas", empresa_slug="floripa-norte-spas", empresa="Floripa Norte Spas e Banheiras", tipo="loja regional",
         cidade_uf="Florianópolis-SC", relevancia="baixa", plataforma="Hostinger Website Builder", metodo="hostinger_store_api", canal="site",
         endpoint="https://api-ecommerce.hostinger.com/store/store_01K008DY4NCTDZSTHJ6ZKYR3XR/products", cats=SO_PRINCIPAIS,
         tem_preco_publico=True, ativo=True, params=dict(site="https://floripanortebanheiras.com"), observacao=None),
    dict(fonte_id="telhanorte", empresa_slug="telhanorte", empresa="Telhanorte", tipo="varejo (home center)", cidade_uf="São Paulo-SP",
         relevancia="media", plataforma="VTEX", metodo="vtex_catalog", canal="site", endpoint="https://www.telhanorte.com.br",
         cats=SO_PRINCIPAIS, tem_preco_publico=True, ativo=True,
         params=dict(so_nome=True, caminhos=["/api/catalog_system/pub/products/search/loucas/banheiras"]),
         observacao="Categoria inteira de banheiras (inclui Ouro Fino; marca em atributos_json.marca)."),
    dict(fonte_id="acal-home-center", empresa_slug="acal-home-center", empresa="Acal Home Center", tipo="varejo (home center)",
         cidade_uf="Fortaleza-CE", relevancia="baixa", plataforma="VTEX", metodo="vtex_catalog", canal="site",
         endpoint="https://www.acalhomecenter.com.br", cats=SO_PRINCIPAIS, tem_preco_publico=True, ativo=True,
         params=dict(so_nome=True, caminhos=["/api/catalog_system/pub/products/search/banheiro/banheiras?sc=1",
                               "/api/catalog_system/pub/products/search?ft=jacuzzi&sc=1",
                               "/api/catalog_system/pub/products/search?ft=hidromassagem&sc=1"]), observacao=None),
    dict(fonte_id="faiberplas-casas-da-agua", empresa_slug="faiberplas", empresa="Faiberplas (via Casas da Água)", tipo="fabricante via varejo",
         cidade_uf=None, relevancia="baixa", plataforma="VTEX", metodo="vtex_catalog", canal="site", endpoint="https://www.casasdaagua.com.br",
         cats=SO_PRINCIPAIS, tem_preco_publico=True, ativo=True,
         params=dict(so_nome=True, caminhos=["/api/catalog_system/pub/products/search?fq=B:209"]), observacao=None),
    dict(fonte_id="balaroti", empresa_slug="balaroti", empresa="Balaroti", tipo="varejo (home center)", cidade_uf="Curitiba-PR",
         relevancia="baixa", plataforma="Wake Commerce", metodo="sitemap_jsonld", canal="site",
         endpoint="https://www.balaroti.com.br/banheiro/banheira-spa-e-hidro-vertical", cats=SO_PRINCIPAIS, tem_preco_publico=True,
         ativo=True, params=dict(so_nome=True, listagem=True, url_regex=r"/produto/"), observacao="Lista pela categoria (JSON-LD ItemList)."),
    # ---------- incluídos na varredura de 31/10/2026 (tarde) ----------
    dict(fonte_id="amazon-spa-joinville", empresa_slug="amazon-spa", empresa="Amazon Spa (rede)", tipo="loja regional / marca própria",
         cidade_uf="Joinville-SC", relevancia="baixa", plataforma="WooCommerce", metodo="woo_store_api", canal="site",
         endpoint="https://amazonspajoinville.com.br", cats=TODAS, tem_preco_publico=True, ativo=True,
         observacao="Unidade Joinville (preço pode diferir da rede)."),
    dict(fonte_id="banheiras-larocha", empresa_slug="banheiras-larocha", empresa="Banheiras Larocha", tipo="fabricante/loja",
         cidade_uf=None, relevancia="media", plataforma="WooCommerce", metodo="woo_store_api", canal="site",
         endpoint="https://banheiraslarocha.com.br", cats=TODAS, tem_preco_publico=True, ativo=True,
         observacao="Parte das banheiras com preço 0 (sob consulta) vira sem_preco."),
    dict(fonte_id="lojao-da-hidro", empresa_slug="lojao-da-hidro", empresa="Lojão da Hidro", tipo="loja",
         cidade_uf=None, relevancia="baixa", plataforma="WooCommerce", metodo="woo_store_api", canal="site",
         endpoint="https://lojaodahidro.com.br", cats=SO_PRINCIPAIS, params=dict(so_nome=True), tem_preco_publico=True, ativo=True,
         observacao="Mesmo grupo da Larocha (links cruzados)."),
    dict(fonte_id="so-lojistas", empresa_slug="so-lojistas", empresa="Só Lojistas", tipo="atacado/revenda", cidade_uf=None,
         relevancia="baixa", plataforma="WooCommerce", metodo="woo_store_api", canal="site", endpoint="https://solojistas.com.br",
         cats=SO_PRINCIPAIS, params=dict(so_nome=True), tem_preco_publico=True, ativo=True, observacao=None),
    dict(fonte_id="multiforma-ofuros", empresa_slug="multiforma-ofuros", empresa="Multiforma Ofurôs", tipo="fabricante",
         cidade_uf=None, relevancia="baixa", plataforma="WooCommerce (modo catálogo)", metodo="woo_store_api", canal="site",
         endpoint="https://multiforma.com.br", cats=TODAS, tem_preco_publico=True, ativo=True,
         observacao="Modo catálogo: preço 0 vira sem_preco; mantida para alertar quando publicar preço."),
    dict(fonte_id="health-saunas", empresa_slug="health-saunas", empresa="Health Saunas", tipo="fabricante (saunas)",
         cidade_uf=None, relevancia="baixa", plataforma="WooCommerce", metodo="woo_store_api", canal="site",
         endpoint="https://health-saunas.com.br", cats=TODAS, tem_preco_publico=True, ativo=True, observacao="Concorre só com as saunas BBB."),
    dict(fonte_id="pet-profissional-atacama", empresa_slug="pet-profissional-atacama", empresa="Pet Profissional (Atacama)",
         tipo="revenda (banheiras pet)", cidade_uf=None, relevancia="baixa", plataforma="WooCommerce", metodo="woo_store_api",
         canal="site", endpoint="https://petprofissional.com.br", cats=["pet"],
         params=dict(so_nome=True, nome_regex=r"banheir", categoria_fixa="pet"), tem_preco_publico=True, ativo=True, observacao="Só banheiras pet (Vitoriana Pets)."),
    dict(fonte_id="kiva-wellness-brazil", empresa_slug="kiva-wellness-brazil", empresa="Kiva Wellness Brazil", tipo="importador",
         cidade_uf=None, relevancia="baixa", plataforma="Shopify", metodo="shopify_products_json", canal="site",
         endpoint="https://kivawellness.com.br", cats=TODAS, tem_preco_publico=True, ativo=True, observacao=None),
    dict(fonte_id="jcn-piscinas", empresa_slug="jcn-piscinas", empresa="JCN Piscinas", tipo="loja de piscina", cidade_uf="Salto-SP",
         relevancia="baixa", plataforma="Tray", metodo="tray_web_api", canal="site", endpoint="https://www.jcnpiscinas.com.br",
         cats=SO_PRINCIPAIS, params=dict(so_nome=True, nome_regex=r"banheir|spa\b|ofur|sauna|hidro"), tem_preco_publico=True,
         ativo=True, observacao=None),
    dict(fonte_id="globalmar-piscinas", empresa_slug="globalmar-piscinas", empresa="Globalmar Piscinas", tipo="loja de piscina",
         cidade_uf="São Paulo-SP", relevancia="baixa", plataforma="Tray", metodo="tray_web_api", canal="site",
         endpoint="https://www.globalmar.com.br", cats=SO_PRINCIPAIS,
         params=dict(so_nome=True, nome_regex=r"banheir|spa\b|ofur|sauna|hidromass"), tem_preco_publico=True, ativo=True,
         observacao="Catálogo de ~1.250 itens; só nomes de banheira/spa/ofurô/sauna."),
    dict(fonte_id="clilmax", empresa_slug="clilmax", empresa="Clilmax", tipo="fabricante (freestanding/vitoriana)", cidade_uf="SP",
         relevancia="baixa", plataforma="Tray", metodo="tray_web_api", canal="site", endpoint="https://www.clilmax.com.br",
         cats=SO_PRINCIPAIS, params=dict(so_nome=True, nome_regex=r"banheir|spa\b|ofur|hidro"), tem_preco_publico=True, ativo=True,
         observacao=None),
    dict(fonte_id="globaltech-brasil", empresa_slug="globaltech-brasil", empresa="Globaltech Brasil", tipo="revenda (saunas)",
         cidade_uf="Atibaia-SP", relevancia="baixa", plataforma="Tray", metodo="tray_html_datalayer", canal="site",
         endpoint="https://www.globaltechbrasil.com/loja/arquivos/424007/sitemaps/sitemap_1.xml", cats=["sauna", "spa", "banheira", "ofuro"],
         params=dict(url_regex=r"sauna|banheir|ofur|/spa", max_paginas=200), tem_preco_publico=True, ativo=True,
         observacao="Catálogo de 3,4 mil itens; só URLs de sauna/spa/banheira pelo sitemap."),
    dict(fonte_id="quero-quero", empresa_slug="quero-quero", empresa="Lojas Quero-Quero", tipo="varejo (home center)", cidade_uf="RS/SC/PR",
         relevancia="baixa", plataforma="VTEX", metodo="vtex_catalog", canal="site", endpoint="https://www.queroquero.com.br",
         cats=SO_PRINCIPAIS, params=dict(so_nome=True, caminhos=["/api/catalog_system/pub/products/search/construcao/banheiro/banheiras-e-acessorios",
                                                                  "/api/catalog_system/pub/products/search?ft=sauna"]),
         tem_preco_publico=True, ativo=True, observacao=None),
    dict(fonte_id="abc-da-construcao", empresa_slug="abc-da-construcao", empresa="ABC da Construção", tipo="varejo (materiais de construção)",
         cidade_uf="rede (MG/DF/BA e outros)", relevancia="baixa", plataforma="Wake Commerce", metodo="sitemap_jsonld", canal="site",
         endpoint="https://www.abcdaconstrucao.com.br/banheiras-spa", cats=SO_PRINCIPAIS,
         params=dict(so_nome=True, listagem=True, paginacao="pagina", url_regex=r"/produto/", max_paginas=320,
                     listagens=["https://www.abcdaconstrucao.com.br/banheiras-spa/%s?tamanho=96" % x
                                for x in ("acrilica", "imersao", "spa/spa", "ofuro", "fibra/gel-coat")]),
         tem_preco_publico=True, ativo=True, observacao="Revende Ouro Fino, Jacuzzi, Immersi, Astra etc. (marca em atributos_json)."),
    dict(fonte_id="casa-mattos", empresa_slug="casa-mattos", empresa="Casa Mattos", tipo="varejo (home center regional)",
         cidade_uf="Zona da Mata-MG", relevancia="baixa", plataforma="Wake Commerce", metodo="sitemap_jsonld", canal="site",
         endpoint="https://www.casamattos.com.br/banheiro/banheiras-e-spas", cats=SO_PRINCIPAIS,
         params=dict(so_nome=True, listagem=True, paginacao="pagina", url_regex=r"/produto/"), tem_preco_publico=True, ativo=True,
         observacao=None),
    dict(fonte_id="construmarques", empresa_slug="construmarques", empresa="Construmarques", tipo="varejo (home center)",
         cidade_uf="Jaú-SP", relevancia="baixa", plataforma="Wake Commerce", metodo="sitemap_jsonld", canal="site",
         endpoint="https://www.construmarques.com.br/banheiro-e-lavabo/banheiras-e-spas", cats=SO_PRINCIPAIS,
         params=dict(so_nome=True, listagem=True, paginacao="pagina", url_regex=r"/produto/"), tem_preco_publico=True, ativo=True,
         observacao=None),
    dict(fonte_id="design-fino-banheiras", empresa_slug="design-fino-banheiras", empresa="Design Fino Banheiras", tipo="fabricante/revenda",
         cidade_uf="Guarulhos-SP", relevancia="media", plataforma="Loja Virtual (lojavirtual.com.br)", metodo="sitemap_jsonld",
         canal="site", endpoint="https://www.designfinobanheiras.com.br/sitemap.xml", cats=TODAS,
         params=dict(url_regex=r"/p$|/prod-\d+"), tem_preco_publico=True, ativo=True, observacao="Pix publicado só no texto (não coletado)."),
    dict(fonte_id="merito-comercial", empresa_slug="merito-comercial", empresa="Mérito Comercial", tipo="varejo multimarcas online",
         cidade_uf=None, relevancia="baixa", plataforma="Linx Commerce", metodo="sitemap_jsonld", canal="site",
         endpoint="https://www.meritocomercial.com.br/banheiras-spas", cats=SO_PRINCIPAIS,
         params=dict(so_nome=True, listagem=True, paginacao="pg", url_regex=r"-p\d{5,}"), tem_preco_publico=True, ativo=True,
         observacao="JSON-LD offers.price = preço à vista."),
    dict(fonte_id="lojas-aquasol", empresa_slug="aquasol", empresa="Aquasol Banheiras e Piscinas", tipo="fabricante e varejo online",
         cidade_uf="Tubarão-SC", relevancia="media", plataforma="Climba Commerce", metodo="sitemap_jsonld", canal="site",
         endpoint="https://feeds.climbacommerce.com.br/public/aquasol/sitemap/sitemap.xml.gz", cats=SO_PRINCIPAIS,
         params=dict(so_nome=True, url_regex=r"banheir|/spa|-spa-|ofur|sauna", delay=2.0, max_paginas=400, rodizio=True,
                     jsonld_preco_e_pix=True),
         tem_preco_publico=True, ativo=True,
         observacao="WAF Azion bloqueia rajada: 1 req/2 s. ~970 URLs: rodízio diário de 400 páginas (catálogo completo a cada 3 dias)."),
    dict(fonte_id="mac-metais", empresa_slug="mac-metais", empresa="Mac Metais", tipo="loja (vitorianas vintage)", cidade_uf=None,
         relevancia="baixa", plataforma="Nuvemshop", metodo="nuvemshop_sitemap_variants", canal="site",
         endpoint="https://www.macmetais.com.br/sitemap.xml.gz", cats=SO_PRINCIPAIS, params=dict(url_regex=r"/produtos/banheira"),
         tem_preco_publico=True, ativo=True, observacao=None),
    dict(fonte_id="o-magnata-banheira-vitoriana", empresa_slug="o-magnata-banheira-vitoriana", empresa="O Magnata Banheira Vitoriana",
         tipo="fabricante/loja", cidade_uf=None, relevancia="media", plataforma="OpenCart", metodo="opencart_html", canal="site",
         endpoint="https://www.omagnatabanheiravitoriana.com.br", cats=TODAS,
         params=dict(sitemap="https://www.omagnatabanheiravitoriana.com.br/index.php?route=feed/google_sitemap", delay=1.5),
         tem_preco_publico=True, ativo=True, observacao=None),
    dict(fonte_id="pretty-jet", empresa_slug="pretty-jet", empresa="Pretty Jet", tipo="fabricante", cidade_uf=None,
         relevancia="media", plataforma="OpenCart (loja.prettyjet.com.br)", metodo="opencart_html", canal="site",
         endpoint="https://loja.prettyjet.com.br", cats=TODAS,
         params=dict(sitemap="https://loja.prettyjet.com.br/index.php?route=feed/google_sitemap", delay=1.5),
         tem_preco_publico=True, ativo=True, observacao="Site institucional sem preço; preço na loja OpenCart."),
    dict(fonte_id="evolucao-pet", empresa_slug="evolucao-pet", empresa="Evolução Pet", tipo="loja (equipamento veterinário)",
         cidade_uf="São Paulo-SP", relevancia="baixa", plataforma="PrestaShop", metodo="sitemap_jsonld", canal="site",
         endpoint="https://www.evolucaopet.com.br/sitemap_shop_1.xml", cats=["pet"],
         params=dict(url_regex=r"/banheiras/", categoria_fixa="pet"), tem_preco_publico=True, ativo=True,
         observacao="Concorre só com as banheiras pet BBB."),
    dict(fonte_id="hidro-forte", empresa_slug="hidro-forte", empresa="Hidro Forte", tipo="fabricante/loja", cidade_uf=None,
         relevancia="media", plataforma="Loja Integrada", metodo="sitemap_jsonld", canal="site",
         endpoint="https://www.hidroforte.com.br/sitemap/product-1.xml", cats=SO_PRINCIPAIS, tem_preco_publico=True, ativo=True,
         observacao="WAF da Loja Integrada responde 403 a robô: esperado 'bloqueado'."),
    dict(fonte_id="deca-dexco", empresa_slug="deca-dexco", empresa="Deca (Loja Dexco)", tipo="fabricante (louças/banheiras)",
         cidade_uf=None, relevancia="baixa", plataforma="VTEX", metodo="vtex_catalog", canal="site", endpoint="https://www.lojadexco.com.br",
         cats=SO_PRINCIPAIS, params=dict(so_nome=True, caminhos=["/api/catalog_system/pub/products/search?ft=banheira"]),
         tem_preco_publico=True, ativo=True, observacao="WAF responde 403 inclusive ao robots.txt: esperado 'bloqueado'."),
    # ---------- registrados, mas bloqueiam robô (o log mostra "bloqueado") ----------
    dict(fonte_id="elevato", empresa_slug="elevato", empresa="Elevato", tipo="varejo multimarcas", cidade_uf="RS",
         relevancia="baixa", plataforma="headless Vercel", metodo="sitemap_jsonld", canal="site",
         endpoint="https://www.elevato.com.br/sitemap.xml", cats=SO_PRINCIPAIS, tem_preco_publico=True, ativo=True,
         params=dict(so_nome=True, url_regex=r"banheira|spa|ofuro", max_paginas=60),
         observacao="Vercel Security Checkpoint devolve 429 para robô: esperado 'bloqueado'."),
    dict(fonte_id="atacadao-da-hidro", empresa_slug="atacadao-da-hidro", empresa="Atacadão da Hidro", tipo="fabricante/loja",
         cidade_uf="Novo Horizonte-SP", relevancia="media", plataforma="Loja Integrada", metodo="sitemap_jsonld", canal="site",
         endpoint="https://www.atacadaodahidro.com.br/sitemap.xml", cats=SO_PRINCIPAIS, tem_preco_publico=True, ativo=True,
         params=dict(url_regex=r"banheir|spa|ofur"), observacao="WAF da Loja Integrada responde 403 a robô: esperado 'bloqueado'."),
    dict(fonte_id="ouro-fino-shop", empresa_slug="ouro-fino", empresa="Ouro Fino (Ouro Fino Shop)", tipo="revenda autorizada",
         cidade_uf=None, relevancia="media", plataforma="Loja Integrada", metodo="sitemap_jsonld", canal="site",
         endpoint="https://www.ourofinoshop.com.br/sitemap.xml", cats=SO_PRINCIPAIS, tem_preco_publico=True, ativo=True,
         params=dict(url_regex=r"banheir|spa|ofur"), observacao="Loja Integrada (403 para robô). Preço Ouro Fino também vem pela Telhanorte."),
    dict(fonte_id="leroy-merlin", empresa_slug="leroy-merlin", empresa="Leroy Merlin", tipo="varejo (home center)", cidade_uf=None,
         relevancia="media", plataforma="própria", metodo="sitemap_jsonld", canal="site",
         endpoint="https://www.leroymerlin.com.br/sitemap.xml", cats=SO_PRINCIPAIS, tem_preco_publico=True, ativo=True,
         params=dict(so_nome=True, url_regex=r"banheira|spa", max_paginas=60), observacao="robots.txt responde 403 para robô: esperado 'bloqueado'."),
    # ---------- sem método automático viável ou sem preço público (registrados, desativados) ----------
    dict(fonte_id="primavera-banheiras", empresa_slug="primavera-banheiras", empresa="Primavera Banheiras", tipo="fabricante", cidade_uf=None,
         relevancia="alta", plataforma="Mercado Livre (loja oficial)", metodo="mercadolivre_api", canal="marketplace:mercadolivre",
         endpoint="official_store_id=370268", cats=TODAS, tem_preco_publico=True, ativo=False,
         observacao="DESATIVADO: só vende no Mercado Livre; API exige app autenticado."),
    dict(fonte_id="total-hidro", empresa_slug="total-hidro", empresa="Total Hidro Banheiras", tipo="fabricante", cidade_uf=None,
         relevancia="media", plataforma="Mercado Livre", metodo="mercadolivre_api", canal="marketplace:mercadolivre", endpoint=None,
         cats=TODAS, tem_preco_publico=True, ativo=False, observacao="DESATIVADO: preço só no Mercado Livre (exige app autenticado)."),
    dict(fonte_id="serena-banheiras-e-spas", empresa_slug="serena-banheiras-e-spas", empresa="Serena Banheiras e Spas", tipo="fabricante",
         cidade_uf=None, relevancia="baixa", plataforma="Mercado Livre", metodo="mercadolivre_api", canal="marketplace:mercadolivre",
         endpoint="seller_id=216125395", cats=TODAS, tem_preco_publico=True, ativo=False,
         observacao="DESATIVADO: Mercado Livre (exige app autenticado)."),
    dict(fonte_id="king-banheiras", empresa_slug="king-banheiras", empresa="King Banheiras", tipo="fabricante", cidade_uf="São Paulo-SP",
         relevancia="baixa", plataforma="Mercado Livre", metodo="mercadolivre_api", canal="marketplace:mercadolivre", endpoint=None,
         cats=TODAS, tem_preco_publico=True, ativo=False, observacao="DESATIVADO: preço só no Mercado Livre."),
    dict(fonte_id="real-banheiras", empresa_slug="real-banheiras", empresa="Real Banheiras", tipo="fabricante", cidade_uf=None,
         relevancia="baixa", plataforma="Mercado Livre", metodo="mercadolivre_api", canal="marketplace:mercadolivre", endpoint=None,
         cats=TODAS, tem_preco_publico=True, ativo=False, observacao="DESATIVADO: preço só no Mercado Livre."),
    dict(fonte_id="espaco-deks-banheiras", empresa_slug="espaco-deks-banheiras", empresa="Espaço Deks Banheiras", tipo="fabricante",
         cidade_uf=None, relevancia="baixa", plataforma="Mercado Livre", metodo="mercadolivre_api", canal="marketplace:mercadolivre",
         endpoint="seller_id=1446226292", cats=TODAS, tem_preco_publico=True, ativo=False, observacao="DESATIVADO: Mercado Livre."),
    dict(fonte_id="mb-banheiras", empresa_slug="mb-banheiras", empresa="MB Banheiras", tipo="fabricante", cidade_uf="Santa Bárbara d'Oeste-SP",
         relevancia="baixa", plataforma="Mercado Livre", metodo="mercadolivre_api", canal="marketplace:mercadolivre", endpoint=None,
         cats=TODAS, tem_preco_publico=True, ativo=False, observacao="DESATIVADO: Mercado Livre + Instagram."),
    dict(fonte_id="hidrozen", empresa_slug="hidrozen", empresa="Hidrozen", tipo="fabricante", cidade_uf="Congonhal-MG",
         relevancia="alta", plataforma="landing própria + Mercado Livre", metodo="manual", canal="site", endpoint="https://hidrozen.com.br/",
         cats=TODAS, tem_preco_publico=True, ativo=False,
         observacao="DESATIVADO: mesma cidade da fábrica BBB; landing com poucos preços soltos (sem ficha por produto) e ML. Cliente oculto / ML com app."),
    dict(fonte_id="ferreira-costa", empresa_slug="ferreira-costa", empresa="Ferreira Costa", tipo="varejo (home center)", cidade_uf="Recife-PE",
         relevancia="baixa", plataforma="Next.js próprio", metodo="nextdata_categoria", canal="site",
         endpoint="https://ferreiracosta.com/Categoria/banheiras", cats=SO_PRINCIPAIS, tem_preco_publico=True, ativo=False,
         observacao="DESATIVADO nesta versão: 3 produtos; adaptador __NEXT_DATA__ específico não implementado."),
    dict(fonte_id="unique-spa", empresa_slug="unique-spa", empresa="Unique SPA (via revendas)", tipo="fabricante", cidade_uf=None,
         relevancia="baixa", plataforma="revendas (Bonatto, Bello Bagno)", metodo="manual", canal="site", endpoint=None,
         cats=SO_PRINCIPAIS, tem_preco_publico=True, ativo=False, observacao="DESATIVADO: preço só em revendas via busca (/pesquisa)."),
    dict(fonte_id="fibertec", empresa_slug="fibertec", empresa="Fibertec", tipo="fabricante", cidade_uf="Florianópolis-SC",
         relevancia="baixa", plataforma="WordPress página única", metodo="manual", canal="site", endpoint="https://lojafibertec.com.br/",
         cats=SO_PRINCIPAIS, tem_preco_publico=True, ativo=False, observacao="DESATIVADO: 6 preços soltos numa página única."),
    dict(fonte_id="jet-pool-spas", empresa_slug="jet-pool-spas", empresa="Jet Pool Spas", tipo="fabricante", cidade_uf="Imbituba-SC",
         relevancia="baixa", plataforma="WordPress institucional", metodo="manual", canal="site", endpoint="https://jetpool.com.br",
         cats=SO_PRINCIPAIS, tem_preco_publico=False, ativo=False, observacao="DESATIVADO: preço em texto livre nas páginas de linha."),
    dict(fonte_id="beny", empresa_slug="beny", empresa="Beny Lar e Construção", tipo="varejo", cidade_uf="Manaus-AM",
         relevancia="baixa", plataforma="Tray", metodo="tray_web_api", canal="site", endpoint="https://www.beny.com.br",
         cats=SO_PRINCIPAIS, tem_preco_publico=True, ativo=False, observacao="DESATIVADO: catálogo de 10 mil itens para 1 banheira."),
    dict(fonte_id="lazer-show", empresa_slug="lazer-show", empresa="Lazer Show", tipo="revenda Mondialle", cidade_uf="Belo Horizonte-MG",
         relevancia="baixa", plataforma="WooCommerce", metodo="woo_store_api", canal="site", endpoint="https://www.lazershow.ind.br",
         cats=SO_PRINCIPAIS, tem_preco_publico=False, ativo=False, observacao="DESATIVADO: só 6 produtos com preço; resto por orçamento."),
]
_INATIVOS_SEM_METODO = [
    ("jg-hidro", "JG Hidro", "Loja Integrada", "https://jghidro.lojaintegrada.com.br", "Loja Integrada responde 403 a robô (31/10/2026)."),
    ("casa-pool-sjc", "Casa Pool SJC", "Loja Integrada", "https://www.casapoolsjc.com.br", "Loja Integrada responde 403 a robô (31/10/2026)."),
    ("tanto", "Tanto", "Jet NEO", "https://www.tanto.com.br/categoria/banheiras-spas", "Sem JSON-LD; adaptador HTML Jet NEO não implementado."),
    ("hidrospa-bauru", "HidroSpa Bauru", "2RS CMS", "https://hidrospabrasil.2rscms.com.br/sitemap.xml", "Sem JSON-LD; adaptador HTML 2rscms não implementado."),
    ("dr-hidro", "Dr. Hidro", "WordPress + landing pages", "https://drhidro.com.br", "Preço em landing pages Elementor; sem ficha estruturada."),
    ("casa-das-aguas-teresina", "Casa das Águas Teresina", "Base44", None, "API de entidades Base44; adaptador não implementado."),
    ("super-hidro-spas", "Super Hidro Spas", "PHP próprio", "https://www.spasuperhidro.com.br/modelos", "Preço em texto livre por modelo."),
    ("ice-flow", "Ice Flow", "SPA React", "https://iceflow.com.br/precos", "Página de preços pré-renderizada; adaptador não implementado."),
    ("max-saunas-banheiras-ofuros", "Max Saunas, Banheiras e Ofurôs", "Wix Stores", "https://www.maxbanheirasofuros.com.br", "JSON-LD com preço 0 (sob consulta)."),
    ("roca-brasil", "Roca Brasil Store", "Magento 2", "https://www.rocabrasilstore.com.br/graphql", "GraphQL público, mas o adaptador Magento é o do cadastro BBB; Roca não vende banheira de hidro comparável."),
    ("magazine-luiza", "Magazine Luiza", "Magalu", None, "Akamai bloqueia robô (403)."),
    ("elite-banheiras", "Elite Banheiras", "Mercado Livre", None, "Só Mercado Livre (exige app autenticado)."),
    ("zdeque-banheiras", "Zdeque Banheiras", "Mercado Livre + Instagram", None, "Só Mercado Livre/Instagram."),
    ("porto-banheiras", "Porto Banheiras", "Mercado Livre", None, "Só Mercado Livre."),
    ("jucile-banheira-plenitud", "Jucile Banheira Plenitud", "Mercado Livre", None, "Só Mercado Livre."),
    ("bidell-hidromassagem", "Bidell Hidromassagem", "Mercado Livre", None, "Só Mercado Livre."),
    ("decortub", "Decortub", "Mercado Livre + Instagram", None, "Só Mercado Livre/Instagram."),
]
for _s, _n, _pl, _ep, _ob in _INATIVOS_SEM_METODO:
    CONCORRENTES.append(dict(fonte_id=_s, empresa_slug=_s, empresa=_n, tipo=None, cidade_uf=None, relevancia="baixa", plataforma=_pl,
                             metodo="manual", canal="site", endpoint=_ep, cats=TODAS, tem_preco_publico=True, ativo=False,
                             observacao="DESATIVADO: " + _ob))
# Concorrentes sem preço público (vitrine/orçamento) ficam fora desta coleta diária:
SEM_PRECO_PUBLICO = ["abba-home", "acqua-banheiras", "aqua-home-piscinas", "axell", "banheira-spa-bh", "banheiras-brasilia",
                     "casa-das-banheiras", "deck-tropical", "difference-hydro", "home-banheiras", "iguafibras", "innova-hidro",
                     "maxifibras", "mega-banheiras", "mundo-das-banheiras", "nature-spas", "ofuro-quero-banho", "paradiso-banheiras-e-spas",
                     "premier-banheiras", "riolax", "serras-banheiras", "universo-banheiras", "nicetubs", "plenitude-banheiras",
                     "fiberclass-hidromassagem", "hidromassagens-marjorie", "aquatec-piscinas", "todimo",
                     "calena", "portobello", "raya-banheiras-e-spas", "richesse-spas", "banheiras-design", "mondialle", "hidrowill",
                     "igapo-piscinas", "spas-versati", "sol-ar-mundo-agua", "espaco-zen-e-lazer", "hot-weather-saunas", "sauna-bh-sparta",
                     "via-spas-e-banheiras", "almeida-hidro-spa", "plaforte", "manancial-piscinas", "bela-metais"]
for _s in SEM_PRECO_PUBLICO:
    CONCORRENTES.append(dict(fonte_id=_s, empresa_slug=_s, empresa=_s.replace("-", " ").title(), tipo=None, cidade_uf=None,
                             relevancia="baixa", plataforma=None, metodo="sem_preco_publico", canal="site", endpoint=None,
                             cats=TODAS, tem_preco_publico=False, ativo=False,
                             observacao="Sem preço público (vitrine/orçamento): só cliente oculto."))


# ---------------------------------------------------------------------------
# Utilidades de texto e números
# ---------------------------------------------------------------------------
def agora():
    return datetime.datetime.now(TZ_SP)


def norm(s):
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode().lower()
    return re.sub(r"\s+", " ", s).strip()


def html_txt(s):
    s = re.sub(r"(?is)<(script|style|noscript|svg)[^>]*>.*?</\1>", " ", s or "")
    s = re.sub(r"(?i)<br\s*/?>|</p>|</li>|</tr>|</h\d>|</div>", "\n", s)
    s = html.unescape(re.sub(r"<[^>]+>", " ", s))
    s = re.sub(r"[ \t\r\f\v ]+", " ", s)
    return re.sub(r"\s*\n\s*", "\n", s).strip()


def brl(s):
    """'R$ 1.234,56' / '1234.56' / 1234.56 -> float"""
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s) if not (isinstance(s, float) and math.isnan(s)) else None
    t = str(s).replace("R$", "").replace("\xa0", " ").strip()
    m = re.search(r"\d[\d.,]*", t)
    if not m:
        return None
    t = m.group(0)
    if "," in t and "." in t:
        t = t.replace(".", "").replace(",", ".") if t.rfind(",") > t.rfind(".") else t.replace(",", "")
    elif "," in t:
        t = t.replace(".", "").replace(",", ".")
    elif t.count(".") > 1 or re.search(r"\.\d{3}$", t):
        t = t.replace(".", "")
    try:
        return float(t)
    except ValueError:
        return None


def fnum(x):
    try:
        if x is None or x == "":
            return None
        v = float(x)
        return None if math.isnan(v) else v
    except (TypeError, ValueError):
        return None


MARCADORES_CORTE = ["produtos relacionados", "produtos similares", "voce tambem pode gostar", "voce pode gostar",
                    "quem viu este produto", "quem viu, viu tambem", "quem comprou", "compre junto", "veja tambem",
                    "related products", "produtos recomendados", "aproveite e leve", "confira tambem", "outros produtos",
                    "you may also like", "recomendados para voce", "mais produtos", "produtos em destaque",
                    "complete seu projeto", "itens relacionados", "voce tambem vai amar", "voce tambem vai gostar",
                    "quem comprou, comprou tambem"]
# marcadores que só valem como linha inteira (no meio de uma frase, "mais vendidos" pode ser da descrição)
MARCADORES_CORTE_LINHA = re.compile(r"^(?:os )?mais vendidos$|^tambem viram$|^quem viu tambem viu$|^ofertas relacionadas$|"
                                    r"^aproveite tambem$|^produtos que voce viu$|^vistos recentemente$")
# depois de um carrossel de relacionados, a página às vezes volta para o produto (Balaroti: "Sobre este item")
RETOMA_PRODUTO = re.compile(r"^(?:sobre este (?:item|produto)|especificac(?:ao|oes)(?: tecnicas?)?|ficha tecnica|"
                            r"informacoes (?:complementares|tecnicas)(?: e tecnicas)?|caracteristicas(?: tecnicas| do produto)?|"
                            r"descricao(?: do produto)?)\s*:?$")


def _corta_linha(linha):
    n = norm(linha)
    # marcador citado no meio de uma frase como nome de seção ("Os acessórios opcionais ... serão indicados ao lado
    # em PRODUTOS RELACIONADOS e também no carrinho", BBB Campinas) não é o carrossel: não corta
    pos = [n.find(m) for m in MARCADORES_CORTE if n.find(m) >= 0
           and not re.search(r"\b(?:em|no|na|nos|nas|ao lado|aba|secao|campo)\s*[\"'“]?$", n[:n.find(m)])]
    if MARCADORES_CORTE_LINHA.search(n):
        pos.append(0)
    if not pos:
        return None
    i = min(pos)
    # norm() remove acento e junta espaços; na mesma linha o deslocamento é pequeno
    return linha[:i] if i > 0 else ""


def corta_relacionados(texto):
    """Mantém só o bloco do produto: tira o trecho de 'produtos relacionados' (e similares).
    Corta no marcador; se depois do carrossel a página voltar para o produto ('Sobre este item', 'Especificações',
    'Informações complementares'), retoma dali, até o próximo marcador."""
    if not texto:
        return ""
    out, ativo = [], True
    for linha in str(texto).split("\n"):
        if ativo:
            c = _corta_linha(linha)
            if c is None:
                out.append(linha)
            else:
                if c.strip():
                    out.append(c)
                ativo = False
        elif RETOMA_PRODUTO.match(norm(linha)):
            ativo = True
            out.append(linha)
    return "\n".join(out)


_RX_OPC_ROTULO = re.compile(r"(?im)(?:^|(?<=[\s.;*(]))(?:(?:itens?\s+|op[cç][oõ]es\s+)?(?:opcion(?:al|ais)|adicionais)"
                            # Splaz: "Equipamentos que você pode adicionar:" (aquecedor e cromo são opcionais pagos)
                            r"|(?:(?:equipamentos|itens|acess[oó]rios)\s+)?que\s+voc[eê]\s+pode\s+adicionar"
                            r"|(?:equipamentos|itens|acess[oó]rios)\s+que\s+podem\s+ser\s+adicionad[oa]s)\s*(?:[:\-–]|$)")
_RX_OPC_FIM = re.compile(r"(?im)^\s*(?:acompanha|itens de s[eé]rie|inclusos?|configura[cç][aã]o|conf\.?\s*kit|especifica[cç]|"
                         r"caracter[ií]sticas|ficha t[eé]cnica|descri[cç][aã]o|garantia|medidas|dimens)")


# Cabeçalho de lista de opcionais numa linha própria, sem ":" (reconferência r4): Fábrica das Banheiras "OPCIONAIS CASO
# QUEIRA AGREGAR AO SPA", "PRODUTOS ADICIONAIS A PARTE:", "ACESSÓRIOS ADICIONAIS PARA SPA". A linha não tem número nem
# preço (uma linha "Adicional de R$ 400 para 4 pontos" é item, não cabeçalho).
_RX_OPC_CAB = re.compile(r"(?im)^[ \t]*(?:(?:produtos|acess[oó]rios|itens|equipamentos|op[cç][oõ]es)[ \t]+)?"
                         r"(?:opcion(?:al|ais)|adicion(?:al|ais))\b[^\n:\d$]{0,50}:?[ \t]*$")
# "Os demais acessórios que não acompanham a Banheira ... podem ser encontrados em nossa loja separadamente" seguido da
# lista com "–", sem ":" (Acqualine Luminárias, reconferência r4 preços #21)
_RX_OPC_NAO_ACOMP = re.compile(r"(?im)^[^\n]{0,80}\bn[aã]o\s+acompanha\w*\b[^\n]{0,160}?\bseparadamente\b[ \t]*:?[ \t]*(?=\n|[–-]|$)")


def _opc_rotulos(t):
    """Início e fim do rótulo de cada lista de opcionais, em ordem."""
    ms = list(_RX_OPC_ROTULO.finditer(t)) + list(_RX_OPC_CAB.finditer(t)) + list(_RX_OPC_NAO_ACOMP.finditer(t))
    return sorted(ms, key=lambda m: m.start())


def sem_opcionais(texto):
    """Tira do texto a lista de opcionais ('Opcional: 6 Jatos Plus, 2 Jatos Grandes...', 'ITEM OPCIONAL:', bloco
    'Opcionais'), para jatos/motores/potência não somarem o que não vem com o produto. A lista vai até a próxima seção
    (Acompanha, Configuração, Especificações...) ou o fim. 'Inox opcional c/ custo' no meio da frase não é rótulo.
    Também valem o cabeçalho em linha própria sem ':' ('OPCIONAIS CASO QUEIRA AGREGAR AO SPA') e a frase
    'não acompanham ... separadamente' seguida da lista."""
    t = str(texto or "")
    out, pos = [], 0
    for m in _opc_rotulos(t):
        if m.start() < pos:
            continue
        out.append(t[pos:m.start()])
        f = _RX_OPC_FIM.search(t, m.end())
        pos = f.start() if f else len(t)
    out.append(t[pos:])
    return "".join(out)


def bloco_produto_html(h, nome=None, max_chars=8000):
    """Texto do produto a partir do HTML: remove cabeçalho/menu/rodapé, começa no nome e para nos relacionados."""
    h = re.sub(r"(?is)<(header|nav|footer|aside)[^>]*>.*?</\1>", " ", h or "")
    # navegação "produto anterior / próximo produto" do tema (Woodmart "woodmart-products-nav", "wd-products-nav",
    # "product-navigation"): o nome do produto vizinho ("Acionador Digital com Sensor de Nível") virava item incluso
    h = re.sub(r'(?is)<div[^>]*class="[^"]*\b(?:woodmart-products-nav|wd-products-nav|product-navigation|products-nav)\b'
               r'.*?(?=<h1\b)', " ", h)
    t = html_txt(h)
    if nome:
        i = norm(t).find(norm(nome)[:40])
        if i > 0:
            t = t[i:]
    return corta_relacionados(t)[:max_chars]


def ld_json_blocos(h):
    out = []
    for m in re.findall(r'(?is)<script[^>]*application/ld\+json[^>]*>(.*?)</script>', h or ""):
        try:
            d = json.loads(m.strip())
        except Exception:
            try:
                d = json.loads(re.sub(r"[\x00-\x1f]", " ", m.strip()))
            except Exception:
                continue
        pilha = d if isinstance(d, list) else [d]
        while pilha:
            x = pilha.pop(0)
            if isinstance(x, dict):
                out.append(x)
                if isinstance(x.get("@graph"), list):
                    pilha.extend(x["@graph"])
            elif isinstance(x, list):
                pilha.extend(x)
    return out


def ld_tipo(x, t):
    tp = x.get("@type")
    return tp == t or (isinstance(tp, list) and t in tp)


_RS = r"R\$\s*([\d.]+,\d{2}|\d+(?:\.\d{2})?)"


def _pix_html(h, preco_ref=None):
    """Preço no Pix publicado na página do produto. Aceita 'R$ X no pix', 'R$ X à vista no pix', 'À vista por R$ X
    (no PIX ...)', 'Pix: R$ X'. Recebe HTML ou texto; vale o 1º valor entre 80% e 100% do preco_ref (descarta Pix de
    vitrine, de 'compre junto' e de outro produto)."""
    t = _junta_valor(html_txt(h) if "<" in (h or "") else (h or ""))
    t = re.sub(r"[ \t\xa0]+", " ", t.replace("\n", " "))
    cands = []
    for m in re.finditer(_RS + r"[^R$%]{0,40}?(?:no|via|pelo|com)\s+pix\b", t, re.I):  # sem "%": "Ganhe 5% ... no Pix" é selo
        cands.append(m.group(1))
    # "Pix: R$ X", "no Pix por R$ X" (rótulo colado ao valor; "Crédito Pix R$ X sem juros" de tabela de parcelas não vale)
    for m in re.finditer(r"\bpix\b\s*(?::|=|por|-)\s*" + _RS, t, re.I):
        cands.append(m.group(1))
    for v in cands:
        x = brl(v)
        if x is None:
            continue
        if preco_ref is None or (0.8 * preco_ref - 0.01 <= x <= preco_ref + 0.01):
            return x
    return None


def _junta_valor(t):
    """Tray quebra o valor em spans: 'R$ 269 , 83' / '2.428 , 51' vira 'R$ 269,83'."""
    return re.sub(r"(\d)\s+([.,])\s*(\d{2,3})\b", r"\1\2\3", re.sub(r"(\d)([.,])\s+(\d{2,3})\b", r"\1\2\3", t))


def _parcelas_html(h):
    """Maior número de parcelas sem juros publicado ('10x de R$ 1.264,68 sem juros', 'até 12x sem juros', Tray
    '10 x de R$ 269 , 83 Sem juros', tabela Magento '6x de' + 'R$1.431,67 Sem juros' em linhas separadas)."""
    t = norm(_junta_valor(html_txt(h) if "<" in (h or "") else (h or "")))
    m = re.findall(r"(?:ate|em|ou)?\s*(\d{1,2})\s*x\s*(?:de\s*)?(?:r\$\s*[\d.,]+\s*)?sem juros", t)
    v = [int(x) for x in m if 1 < int(x) <= 24]
    return max(v) if v else None


def _pix_percentual_html(h, preco_ref=None):
    """Desconto do Pix da loja em % ('10% OFF no PIX', 'no pix 10% de desconto', 'PIX à vista 10% OFF').
    Sem o selo, deduz da razão Pix/preço na mesma página, quando os dois estão publicados."""
    t = norm(html_txt(h) if "<" in (h or "") else (h or ""))
    m = (re.search(r"(\d{1,2})\s*%\s*(?:off|de desconto|desconto)[^.]{0,20}\bpix\b", t)
         or re.search(r"\bpix\b[^.%]{0,30}?(\d{1,2})\s*%\s*(?:off|de desconto|desconto)", t))
    if m and 0 < int(m.group(1)) <= 20:
        return float(m.group(1))
    if preco_ref:
        px = _pix_html(h, preco_ref)
        if px and px < preco_ref:
            pct = round((1 - px / preco_ref) * 100, 1)
            if 0 < pct <= 20:
                return pct
    return None


def _cartao_da_pagina(h):
    """Preços de cartão publicados no bloco do produto: 'Ou R$ Y em até 12x', 'R$ Y em 10x' e '12x de R$ Z sem juros'
    (Y = N x Z). Lista, na ordem da página."""
    t = _junta_valor(html_txt(h) if "<" in (h or "") else (h or ""))
    t = re.sub(r"\s+", " ", t)
    out = []
    for m in re.finditer(_RS + r"\s*(?:em\s+at[eé]|em|at[eé]|parcelado\s+em)\s*(\d{1,2})\s*x", t, re.I):
        out.append(brl(m.group(1)))
    for m in re.finditer(r"(\d{1,2})\s*x\s*(?:de\s*)?" + _RS + r"\s*sem juros", t, re.I):
        v = brl(m.group(2))
        if v:
            out.append(round(int(m.group(1)) * v, 2))
    return [x for x in out if x]


def _por_ja_e_pix(h, preco, pix=None, pct=None, cartao_ld=None):
    """Regra geral (reconferência r3, Mérito): o "Por" da página JÁ é o preço do Pix quando
      (a) a página declara "R$ X no Pix" e X é igual ao "Por"; ou
      (b) a página declara "N% de desconto no Pix" e mostra um preço de cartão Y com Y x (1 - N%) = "Por"
          ('De 18.034,94 Por 16.231,45 · 10% de desconto no Pix ou boleto · Ou R$ 18.034,94 em até 12x').
    Devolve o preço de cartão Y (vira preco_por; o "Por" vira preco_pix) ou None. Não aplica o desconto de novo."""
    if not preco:
        return None
    # ordem de confiança: maxPrice do JSON-LD e "Ou R$ Y em até Nx" (total publicado) antes de N x parcela
    # (12 x 1.502,91 = 18.034,92 arredonda 2 centavos abaixo do total 18.034,94 da página)
    cands = ([cartao_ld] if cartao_ld else []) + _cartao_da_pagina(h)
    for y in dict.fromkeys(round(x, 2) for x in cands if x and preco + 0.01 < x <= preco * 1.35):
        if pix is not None and abs(pix - preco) <= 0.02:
            return y
        if pct and abs(y * (1 - pct / 100.0) - preco) <= max(0.05, 0.0005 * y):
            return y
    return None


def _pix_da_loja(c, url, preco_ref=None):
    """Lê uma vez por fonte (na 1ª página de produto) o % do Pix e as parcelas sem juros. Cache no coletor."""
    if getattr(c, "_pix_loja", None) is not None:
        return c._pix_loja
    res = {"pct": None, "parcelas": None, "url": url}
    try:
        h = c.f.texto(url)
        res["pct"] = _pix_percentual_html(h, preco_ref)
        res["parcelas"] = _parcelas_html(bloco_produto_html(h, None, 20000))
    except (requests.RequestException, Bloqueado, Corte) as e:
        c.f.notas.append("pix da loja não lido (%s)" % type(e).__name__)
    c._pix_loja = res
    return res


# ---------------------------------------------------------------------------
# Normalizador de atributos (só recebe texto do bloco do produto/variante)
# ---------------------------------------------------------------------------
PALAVRAS_NUM = {"um": 1, "uma": 1, "dois": 2, "duas": 2, "tres": 3, "quatro": 4, "cinco": 5, "seis": 6, "sete": 7,
                "oito": 8, "nove": 9, "dez": 10, "doze": 12}
_NUM = r"(\d{1,2}|um|uma|dois|duas|tres|quatro|cinco|seis|sete|oito|nove|dez|doze)"


def _n(x):
    return PALAVRAS_NUM.get(x, None) if not str(x).isdigit() else int(x)


_RX_DIAM = re.compile(r"(?:\bdiam\b|\bdiametro\b)\s*(?:de\s*)?:?\s*(\d{1,3}(?:[.,]\d{1,3})?)\s*(m|cm)?"
                      r"(?:\s*(?:x|\|)\s*(?:(?:altura|alt\.?|profundidade|prof\.?)\s*:?\s*)?(\d{1,3}(?:[.,]\d{1,3})?)\s*(m|cm)?)?")


def _metros(g):
    if g is None:
        return None
    v = float(g.replace(",", "."))
    if v >= 400:
        v = v / 1000.0
    elif v > 4.5:
        v = v / 100.0
    return round(v, 3)


def extrai_medidas(texto):
    """Lista de (comprimento, largura, profundidade) em metros encontrados no texto.
    Diâmetro ('Ø 1,50 x 0,40 m', 'Diâmetro: 2,00 m'): comprimento = largura = Ø e o 2º número é a profundidade."""
    t0 = re.sub(r"[Øø⌀∅]", " diam ", str(texto or ""))  # Ø some no norm() (NFKD não decompõe)
    # "1,8 × 1,8 × 0,71 m" (Lineaplas): o × vira x antes do norm(), que descarta o que não é ASCII
    t = norm(t0.replace("×", "x").replace("✕", "x"))
    # "(C) 2,90 (L) 2,20 x (A) 0,90": rótulos de comprimento/largura/altura no lugar do "x"
    t = re.sub(r"\(c\)\s*([\d.,]+)\s*(m|cm)?\s*x?\s*\(l\)\s*([\d.,]+)\s*(m|cm)?(?:\s*x?\s*\((?:a|p|h)\)\s*([\d.,]+))?",
               lambda m: " %s%s x %s%s%s " % (m.group(1), m.group(2) or "", m.group(3), m.group(4) or "",
                                              (" x " + m.group(5)) if m.group(5) else ""), t)
    res = []
    for m in _RX_DIAM.finditer(t):
        d, p = _metros(m.group(1)), _metros(m.group(3))
        if d is None or not (0.9 <= d <= 4.5) or re.match(r"\s*mm", t[m.end(1):]):
            continue  # diâmetro de tubo/dreno ("diâmetro 50 mm") não é medida da banheira
        if p is not None and not (0.15 <= p <= 2.0):
            p = None
        res.append((d, d, p))
    t = _RX_DIAM.sub(" ", t)  # o par "Ø x profundidade" não é comprimento x largura
    for m in re.finditer(r"(\d{1,3}(?:[.,]\d{1,3})?)\s*(m|cm)?\s*x\s*(\d{1,3}(?:[.,]\d{1,3})?)\s*(m|cm)?"
                         r"(?:\s*x\s*(\d{1,3}(?:[.,]\d{1,3})?)\s*(mm|cm|m)?)?", t):
        vals = []
        brutos = [float(g.replace(",", ".")) for g in (m.group(1), m.group(3), m.group(5)) if g is not None]
        # milímetros só quando o conjunto tem número de 4 dígitos ("1700 x 800 x 450"); sem isso, 3 dígitos são
        # centímetros (Faiberplas Spa Caribe "403 x 207 x 85" = 4,03 m, não 0,403; reconferência r4 preços #5)
        em_mm = any(v >= 1000 for v in brutos)
        for i, g in enumerate((m.group(1), m.group(3), m.group(5))):
            if g is None:
                vals.append(None)
                continue
            v = float(g.replace(",", "."))
            if v > 4.5:
                v = v / (1000.0 if em_mm or (i == 2 and m.group(6) == "mm") else 100.0)
            vals.append(round(v, 3))
        a, b, c = vals
        if a is None or b is None or a < 0.25 or b < 0.25 or a > 4.5 or b > 4.5:
            continue
        if c is not None and (c < 0.15 or c > 2.0):
            c = None
        if c is None and m.group(5) is None and min(a, b) <= 0.60 and max(a, b) >= 1.0:
            # só 2 números e o 2º de 0,60 m ou menos: é comprimento x profundidade ("150 x 40 Cm" da Faiberplas
            # Ipanema), não largura; a largura fica vazia
            res.append((max(a, b), None, min(a, b) if min(a, b) >= 0.15 else None))
            continue
        res.append((max(a, b), min(a, b), c))
    return res


def extrai_lugares(texto, permite_palavras=True):
    """Número de pessoas. Aceita '4 lugares', '2 a 3 pessoas', 'duas pessoas', 'acomoda confortavelmente duas pessoas',
    'Capacidade para 5 pessoas', 'Acomoda: 4 adultos', 'Nº recomendado de pessoas (máx): 3 a 5', 'Capacidade: 1 pessoa'.
    Faixa vale o máximo. permite_palavras: 'individual' = 1, 'dupla/casal' = 2 (só no nome/variante)."""
    t = norm(texto)
    vals = []
    # rótulo antes do número: "pessoas (max): 3 a 5", "lugares: 4", "capacidade de pessoas: 3"
    for a, b in re.findall(r"(?:pessoas|lugares|ocupantes)\s*(?:\([^)]{0,15}\))?\s*:\s*" + _NUM + r"(?:\s*(?:a|ate|-|/|ou)\s*" + _NUM + r")?\b", t):
        v = max(_n(a) or 0, _n(b) or 0) if b else _n(a)
        if v and 1 <= v <= 12:
            vals.append(v)
    for a, b in re.findall(r"\b" + _NUM + r"(?:\s*(?:a|ate|-|/|ou)\s*" + _NUM + r")?\s*(?:lugares|lugar|pessoas|pessoa|adultos|adulto)\b", t):
        v = max(_n(a) or 0, _n(b) or 0) if b else _n(a)
        if v and 1 <= v <= 12:
            vals.append(v)
    if vals:
        return vals[0]
    if permite_palavras:
        if re.search(r"\bindividua(l|is)\b", t):
            return 1
        if re.search(r"\b(dupla|duplo|casal|duo)\b", t):
            return 2
    return None


def extrai_material(texto):
    t = norm(texto)
    tem_acr = re.search(r"acril", t)
    tem_gel = re.search(r"gel ?coat|gelcoat|\bem gel\b|material\s*:\s*gel\b", t)
    if tem_acr and not tem_gel:
        return "acrilico"
    if tem_gel and not tem_acr:
        return "gel coat"
    if tem_acr and tem_gel:
        return None  # texto cita os dois (ex.: escolha por variante): não decide
    if re.search(r"fibra de vidro|\bfibra\b|poliester", t):
        return "fibra"
    if re.search(r"solid ?surface|ferro fundido|resina|marmore|cerami|aco\b|madeira|luxecast", t):
        return "outro"
    return None


def _soma_unica(achados):
    vistos, tot = set(), 0
    for chave, v in achados:
        if chave in vistos:
            continue
        vistos.add(chave)
        tot += v
    return tot if vistos else None


_DESC_VAZIO = {"", "de", "e", "com", "na", "nas", "no", "nos", "em", "para", "-", "da", "do", "que", "sendo", "total",
               # descritivos sem sentido de grupo: "13 Jatos 175x175x80cm" -> "x"/"cm"; "13 jatos de hidromassagem"
               "x", "cm", "m", "mm", "hidromassagem", "hidro", "massagem", "os", "as", "a", "o", "sao", "ao", "um", "uma",
               "que", "oferecem", "oferece", "proporcionam", "acrilico", "cada", "por", "ou", "inclusos", "incluso"}


def _chave_desc(resto):
    """Primeira palavra descritiva logo depois de 'jatos' ('grandes', 'cromados'); para no 1º token com número ou
    pontuação ('13 Jatos 175x175x80cm Spa...' não tem descritivo)."""
    for tok in (resto or "").split():
        if not re.fullmatch(r"[a-z]+", tok):
            break
        if tok not in _DESC_VAZIO and len(tok) > 1:
            return tok
    return ""


_JATO = r"(?:hidro)?jat(?:o|oe)s?\b"  # "jatões" vira "jatoes" no norm()


def extrai_jatos(texto):
    """(jatos, mini_jatos). Soma grupos distintos ('4 jatos grandes' + '2 jatos laterais'); o mesmo grupo repetido
    no nome e na descrição conta uma vez (chave = número + primeira palavra descritiva). Aceita 'jatões', '20J' no
    nome e 'Total de Jatos (...): 19' (total declarado vence a soma). Lista de opcionais não entra."""
    # linha com preço ("01 - Kit combo de 10 jatos com motobomba 1 CV R$ 1.350,00") é item vendido à parte (Fábrica
    # das Banheiras, reconferência r4 preços #18)
    t = norm("\n".join(x for x in sem_opcionais(texto).split("\n") if not re.search(r"R\$\s*\d", x)))
    # "10 - Mini Jatos", "10 - Jatos Grandes" (Fábrica): o hífen entre a quantidade e o item
    t = re.sub(r"\b(\d{1,2})\s*-\s+(?=(?:mini|micro|hidro|turbo|super|maxi)?[\s-]?jat)", r"\1 ", t)
    # total com o detalhamento entre parênteses: "10 jatos de hidromassagem (4 grandes e 6 mini jatos)" (Telhanorte
    # Nautacril) = 4 jatos + 6 mini, não 10 + 6 (reconferência r3, par 20)
    for m in re.finditer(r"\b(\d{1,2})\s*" + _JATO + r"[^()\n]{0,40}\(\s*(\d{1,2})\s*(?:jat\w*\s*)?(?:grandes?|normais|"
                         r"turbo|direcion\w*|maiores)?[^()\d]{0,20}?(?:\be\b|\+|,)\s*(\d{1,2})\s*(?:mini|micro)[\s-]?jat", t):
        tot, a, b = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if a + b == tot:
            return a, b
    minis, jatos, totais = [], [], []
    # "micro jatos" (WT Fibras: "08 Micro Jatos terapêuticos" + "04 Jatos Direcionável") são mini jatos
    for m in re.finditer(_NUM + r"\s*(?:\(\w+\)\s*)?(?:mini|micro)[\s-]?" + _JATO + r"(?=([^\n,;.]{0,25}))", t):
        v = _n(m.group(1))
        if v:
            minis.append(((v, _chave_desc(m.group(2))), v))
    for m in re.finditer(r"(?:mini|micro)[\s-]?jat(?:o|oe)s?\s*[:=]\s*(\d{1,2})", t):  # só ":" ou "=": "mini jatos - 1 entrada" é o item seguinte da lista
        minis.append(((int(m.group(1)), ""), int(m.group(1))))
    for m in re.finditer(_NUM + r"\s*(?:\(\w+\)\s*)?(?:(turbo|super|maxi)\s+)?" + _JATO + r"(?=([^\n,;.]{0,25}))", t):
        ini = m.start()
        antes = t[max(0, ini - 6):ini]
        if re.search(r"(?:mini|micro)[\s-]?$", antes):
            continue
        if re.search(r"jat[a-z]*\b[^:=]{0,45}[:=]\s*(?:\d{1,2}\s+)?$", t[max(0, ini - 60):ini]):
            continue  # "Jatos Ajustáveis (PLW): 3 Jatos Direcionáveis": o número é valor do rótulo anterior
        if re.search(r"\b(?:a|para)\s+cada\s+$", t[max(0, ini - 12):ini]):
            continue  # "1 motobomba de 1cv a cada 10 jatos": proporção, não quantidade do produto
        v = _n(m.group(1))
        if v and v <= 60:
            chave = (v, m.group(2) or _chave_desc(m.group(3)))
            if chave[1] == "plus":  # "15 jatos plus": mini jatos nas costas (Hidromundi: "Jatos plus: são mini jatos")
                minis.append((chave, v))
            else:
                jatos.append((chave, v))
    # "20J" no nome (Personalizze: "...ACRILICO, 20J")
    for m in re.finditer(r"(?:^|[\s,;(])(\d{1,2})\s?j\b(?![./])", t):
        v = int(m.group(1))
        if 1 <= v <= 60:
            jatos.append(((v, ""), v))
    # "Total de jatos: 19", "Total de Jatos Cinzas (ou Inox opcional c/ custo): 19"
    for m in re.finditer(r"total de jat(?:o|oe)s?\b[^:\n]{0,40}?(?:\([^)]{0,60}\))?\s*[:=]\s*(\d{1,2})\b", t):
        totais.append(int(m.group(1)))
    # rótulo com quantidade: "Jatos: 3", "Jatos Direcionáveis (BMH): 2", "jatos laterais = 4"
    rot = []
    # o valor não é medida ("jatos plus nas costas Tamanho: 2,20 x 2,20": o norm() junta as linhas)
    for m in re.finditer(r"(?<!mini )(?<!mini-)(?<!micro )(?<!micro-)(?<!total de )jat(?:o|oe)s?((?: [a-z]+){0,4})\s*(?:\([^)]{0,20}\))?\s*[:=]\s*(\d{1,2})\b(?![.,]\d)", t):
        rot.append(((int(m.group(2)), _chave_desc(m.group(1))), int(m.group(2))))
    # "Jatos: 3 | Jatos Direcionáveis: 2 | Jatos Ajustáveis: 1": o rótulo sem descritivo é o total quando bate com a soma
    sem = [v for k, v in rot if not k[1]]
    com = [(k, v) for k, v in rot if k[1]]
    if sem and com and sum(dict(com).values()) == sem[0]:
        rot = [x for x in rot if not x[0][1]]
    jatos += rot

    # mesma quantidade com e sem descritivo ("10 jatos" no nome e "10 jatos cromados" na ficha) conta uma vez
    def limpa(ach):
        com_desc = {k[0] for k, _ in ach if k[1]}
        return [(k, v) for k, v in ach if k[1] or k[0] not in com_desc]

    def total_declarado(ach):
        """'Configuração G4 com 8 jatos' + '5 jatos direcionáveis' + '3 jatos ajustáveis' (na descrição e de novo na
        ficha 'Tipo de Jato'): a quantidade sem descritivo que é igual à soma dos grupos é o total; vale ela."""
        sem_d = {k[0] for k, _ in ach if not k[1]}
        grupos = {k: v for k, v in ach if k[1]}
        soma = sum(grupos.values())
        return soma if len(grupos) >= 2 and soma in sem_d else None
    jl = limpa(jatos)
    tot_j = totais[0] if totais else (total_declarado(jl) or _soma_unica(jl))
    return tot_j, (None if totais else _soma_unica(limpa(minis)))


def extrai_motores(texto):
    t = norm(sem_opcionais(texto))
    achados = []
    for m in re.finditer(_NUM + r"\s*x?\s*(?:\(\w+\)\s*)?(?:moto[\s-]?bombas?|motobombaa|motores|motor)\b", t):
        v = _n(m.group(1))
        # "(01 motobomba de 1cv a cada 10 jatos)": proporção da linha, não quantidade do produto (Splaz, r3 preços #8;
        # a mesma régua de extrai_jatos)
        if re.match(r"[^.;\n()]{0,30}?\b(?:a|para)\s+cada\s+\d", t[m.end():]):
            continue
        if v and v <= 6:
            achados.append((m.group(0), v))
    tot = _soma_unica(achados)
    if tot:
        return tot
    if re.search(r"moto[\s-]?bomba", t):
        return 1
    # "motor 1cv", "motor de 1/2 cv": um motor citado no singular, sem quantidade, e sem plural no texto
    if re.search(r"\bmotor\b(?:\s+de)?\s+(?:\d+(?:[.,]\d+)?|\d/\d)\s*(?:cv|hp)\b", t) and not re.search(r"\bmotores\b", t):
        return 1
    return None


_RX_MOTOR_PROPORCAO = re.compile(r"\b(\d{1,2}|um|uma)\s*(?:moto[\s-]?bo?mbas?|motores|motor)\s*(?:de\s+)?(\d+(?:[.,]\d+)?|\d/\d)?\s*"
                                 r"(?:cv|hp)?\s*(?:a|para)\s+cada\s+(\d{1,2})\s*(?:hidro)?jat")


_RX_MOTOR_PROPORCAO_INV = re.compile(r"(?:a|para)\s+cada\s+(\d{1,2})\s*(?:hidro)?jat\w*\s*,?\s*(?:acompanha|vem|tem)\s+(\d{1,2}|um|uma)\s*"
                                     r"(?:moto[\s-]?bo?mbas?|motores|motor)\s*(?:de\s+)?(\d+(?:[.,]\d+)?|\d/\d)?")


def motores_por_proporcao(texto, jatos):
    """'(01 motobomba de 1cv a cada 10 jatos)' (Splaz): nº de motores da VARIANTE = N x (jatos // K), mínimo N; a
    potência por motor é a da frase. 12 jatos = 1 motor (r3 preços #8); 42 = 4; 52 = 5 (reconferência r4 preços
    #9 e #10: as variantes de 20 a 52 jatos ficavam com 1 motor). Devolve (motores, cv por motor) ou None."""
    if not jatos:
        return None
    t = norm(sem_opcionais(texto))
    m = _RX_MOTOR_PROPORCAO.search(t)
    if m:
        n, k, cvg = _n(m.group(1)) or 1, int(m.group(3)), m.group(2)
    else:
        # ordem inversa: "(a cada 10 jatos, acompanha 1 motobomba de 1cv)"
        m = _RX_MOTOR_PROPORCAO_INV.search(t)
        if not m:
            return None
        n, k, cvg = _n(m.group(2)) or 1, int(m.group(1)), m.group(3)
    if not (1 <= k <= 30):
        return None
    cv = _cv_val(cvg) if cvg else None
    return n * max(1, int(jatos) // k), (cv if cv and 0.1 <= cv <= 10 else None)


_CV_NUM = r"(?:\d+(?:[.,]\d+)?|\d/\d)"
_RX_CV_LISTA = re.compile(r"(?:" + _CV_NUM + r"\s*(?:cv|hp)?\s*(?:;|,\s|\bou\b|\s/\s)\s*)+" + _CV_NUM + r"\s*(?:cv|hp)\b")


def _cv_val(g):
    if "/" in g:
        a, b = g.split("/")
        return float(a) / float(b) if float(b) else None
    return float(g.replace(",", "."))


def extrai_cv(texto):
    """Potência por motor (cv). Lista de opções ('Potência 1,00 CV ; 1,5 CV ; 2,00 CV', '1, 1,5 ou 2 cv') é escolha do
    cliente, não a potência do produto: não entra (a variante, quando diz, vence)."""
    t = norm(sem_opcionais(texto).replace("\u00bd", "1/2"))

    def _lista(m):
        vs = [_cv_val(x) for x in re.findall(r"\d+(?:[.,]\d+)?|\d/\d", m.group(0))]
        vs = [v for v in vs if v]
        ok = len(vs) >= 2 and all(0.25 <= v <= 5 for v in vs) and all(a < b for a, b in zip(vs, vs[1:]))
        return " " if ok else m.group(0)
    t = _RX_CV_LISTA.sub(_lista, t)
    vals = []
    for m in re.finditer(r"(\d+(?:[.,]\d+)?|\d/\d)\s*(?:cv|hp)\b", t):
        g = m.group(1)
        if "/" in g:
            a, b = g.split("/")
            v = float(a) / float(b) if float(b) else None
        else:
            v = float(g.replace(",", "."))
        if v and 0.1 <= v <= 10:
            vals.append(v)
    return max(vals) if vals else None


def so_opcionais(texto):
    """O que sem_opcionais() tira: só as listas de opcionais."""
    t = str(texto or "")
    partes, pos = [], 0
    for m in _opc_rotulos(t):
        if m.start() < pos:
            continue
        f = _RX_OPC_FIM.search(t, m.end())
        pos = f.start() if f else len(t)
        partes.append(t[m.start():pos])
    return "\n".join(partes)


def _flag_item(nome, texto, item_rx):
    """True = incluso de série; False = opcional/ausente; None = não dá para afirmar.
    Nome: 'sem'/'s/' antes do item = False ('S/ AQUECEDOR', 'S/AQUECEDOR'); 'com'/'c/'/'+' = True.
    Texto: negação explícita > item listado no que acompanha (linha '1 Aquecedor elétrico', 'AQUECEDOR DIGITAL 5000W'
    dentro do kit) > item só na lista de opcionais = False. Item que o texto não cita fica None (não inventa).
    Item que a mesma página põe na lista de opcionais ('Opcionais: ... aquecedor universal ..., cromoterapia') não
    acende o incluso pelo texto, mesmo que outra frase diga 'Itens inclusos: aquecedor' (Aquaplas Nauta Basic:
    a descrição é a mesma das três versões; o que vale é a lista de opcionais e a tabela de itens de série)."""
    rx = r"(?:" + item_rx + r")"
    n = norm(nome)
    if re.search(r"(?:\bsem|\bs/)\s*" + rx, n):
        return False
    kit = r"(?:kit\s+(?:de\s+)?)?"
    if re.search(r"(?:\bcom|\bc/|\+)\s*(?:o\s+|um\s+)?" + kit + rx, n):
        return True
    # "Spa ... Com Aquecedor Digital e Kit Cromoterapia": item ligado por "e"/"+" a um "com" anterior do nome
    if re.search(r"(?:\bcom|\bc/)\s[^|,;]{0,50}?(?:\be|\+|,)\s*(?:o\s+|um\s+)?" + kit + rx, n):
        return True
    base = sem_opcionais(texto)
    t = norm(base)
    neg = [r"(?:\bsem|\bs/)\s*" + rx,
           rx + r"[^.\n]{0,40}(opcional|adiciona|\+\s*r\$|vendido separad|nao acompanha|nao incluso|nao inclu|quando opcional)",
           r"(opciona(l|is)|adicionais|nao acompanha|nao faz(em)? parte)[^.\n]{0,60}" + rx,
           rx + r"\s*:\s*(?:nao|-|opcional)\b"]
    pos = [r"\bcom\s+" + rx, rx + r"[^.\n]{0,20}(incluso|de serie|incluido|acompanha)",
           r"(acompanha|itens de serie|vem com|inclui|inclusos?)\s*:?[^.\n]{0,80}" + rx,
           rx + r"\s*:\s*(?:sim|de serie|incluso)\b",
           r"(?:^|\s)\d{1,2}\s+pontos?\s+de\s+" + rx]
    if any(re.search(p, t) for p in neg):
        return False
    # 1) item listado como linha do que acompanha (vence a lista geral de opcionais: kit da Aqually)
    # linha de lista do que acompanha: "1 Aquecedor elétrico analógico", "– 01 Cromoterapia", "AQUECEDOR DIGITAL 5000W"
    # (linha só com a palavra, como título de aba, não conta)
    # item com quantidade no meio de uma lista corrida ("...(Brinde) 01 Aquecedor digital", "; 01 Cromoterapia")
    # "01 Kit Cromoterapia com 2 pontos em led": kit com quantidade é item que acompanha
    if re.search(r"(?:^|[^\d.,/%])\d{1,2}\s+(?:un\.?\s+)?" + kit + rx + r"\b", t):
        return True
    secao = ""
    # cabeçalho de lista do que NÃO vem com o produto ("Os demais acessórios que não acompanham a Banheira de Hidro
    # completa podem ser encontrados em nossa loja separadamente:" na Acqualine): as linhas abaixo não são inclusos
    rx_sec_neg = r"nao acompanha|separadamente|vendidos? a parte|opciona(?:l|is)|adicionais|nao inclu"
    for linha in base.split("\n"):
        ln = norm(linha)
        if re.match(r"^.{3,200}:\s*$", ln):  # cabeçalho longo (até 200 caracteres) terminado em ":"
            secao = ln
            continue
        if re.match(r"^(?:[-*>\u2022\u2013]\s*)?" + kit + rx + r"\b", ln) and re.search(rx_sec_neg, secao):
            return False
        if re.match(r"^(?:[-*>\u2022]\s*)?(?:\d{1,2}\s+(?:un\.?\s+)?)?" + kit + rx + r"\b", ln) and len(ln.split()) >= 2:
            return True
        # item sozinho na linha, dentro da lista "Itens inclusos:" (Aquasol Jacuzzi: "...Sistema de Drenagem: POP-UP\nAquecedor")
        if re.match(r"^(?:[-*>\u2022]\s*)?" + rx + r"$", ln) and re.search(r"inclus|acompanha|de serie|vem com", secao):
            return True
        if re.match(r"^[^:]{3,40}:\s*$", ln):  # cabeçalho de seção ("ITENS INCLUSOS:", "ATENÇÃO:")
            secao = ln
    # 2) a mesma página põe o item na lista de opcionais: a frase corrida "Itens inclusos: aquecedor, ..." não vence
    if re.search(rx, norm(so_opcionais(texto))):
        return False
    # 3) frase corrida que diz incluso
    if any(re.search(p, t) for p in pos):
        return True
    return None


# Itens que a BBB vende como opcional pago (lista de opcionais do Magento) além de aquecedor e cromo. Quando o concorrente
# inclui de série, o equivalencias.py desconta o preço do opcional da BBB (reconferência r3, pares 8 e 11: Fábrica Spa
# Baby e Master Top incluem "aparelho de ozônio" e "kit Air Blower com 10 bicos"). Chave = a mesma que o
# equivalencias.py dá ao opcional da BBB. Filtro de cartucho não é o "kit filtragem completo (areia)" da BBB.
ITENS_EXTRA = (
    ("ozonio", r"(?:aparelho|gerador|sistema)?\s*(?:de\s+)?(?:ozonio|ozonizador|ozonizacao|ozone)"),
    ("airblower", r"air[\s-]?blower|blower|soprador(?:\s+de\s+ar)?"),
    ("filtragem", r"(?:kit\s+(?:de\s+)?)?filtr(?:o|agem|acao)[^.\n]{0,25}\bareia\b"),
    ("acionador", r"acionador\s+(?:digital|eletronico)"),
)


def itens_extra(nome, texto):
    """{item: True/False} dos ITENS_EXTRA que a página afirma (incluso de série ou opcional). Item não citado fica de fora.
    Filtro no nome ('Completa Com Aquecedor, Luzes, Filtro e Ozônio') conta como kit filtragem quando o texto do
    produto (fora da lista de opcionais) diz que a filtração é de areia (reconferência r4, Fábrica Copacabana)."""
    out = {}
    for k, rx in ITENS_EXTRA:
        f = _flag_item(nome, texto, rx)
        if f is not None:
            out[k] = f
    if out.get("filtragem") is None and re.search(r"(?:\bcom|\bc/|\+)\s[^|]{0,60}?\bfiltro\b", norm(nome)) \
            and re.search(r"filtr\w*[^.\n]{0,25}\bareia\b", norm(sem_opcionais(texto))):
        out["filtragem"] = True
    return out


def extrai_cromo_pontos(texto):
    """Pontos de cromoterapia que vêm com o produto ('3 pontos de cromoterapia', 'Kit Cromoterapia com 2 pontos em
    led', '1 ponto de LED', 'cromoterapia com 4 spots'). Lista de opcionais não entra; 'Cromoterapia 2 ou 4 spots'
    (escolha) não decide. Serve para descontar o opcional de cromo da BBB com o mesmo nº de pontos."""
    t = norm(sem_opcionais(texto))
    vals = []
    for m in re.finditer(r"\b(\d{1,2})\s+(?:pontos?|spots?|leds?)\s+(?:de\s+|em\s+)?(?:cromo\w*|led\w*|luz|ilumina\w*)", t):
        vals.append(int(m.group(1)))
    for m in re.finditer(r"cromo\w*[^.\n]{0,25}?\bcom\s+(\d{1,2})\s+(?:pontos?|spots?|leds?)\b(?!\s*ou)", t):
        vals.append(int(m.group(1)))
    # "01 Cromoterapia 2 pontos + Controlador" (Fábrica), "Cromoterapia 2 spots de led" (Splaz): reconferência r4
    for m in re.finditer(r"cromo\w*\s+(\d{1,2})\s+(?:pontos?|spots?|leds?)\b(?!\s*ou)", t):
        vals.append(int(m.group(1)))
    vals = [v for v in vals if 1 <= v <= 20]
    return vals[0] if vals and len(set(vals)) == 1 else (max(vals) if vals else None)


def extrai_instalacao(nome, texto):
    """Instalação inclusa no preço (Versátil: 'com aquecimento e instalação'). Não confunde com 'kit de instalação'
    (base/suporte), 'manual de instalação' ou 'pré-instalação'."""
    rx = r"(?<!kit de )(?<!kit )(?<!manual de )(?<!pre-)(?<!pre )(?<!guia de )instalacao"
    n = norm(nome)
    t = norm(texto)
    if re.search(r"(?:\bsem|\bs/)\s*" + rx, n) or re.search(r"(?:\bsem\s+" + rx + r"|" + rx + r"\s+nao\s+(?:inclusa|incluida|esta incluida))", t):
        return False
    if re.search(r"(?:\bcom|\bc/|\+|\be)\s+" + rx, n):
        return True
    if re.search(rx + r"\*?\s*(?:inclusa|incluida|gratis|gratuita)|(?:inclui(?:ndo)?|com)\s+" + rx + r"\b|" + rx + r"\*?\s*:\s*sim\b", t):
        return True
    return None


def extrai_forma(texto, so_rotulo=False):
    """Forma da banheira lida de atributo ('Formato: Quadrado', 'Forma: Redonda') ou da descrição ('freestanding').
    Devolve um rótulo que equivalencias.forma_de() entende. so_rotulo=True: só a forma com rótulo."""
    t = norm(texto)
    m = re.search(r"\bforma(?:to)?\s*:\s*([a-z ]{3,30})", t)
    alvo = m.group(1) if m else ("" if so_rotulo else t)
    for rx_, rot in ((r"vitorian", "Vitoriana"), (r"freestanding|free standing|de apoio\b|\bilha\b", "Freestanding"),
                     (r"octogon|oitavad", "Octogonal"), (r"sextavad|hexagon", "Sextavada"),
                     (r"quadrad", "Quadrada"), (r"redond|circular", "Redonda"), (r"\bde canto\b|\bcanto\b", "Canto"),
                     (r"\boval", "Oval"), (r"retangular", "Retangular")):
        if re.search(rx_, alvo):
            return rot
    return None


def forma_evidencia(texto):
    """Forma por evidência indireta (categoria própria do produto, tag, nome/alt da imagem): só quando o texto cita
    UMA forma ('product_cat-banheiras-de-canto-basicas', imagem 'spa-octogonal-4-lugares.jpg'). Duas formas
    diferentes (carrossel, categoria 'redondas e de canto') não decidem."""
    t = norm(re.sub(r"[-_/]+", " ", str(texto or "")))
    achadas = set()
    for rx_, rot in ((r"octogon|oitavad", "Octogonal"), (r"sextavad|hexagon", "Sextavada"), (r"quadrad", "Quadrada"),
                     (r"redond|circular", "Redonda"), (r"\bcanto\b", "Canto"), (r"\boval", "Oval"), (r"retangular", "Retangular")):
        if re.search(rx_, t):
            achadas.add(rot)
    return achadas.pop() if len(achadas) == 1 else None


def _classes_produto_html(h):
    """Categorias e tags do PRÓPRIO produto numa página WooCommerce (classes product_cat-*/product_tag-* do
    <div id="product-N">), não as dos carrosséis."""
    m = (re.search(r'<div[^>]*\bid="product-\d+"[^>]*\bclass="([^"]*)"', h or "")
         or re.search(r'<div[^>]*\bclass="([^"]*)"[^>]*\bid="product-\d+"', h or ""))
    return " ".join(re.findall(r"\bproduct_(?:cat|tag)-([\w-]+)", m.group(1))) if m else ""


def extrai_travesseiros(texto):
    """Nº de travesseiros/apoios de cabeça que acompanham ('8 TRAVESSEIROS (PRETO) APOIO DE CABEÇA', '07 Travesseiros
    anatômicos'). Lista de opcionais não entra. Só um valor (ou o mesmo repetido) decide."""
    t = norm(sem_opcionais(texto))
    vals = {int(m.group(1)) for m in re.finditer(r"\b(\d{1,2})\s+(?:travesseiros?|apoios? de cabeca|encostos? de cabeca)\b", t)}
    vals = {v for v in vals if 1 <= v <= 12}
    return vals.pop() if len(vals) == 1 else None


def extrai_deck(nome, texto):
    """Deck incluso no preço: 'com deck' no nome/variante = True; 'sem deck' / 'não acompanha deck' = False;
    'Acabamento: deck incluso' = True. O resto fica None."""
    n, t = norm(nome), norm(sem_opcionais(texto))
    if re.search(r"(?:\bsem|\bs/)\s*deck\b", n) or re.search(r"\bnao acompanha\s+(?:o\s+)?deck\b|\bsem deck\b|deck[^.\n]{0,20}(?:nao incluso|vendido separad|opcional)", t):
        return False
    if re.search(r"(?:\bcom|\bc/|\+)\s*(?:o\s+)?deck\b", n) or re.search(r"deck\s*(?:de madeira\s*)?(?:incluso|incluido)|acompanha\s+(?:o\s+)?deck\b", t):
        return True
    return None


def extrai_garantia(texto):
    t = norm(texto)
    cand = []
    for m in re.finditer(r"garantia[^.\n]{0,80}?" + _NUM + r"\s*anos?", t):
        cand.append((("casco" in m.group(0)) or ("estrutura" in m.group(0)), _n(m.group(1))))
    for m in re.finditer(_NUM + r"\s*anos?\s+de\s+garantia([^.\n]{0,40})", t):
        cand.append(("casco" in m.group(0) or "estrutura" in m.group(0), _n(m.group(1))))
    cand = [(c, v) for c, v in cand if v and 0 < v <= 30]
    if not cand:
        return None
    casco = [v for c, v in cand if c]
    return float(casco[0] if casco else cand[0][1])


_RX_POR_VERSAO = re.compile(
    r"(\d{1,2})\s+((?:mini[\s-]?)?jat\w*|travesseiros?|motores|motobombas?|pontos?\s+de\s+\w+)([^.\n\d]{0,40}?)"
    r"\s+n[ao]s?\s+vers(?:[aã]o|[oõ]es)\s+([a-z]\w*)"
    r"((?:\s*(?:,|\be\b)\s*\d{1,2}\s+n[ao]\s+(?:vers[aã]o\s+)?[a-z]\w*)+)?", re.I)


_VERSOES = r"basic[ao]?|standar[dt]|premium|plus|luxo|master|slim|essencial|conforto|top|completa?|simples"


def texto_da_versao(texto, variante):
    """Descrição que dá a quantidade por versão ('4 jatos e 3 mini-jatos na versão standard e 6 na premium'):
    deixa no texto só a quantidade da versão escolhida (Basic: sem mini-jatos; Standard: 3; Premium: 6).
    A versão vem do valor da variante ('Tipo: Basic'). Sem frase por versão, o texto volta igual."""
    if not texto or not variante:
        return texto
    vals = set()
    for x in re.split(r"\s*\|\s*", variante):
        k, _, v = x.rpartition(":")
        v = norm(v)
        if re.search(r"tipo|vers|model|nivel|linha|configura", norm(k)) or re.fullmatch(_VERSOES, v):
            vals.add(v)
    if not vals:
        return texto

    def troca(m):
        por = {norm(m.group(4)): m.group(1)}
        for q, v in re.findall(r"(\d{1,2})\s+n[ao]\s+(?:vers[aã]o\s+)?([a-z]\w*)", m.group(5) or "", re.I):
            por[norm(v)] = q
        alvo = next((por[v] for v in por if v in vals), None)
        if alvo is None:
            # versão escolhida não está na frase: ela não tem o item (Basic, quando só Standard e Premium têm)
            return " "
        return "%s %s%s" % (alvo, m.group(2), m.group(3))
    return _RX_POR_VERSAO.sub(troca, texto)


_RX_INCLUSOS = re.compile(r"(?:itens\s+inclusos|acessorios|acompanha|conteudo\s+incluso|inclui)\s*:?\s*([^\n]{3,400})")


def jatos_declarados(nome, texto, jt, mt):
    """Quantidade de jatos que o título ou a lista "Itens Inclusos" declara vence a soma da descrição genérica, quando
    a soma só repete o mesmo número em frases diferentes (Telhanorte Lebloncril: "4 Jatos" no título, nos itens
    inclusos, em "equipada com 4 jatos" e em "os 4 jatos de hidromassagem" somava 12; reconferência r3, par 24).
    Só troca quando a soma é múltiplo do declarado e o declarado aparece no texto; o total com detalhamento
    ("10 jatos (4 grandes e 6 mini)") continua valendo."""
    if not jt:
        return jt, mt
    decl = []
    jn, mn = extrai_jatos(nome)
    # total declarado no nome vence a soma quando a soma passa dele (Fábrica "Spa Quadrado 30 jatos": o título, o
    # resumo "30 jatos em aço inoxidável" e a lista "20 Mini Jatos; 10 Jatos Turbo" somavam 70; reconferência r4
    # preços #18). Se a lista detalhada (sem as menções do total) fecha com o total, vale a lista (10 + 20 mini).
    if jn and not mn and jt > jn:
        resto = re.sub(r"(?i)\b0?%d\s*(?:hidro)?jat(?:o|õe|oe)s?\b" % jn, " ", str(texto or ""))
        jl, ml = extrai_jatos(resto)
        if jl and (jl + (ml or 0)) == jn:
            return jl, ml
        return jn, None
    if jn:
        decl.append((jn, mn))
    for m in _RX_INCLUSOS.finditer(norm(sem_opcionais(texto))):
        ji, mi = extrai_jatos(m.group(1))
        if ji:
            decl.append((ji, mi))
    for jd, md in decl:
        if jd + (md or 0) == jt + (mt or 0) or jd == jt:
            return jt, mt  # o texto já bate com o declarado
        if jt > jd and jt % jd == 0 and re.search(r"\b0?%d\s*%s" % (jd, _JATO), norm(sem_opcionais(texto))):
            return jd, (md if md is not None else mt)
    return jt, mt


def atributos(nome, variante="", texto="", base=None):
    """Extrai atributos de UMA variante. Medidas/lugares da variante têm prioridade; texto é só o bloco do produto."""
    base = dict(base or {})
    out = {}
    alvo_var = (variante or "")
    alvo_nome = nome or ""
    # medidas: variante > nome > texto (só se o texto tiver um único conjunto de medidas)
    md = extrai_medidas(alvo_var) or extrai_medidas(alvo_nome)
    if not md:
        mt = list(dict.fromkeys(extrai_medidas(texto)))
        if len(set((a, b) for a, b, _ in mt)) == 1:
            md = mt
    if md and md[0][1] is None:
        # "150 x 40 cm" (comprimento x profundidade, sem largura): completa a largura com a do texto quando o texto
        # traz o mesmo comprimento com largura
        for a_, b_, c_ in extrai_medidas(texto):
            if b_ is not None and abs(a_ - md[0][0]) <= 0.02:
                md = [(md[0][0], b_, md[0][2] if md[0][2] is not None else c_)]
                break
    if md and md[0][2] is None and md[0][1] is not None:
        # nome/variante sem profundidade: completa com a do texto quando o texto traz a mesma medida (C x L)
        for a_, b_, c_ in extrai_medidas(texto):
            if c_ is not None and b_ is not None and abs(a_ - md[0][0]) <= 0.02 and abs(b_ - md[0][1]) <= 0.02:
                md = [(md[0][0], md[0][1], c_)]
                break
    if md:
        out["comprimento_m"], out["largura_m"], out["profundidade_m"] = md[0]
    out["lugares"] = extrai_lugares(alvo_var, False) or extrai_lugares(alvo_nome) or extrai_lugares(texto, False)
    out["material"] = extrai_material(alvo_var) or extrai_material(alvo_nome) or extrai_material(texto)
    jv, mv = extrai_jatos(alvo_var)
    jt, mt_ = jatos_declarados(alvo_nome, texto, *extrai_jatos(texto))
    if jv is not None or mv is not None:  # a variante diz a configuração: não mistura com o texto das outras
        out["jatos"], out["mini_jatos"] = jv, mv
    else:
        out["jatos"], out["mini_jatos"] = jt, mt_
    out["motores"] = extrai_motores(alvo_var) or extrai_motores(texto)
    out["potencia_cv"] = extrai_cv(alvo_var) or extrai_cv(texto)
    # "01 motobomba de 1cv a cada 10 jatos": o nº de motores segue os jatos da variante (reconferência r4)
    prop = None if extrai_motores(alvo_var) else motores_por_proporcao(texto, (out["jatos"] or 0) + (out["mini_jatos"] or 0))
    if prop:
        out["motores"] = prop[0]
        if prop[1] and not extrai_cv(alvo_var):
            out["potencia_cv"] = prop[1]
    nv = alvo_nome + " " + alvo_var
    out["aquecedor_incluso"] = _flag_item(nv, texto, r"aquecedor(?:es)?|aquecimento")
    out["cromo_incluso"] = _flag_item(nv, texto, r"cromoterapia|cromo|leds?\b|iluminacao")
    out["garantia_casco_anos"] = extrai_garantia(texto)
    n = norm(nv)
    # "Sem hidro" numa linha própria da ficha (Personalizze Rosalia Jacuzzi, freestanding de imersão; r4 preços #23)
    sem_hidro_ficha = re.search(r"(?im)^[ \t]*(?:hidromassagem\s*:\s*n[aã]o|sem\s+hidro(?:massagem)?)[ \t.]*$", str(texto or ""))
    if re.search(r"sem hidro|sem hidromassagem", n) or (re.search(r"imersao|vitoriana", n) and not re.search(r"hidro", n)):
        out["com_hidro"] = False if not (out["jatos"] or out["mini_jatos"]) else True
    elif sem_hidro_ficha and not (out["jatos"] or out["mini_jatos"] or out["motores"]):
        out["com_hidro"] = False
    elif re.search(r"hidro|jacuzzi|\bc/ ?h\b", n) or out["jatos"] or out["mini_jatos"] or (out["motores"] or 0) > 0:
        out["com_hidro"] = True
    else:
        out["com_hidro"] = None
    for k, v in base.items():  # valores estruturados da fonte vencem o texto
        if v is not None:
            out[k] = v
    return out


def classifica(nome, extra=""):
    n = norm(nome)
    e = norm(extra)
    if re.match(r"\s*(par |pre.?filtro|valvula|aquecedor|acionador|jato|kit |cromoterapia|air ?blower|cascata|filtro|capa|tampa|"
                r"motor|moto ?bomba|ralo|encosto|almofada|travesseiro|apoio|suporte|dreno|misturador|escada|alca|sensor|painel|"
                r"ozonio|ozonizador|bomba|refil|combo de acess|controlador|teclado|clorador|gerador|cobertura|degrau|"
                r"puff|chaise|toalheiro|torneira|metais|instalacao|frete|servico)", n):
        return "acessorio"
    # metais, peças e insumos que citam "banheira"/"spa" no nome (bica, dispositivo, pé de banheira...) não são banheira
    if (not re.match(r"\s*(banheira|spa|ofuro|mini ?spa|hidro|sauna|jacuzzi)", n)
            and re.search(r"\b(bica|monocomando|misturador|registro|torneira|ducha|chuveiro|desviador|sifao|valvula|"
                          r"acabamento|base para|base monocomando|bocal|escoamento|cuba|vaso|assento|dispositivo|succao|"
                          r"arejador|spot|canhao|mini ?jatos?|jatos? de|pes? de banheira|pes? para banheira|bandeja|etiqueta|"
                          r"botao|cartucho|conjunto de|acoplamento|pega.?mao|alcas?|espuma|sais|sabonete|oleo|essencia|"
                          r"retorno|aspiracao|nivel|refil|capa termica|tampa termica|cobertura|limpador|clarificante|cloro|"
                          r"pastilha|termometro|lampada|led\b|placa|rele|fusivel|pressostato|resistencia|kit)\b", n)):
        return "acessorio"
    if re.search(r"\bsauna", n):
        return "sauna"
    if re.search(r"\bpets?\b|cachorro|caes\b|\bdog", n):
        return "pet"
    if re.search(r"infantil|\bkids?\b|bebe|crianca|bercario", n):
        return "infantil"
    if re.search(r"ofuro", n):
        return "ofuro"
    if re.search(r"\b(mini ?spa|minispa|spa|spas|swim ?spa|hot ?tub)\b", n):
        return "spa"
    if re.search(r"banheira|hidromassagem|imersao|jacuzzi|vitoriana", n):
        return "banheira"
    if re.search(r"acessori|aquecedor|acionador|cromo|blower|cascata|filtro|capa|tampa|motobomba", n + " " + e):
        return "acessorio"
    if re.search(r"ofuro", e):
        return "ofuro"
    if re.search(r"\bspas?\b", e):
        return "spa"
    if re.search(r"sauna", e):
        return "sauna"
    if re.search(r"banheira|hidromassagem", e):
        return "banheira"
    return "outro"


# ---------------------------------------------------------------------------
# robots.txt (grupo "*", com curingas * e $) e cliente HTTP educado
# ---------------------------------------------------------------------------
class Robots(object):
    def __init__(self, texto):
        self.regras = []
        self.delay = None
        grupos, atual, em_regras = [], None, False
        for linha in (texto or "").splitlines():
            linha = linha.split("#", 1)[0].strip()
            if ":" not in linha:
                continue
            k, v = [x.strip() for x in linha.split(":", 1)]
            k = k.lower()
            if k == "user-agent":
                if atual is None or em_regras:
                    atual = {"uas": [], "regras": [], "delay": None}
                    grupos.append(atual)
                    em_regras = False
                atual["uas"].append(v.lower())
            elif atual is not None and k in ("allow", "disallow"):
                em_regras = True
                if v:
                    atual["regras"].append((k == "allow", v))
            elif atual is not None and k == "crawl-delay":
                em_regras = True
                try:
                    atual["delay"] = float(v)
                except ValueError:
                    pass
        for g in grupos:
            if "*" in g["uas"]:
                self.regras += g["regras"]
                if g["delay"] is not None:
                    self.delay = g["delay"]

    @staticmethod
    def _rx(p):
        r = re.escape(p).replace(r"\*", ".*")
        if r.endswith(r"\$"):
            r = r[:-2] + "$"
        return re.compile("^" + r)

    def permitido(self, url):
        u = urlparse(url)
        caminho = (u.path or "/") + (("?" + u.query) if u.query else "")
        melhor = None
        for allow, p in self.regras:
            if self._rx(p).match(caminho):
                tam = len(p)
                if melhor is None or tam > melhor[0] or (tam == melhor[0] and allow):
                    melhor = (tam, allow)
        return True if melhor is None else melhor[1]


class Bloqueado(Exception):
    pass


class Corte(Exception):
    pass


class Fonte(object):
    """Uma sessão por concorrente (1 conexão por domínio), pausa por host e robots.txt."""

    def __init__(self, cfg, prazo):
        self.cfg = cfg
        self.prazo = prazo
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": UA, "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.6",
                               "Accept": "text/html,application/json,application/xhtml+xml,*/*;q=0.8"})
        self.delay = float((cfg.get("params") or {}).get("delay", 1.0))
        self.ultimo = {}
        self.robots = {}
        self.n_req = 0
        self.notas = []
        self.n_bloq = 0

    def robots_de(self, url):
        u = urlparse(url)
        host = u.scheme + "://" + u.netloc
        if host in self.robots:
            return self.robots[host]
        try:
            r = self.s.get(host + "/robots.txt", timeout=25)
            self.n_req += 1
            if r.status_code in (401, 403, 429) or (r.status_code >= 500):
                rb = "BLOQ:%s" % r.status_code
            elif r.status_code == 200 and "html" not in r.headers.get("content-type", ""):
                rb = Robots(r.text)
            else:
                rb = Robots("")
        except requests.RequestException as e:
            rb = Robots("")
            self.notas.append("robots.txt indisponível em %s (%s)" % (host, type(e).__name__))
        self.robots[host] = rb
        return rb

    def get(self, url, method="GET", json_body=None, params=None, headers=None, tolerar=(), stream_bytes=False):
        if time.time() > self.prazo:
            raise Corte("orçamento de tempo esgotado")
        rb = self.robots_de(url)
        if isinstance(rb, str):
            raise Bloqueado("robots.txt respondeu %s" % rb.split(":")[1])
        full = url if not params else url + ("&" if "?" in url else "?") + urlencode(params)
        if not rb.permitido(full):
            raise Bloqueado("robots.txt proíbe %s" % urlparse(full).path)
        host = urlparse(url).netloc
        pausa = max(self.delay, rb.delay or 0)
        espera = self.ultimo.get(host, 0) + pausa - time.time()
        if espera > 0:
            time.sleep(espera)
        for tentativa in range(2):
            try:
                r = self.s.request(method, full, json=json_body, headers=headers, timeout=45)
            except requests.RequestException as e:
                self.ultimo[host] = time.time()
                if tentativa == 0:
                    time.sleep(3)
                    continue
                raise
            self.ultimo[host] = time.time()
            self.n_req += 1
            if r.status_code == 429 and tentativa == 0:
                ra = r.headers.get("Retry-After")
                w = min(float(ra) if ra and ra.isdigit() else 30.0, 60.0)
                if time.time() + w > self.prazo:
                    raise Corte("429 sem tempo para esperar")
                time.sleep(w)
                continue
            break
        if r.status_code in tolerar:
            return r
        if r.status_code in (401, 403, 429) or (r.status_code == 503 and "captcha" in r.text.lower()):
            self.n_bloq += 1
            raise Bloqueado("HTTP %s em %s" % (r.status_code, urlparse(full).path[:80]))
        if r.status_code >= 400:
            raise requests.HTTPError("HTTP %s em %s" % (r.status_code, full[:120]))
        return r

    def texto(self, url, **kw):
        # Accept só HTML: Vnda (e outras) devolvem JSON resumido quando o Accept inclui application/json
        kw.setdefault("headers", {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"})
        r = self.get(url, **kw)
        if r.encoding is None or r.encoding.lower() == "iso-8859-1":
            r.encoding = r.apparent_encoding or "utf-8"
        return r.text

    def json(self, url, **kw):
        kw.setdefault("headers", {"Accept": "application/json, text/plain, */*"})
        return self.get(url, **kw).json()


# ---------------------------------------------------------------------------
# Montagem das linhas
# ---------------------------------------------------------------------------
COLS = ["data_coleta", "coletado_em", "empresa", "empresa_slug", "canal", "dominio", "produto_id", "sku", "variante", "nome", "url",
        "categoria", "preco_de", "preco_por", "preco_pix", "parcelas_sem_juros", "disponivel", "lugares", "comprimento_m",
        "largura_m", "profundidade_m", "material", "com_hidro", "jatos", "mini_jatos", "motores", "potencia_cv",
        "aquecedor_incluso", "cromo_incluso", "garantia_casco_anos", "atributos_json", "metodo", "status_preco",
        "motivo_status", "fonte_id"]
ATTR_KEYS = ["lugares", "comprimento_m", "largura_m", "profundidade_m", "material", "com_hidro", "jatos", "mini_jatos",
             "motores", "potencia_cv", "aquecedor_incluso", "cromo_incluso", "garantia_casco_anos"]


class Coletor(object):
    def __init__(self, cfg, fonte, data_coleta):
        self.cfg = cfg
        self.f = fonte
        self.data = data_coleta
        self.linhas = []
        self.cats = set(cfg.get("cats") or TODAS)
        self.p = cfg.get("params") or {}
        self.vistos = set()

    def add(self, produto_id, nome, url, preco_por, preco_de=None, preco_pix=None, parcelas=None, disponivel=None,
            sku=None, variante="", texto="", attrs_base=None, extra=None, categoria=None, cat_extra="", dominio=None,
            metodo=None, de_igual_ok=False):
        """de_igual_ok: grava o "De" mesmo quando é igual ao preço por (Mérito: "De R$ 18.034,94 Por R$ 16.231,45 (Pix)
        · ou R$ 18.034,94 em 12x": o por passa a ser o preço de cartão, igual ao De que a página mostra)."""
        if self.p.get("so_nome"):
            cat_extra = ""  # varejista: categoria da loja mistura itens; decide só pelo nome
        chave = (str(produto_id), str(sku), norm(variante))
        if chave in self.vistos:
            return False
        self.vistos.add(chave)
        if self.p.get("nome_regex") and not re.search(self.p["nome_regex"], norm(nome)):
            return False
        cat = categoria or classifica(nome, cat_extra)
        if self.p.get("categoria_fixa") and cat != "acessorio":
            cat = self.p["categoria_fixa"]  # loja de nicho (ex.: banheira pet), o nome nem sempre diz
        if cat not in self.cats:
            return False
        nome = re.sub(r"\s+", " ", html.unescape(str(nome or ""))).strip()
        variante = re.sub(r"\s+", " ", html.unescape(str(variante or ""))).strip()
        texto = texto_da_versao(texto, variante)
        ex0 = {}
        # variante "monte o seu" (Splaz "Quantidade de hidromassagem: Personalize"): preço de entrada de uma configuração
        # aberta; não herda jatos/motores/itens do texto (que descreve as outras configurações)
        val_var = [x.split(":", 1)[-1] for x in re.split(r"\s*\|\s*", variante)]
        if any(re.match(r"\s*(personaliz|sob medida|monte (o|a) s|consulte|a combinar)", norm(x)) for x in val_var):
            ex0["variante_personalizada"] = True
            a = atributos(nome, variante, "", attrs_base)
            a0 = atributos(nome, variante, texto, attrs_base)
            for k in ("lugares", "comprimento_m", "largura_m", "profundidade_m", "material", "garantia_casco_anos"):
                if a.get(k) is None:
                    a[k] = a0.get(k)
        else:
            a = atributos(nome, variante, texto, attrs_base)
        ex_in = extra or {}
        # lugares que a ficha não diz: nome da imagem do produto ("SIZE 8 PESSOAS") e, na Gold/Aurum, o nº de
        # travesseiros (apoios de cabeça) da lista do que acompanha (Isla e Ibiza: 8 travesseiros = 8 lugares)
        if a.get("lugares") is None and cat in ("spa", "ofuro", "banheira"):
            if ex_in.get("lugares_imagem"):
                a["lugares"] = ex_in["lugares_imagem"]
                ex0["lugares_origem"] = "nome da imagem do produto"
            elif self.p.get("lugares_por_travesseiro"):
                tv = extrai_travesseiros(texto)
                if tv:
                    a["lugares"] = tv
                    ex0["lugares_origem"] = "nº de travesseiros (apoios de cabeça) do que acompanha"
        dk = extrai_deck(nome + " " + variante, texto)
        if dk is not None and ex_in.get("deck_incluso") is None:
            ex0["deck_incluso"] = dk
        if not ex_in.get("forma"):
            fm = (extrai_forma(nome + " " + variante) or extrai_forma(texto, so_rotulo=True)
                  or ("Freestanding" if re.search(r"\bfree ?standing\b", norm(texto)) else None))
            if fm:
                ex0["forma"] = fm
        if a.get("cromo_incluso") is True and ex_in.get("cromo_pontos") is None:
            cp = extrai_cromo_pontos(nome + " " + variante + "\n" + texto)
            if cp:
                ex0["cromo_pontos"] = cp
        if cat in CATS_PRINCIPAIS and not ex_in.get("itens_inclusos"):
            ie = itens_extra(nome + " " + variante, texto)
            if ie:
                ex0["itens_inclusos"] = ie
        if ex_in.get("instalacao_inclusa") is None:
            inst = extrai_instalacao(nome + " " + variante, texto)
            if inst is not None:
                ex0["instalacao_inclusa"] = inst
        if a.get("garantia_casco_anos") is None and self.p.get("garantia_padrao") and cat in CATS_PRINCIPAIS:
            a["garantia_casco_anos"] = float(self.p["garantia_padrao"])
            ex0["garantia_fonte"] = "regra geral da loja (FAQ), não da ficha do produto"
        pp = fnum(preco_por)
        pp = pp if pp and pp > 0 else None
        pd_ = fnum(preco_de)
        pd_ = pd_ if pd_ and pp and (pd_ > pp + 0.009 or (de_igual_ok and pd_ >= pp - 0.009)) else None
        px = fnum(preco_pix)
        px = px if px and px > 0 else None
        ex = dict(extra or {})
        ex.update(ex0)
        ex = {k: v for k, v in ex.items() if v not in (None, "", [], {})}
        if a.get("motores") and a.get("potencia_cv"):
            ex.setdefault("potencia_total_cv_estimada", round(a["motores"] * a["potencia_cv"], 2))
        row = dict.fromkeys(COLS)
        row.update(dict(
            data_coleta=self.data, coletado_em=agora().isoformat(timespec="seconds"), empresa=self.cfg["empresa"],
            empresa_slug=self.cfg["empresa_slug"], canal=self.cfg.get("canal", "site"),
            dominio=dominio or urlparse(url or self.cfg.get("endpoint") or "").netloc.replace("www.", ""),
            produto_id=str(produto_id) if produto_id not in (None, "") else None, sku=str(sku) if sku not in (None, "") else None,
            variante=variante, nome=nome, url=url, categoria=cat, preco_de=pd_, preco_por=pp, preco_pix=px,
            parcelas_sem_juros=int(parcelas) if parcelas not in (None, "") and str(parcelas).isdigit() else None,
            disponivel=disponivel if disponivel in (True, False) else None,
            atributos_json=json.dumps(ex, ensure_ascii=False, default=str) if ex else "{}",
            metodo=metodo or self.cfg["metodo"], fonte_id=self.cfg["fonte_id"]))
        for k in ATTR_KEYS:
            row[k] = a.get(k)
        self.linhas.append(row)
        return True


    def finaliza(self):
        """Junta variantes que só diferem por voltagem/cor quando o preço é o mesmo (não mudam o preço)."""
        rx = re.compile(r"(?i)(?:\b(?:selecione a )?(?:voltagem|votlagem|tensao|tensão|cor)\s*:?\s*[^|,;]*)|\b(?:110|115|127|220|230|bivolt)\s*v\b|\bbivolt\b")
        grupos, ordem = {}, []
        for r in self.linhas:
            limpa = re.sub(r"\s*(\|\s*|,\s*)+$", "", re.sub(r"^(\s*(\||,)\s*)+", "", rx.sub("", r["variante"] or ""))).strip(" |,-")
            limpa = re.sub(r"\s*\|\s*\|\s*", " | ", re.sub(r"\s*/\s*/\s*", " / ", limpa)).strip(" /")
            k = (r["produto_id"], norm(limpa), r["preco_por"], r["preco_de"], r["url"])
            if k not in grupos:
                grupos[k] = []
                ordem.append(k)
            grupos[k].append((r, limpa))
        out = []
        for k in ordem:
            g = grupos[k]
            r, limpa = g[0]
            if len(g) > 1:
                r = dict(r)
                r["variante"] = limpa
                ex = json.loads(r["atributos_json"] or "{}")
                ex["variantes_agrupadas"] = sorted(set(x[0]["variante"] for x in g))[:12]
                r["atributos_json"] = json.dumps(ex, ensure_ascii=False, default=str)
                r["disponivel"] = any(x[0]["disponivel"] for x in g) if any(x[0]["disponivel"] is not None for x in g) else None
            out.append(r)
        self.linhas = out


# ---------------------------------------------------------------------------
# Adaptadores
# ---------------------------------------------------------------------------
def sitemap_urls(c, url, rx=None, profundidade=0):
    r = c.f.get(url)
    raw = r.content
    if url.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        try:
            raw = gzip.decompress(raw)
        except OSError:
            pass
    txt = raw.decode("utf-8", "replace")
    locs = [html.unescape(x.strip()) for x in re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", txt)]
    if "<sitemapindex" in txt and profundidade < 2:
        out = []
        for sm in locs:
            if rx and not re.search(r"product|produto|sitemap", sm, re.I):
                continue
            try:
                out += sitemap_urls(c, sm, rx, profundidade + 1)
            except (Bloqueado, requests.RequestException):
                continue
        return out
    if rx:
        locs = [u for u in locs if re.search(rx, u, re.I)]
    return list(dict.fromkeys(locs))


# ---- 1) Magento 2 GraphQL (BBB) ----
GQL_ATTRS = ["lugares", "configurac_o", "forma", "material", "comprimento", "largura", "profundidade"]
GQL_OPT = "value{title price price_type}"
GQL_Q = ('{products(search:"",pageSize:%d,currentPage:%d){total_count page_info{total_pages} items{__typename sku name url_key '
         'stock_status lugares configurac_o forma material comprimento largura profundidade short_description{html} '
         'categories{name url_path level} price_range{minimum_price{regular_price{value} final_price{value}}} '
         '... on CustomizableProductInterface{options{title required ... on CustomizableCheckboxOption{%s} '
         '... on CustomizableRadioOption{%s} ... on CustomizableDropDownOption{%s} ... on CustomizableMultipleOption{%s}}}}}}')


def ad_magento2_graphql(c):
    ep = c.cfg["endpoint"]
    site = ep.rsplit("/graphql", 1)[0].rstrip("/")
    meta_q = "{customAttributeMetadata(attributes:[%s]){items{attribute_code attribute_options{label value}}}}" % ",".join(
        '{attribute_code:"%s",entity_type:"4"}' % a for a in GQL_ATTRS)
    rotulos = {}
    try:
        d = c.f.json(ep, method="POST", json_body={"query": meta_q})
        for it in d["data"]["customAttributeMetadata"]["items"]:
            rotulos[it["attribute_code"]] = {o["value"]: o["label"] for o in it.get("attribute_options") or []}
    except Exception as e:
        c.f.notas.append("rótulos de atributos indisponíveis: %s" % e)

    def rot(cod, v):
        if v in (None, ""):
            return None
        ids = str(v).split(",")
        labs = [rotulos.get(cod, {}).get(i.strip(), i.strip()) for i in ids]
        return ", ".join(labs)

    pagina, total_pag, n_prod = 1, 1, 0
    while pagina <= total_pag:
        q = GQL_Q % ((50, pagina) + (GQL_OPT,) * 4)
        d = c.f.json(ep, method="POST", json_body={"query": q})
        if d.get("errors") and not d.get("data"):
            raise RuntimeError("GraphQL: %s" % d["errors"][:1])
        pr = d["data"]["products"]
        total_pag = pr["page_info"]["total_pages"]
        for p in pr["items"]:
            n_prod += 1
            fin = fnum(p["price_range"]["minimum_price"]["final_price"]["value"])
            reg = fnum(p["price_range"]["minimum_price"]["regular_price"]["value"])
            sd = html_txt((p.get("short_description") or {}).get("html") or "")
            cats = " ".join(x["name"] for x in p.get("categories") or [])
            lug = rot("lugares", p.get("lugares"))
            mat = rot("material", p.get("material"))
            conf = rot("configurac_o", p.get("configurac_o"))
            forma = rot("forma", p.get("forma"))
            dims_attr = [rot(k, p.get(k)) for k in ("comprimento", "largura", "profundidade")]
            base = {}
            ln = extrai_lugares(lug or "") if lug else None
            if ln:
                base["lugares"] = ln
            mat_ficha = extrai_material(mat) if mat else None
            # nome e descrição vencem a ficha "Mais informações" quando conflitam (Acqua Spaço "em Acrílico",
            # "Produzida em acrílico", com a ficha "Material: Gel Coat": reconferência r3, preços #2)
            # (só acrílico x gel coat: "outro"/"fibra" lidos de frase solta da descrição não vencem a ficha)
            mat_txt = next((m for m in (extrai_material(p["name"]), extrai_material(sd)) if m in ("acrilico", "gel coat")), None)
            if mat_txt:
                base["material"] = mat_txt
            elif mat_ficha:
                base["material"] = mat_ficha
            opcionais, eixos = [], []
            for o in p.get("options") or []:
                tit = (o.get("title") or "").strip().rstrip(":").strip()
                vals = o.get("value") or []
                if not isinstance(vals, list):
                    vals = [vals]
                eh_tam = norm(tit) in ("tamanho", "medida", "medidas", "tamanhos")
                tem_preco = any(fnum(v.get("price")) for v in vals)
                if norm(tit) in ("opcoes adicionais", "acessorios", "opcionais") or not o.get("required"):
                    for v in vals:
                        if fnum(v.get("price")):
                            opcionais.append({"grupo": tit, "item": (v.get("title") or "").strip(), "preco": fnum(v.get("price")),
                                              "tipo": v.get("price_type")})
                    continue
                if eh_tam or tem_preco:
                    eixos.append((tit, vals))
            itens_opc = norm(" ".join(o["item"] for o in opcionais))
            if "aquecedor" in itens_opc:
                base["aquecedor_incluso"] = False
            if "cromo" in itens_opc:
                base["cromo_incluso"] = False
            extra = {"configuracao": conf, "forma": forma, "categorias_loja": cats[:300], "regra_preco": "Preço Especial (final_price)",
                     "opcionais": opcionais or None,
                     "medidas_cadastro": " x ".join(x for x in dims_attr if x) or None,
                     "material_ficha_diverge": mat if (mat_txt and mat_ficha and mat_txt != mat_ficha) else None}
            url = "%s/%s.html" % (site, p["url_key"]) if p.get("url_key") else None
            disp = p.get("stock_status") == "IN_STOCK"
            texto = p["name"] + "\n" + sd
            # combinações das opções que mudam preço/medida (cada combinação = 1 linha)
            combos = [([], 0.0, 0.0)]
            for tit, vals in eixos:
                novos = []
                for pre, add_f, add_r in combos:
                    for v in vals:
                        pv = fnum(v.get("price")) or 0.0
                        if v.get("price_type") == "PERCENT":
                            af, ar = (fin or 0) * pv / 100.0, (reg or 0) * pv / 100.0
                        else:
                            af, ar = pv, pv
                        novos.append((pre + ["%s: %s" % (tit, (v.get("title") or "").strip())], add_f + af, add_r + ar))
                combos = novos[:48]
            for partes, af, ar in combos:
                variante = " | ".join(partes)
                ex = dict(extra)
                if partes:
                    ex["acrescimo_opcao"] = round(af, 2)
                c.add(p["sku"], p["name"], url, (fin + af) if fin else None, (reg + ar) if reg else None, None, None, disp,
                      sku=p["sku"], variante=variante, texto=texto, attrs_base=base, extra=ex, cat_extra=cats)
        pagina += 1
    return n_prod


# ---- 2) Magento 1 HTML (Só Banheiras) ----
def ad_magento1_html(c):
    urls = sitemap_urls(c, c.cfg["endpoint"], r"\.html$")
    urls = [u for u in urls if urlparse(u).path.count("/") == 1]  # produto fica na raiz: /slug.html
    lim = int(c.p.get("max_paginas", 400))
    if len(urls) > lim:
        c.f.notas.append("corte: %d de %d páginas" % (lim, len(urls)))
        urls = urls[:lim]
    n = 0
    for u in urls:
        try:
            h = c.f.texto(u)
        except requests.HTTPError:
            continue
        m = re.search(r"Product\.OptionsPrice\((\{.*?\})\);", h, re.S)
        op = None
        if m:
            try:
                op = json.loads(m.group(1))
            except ValueError:
                op = None
        mn = re.search(r'"product":\{"id":"(\d+)","sku":"([^"]*)","name":"([^"]*)"', h)
        if op is None:
            # Product.OptionsPrice([]) (produto sem opções, ex.: pronta entrega): preço do bloco do produto, pelo id
            # (os relacionados usam "<id>-related"); só vale com o <h1> do produto na página
            if not (mn and re.search(r"Product\.OptionsPrice\(\[\]\)", h)
                    and re.search(r'(?is)<div class="product-name">\s*(?:<!--.*?-->\s*)?<h1', h)):
                continue  # não é página de produto
            pidb = mn.group(1)
            mpp = re.search(r'id="product-price-%s"[^>]*>(?:\s*<span[^>]*>)?\s*([^<]+)<' % pidb, h)
            mpo = re.search(r'id="old-price-%s"[^>]*>(?:\s*<span[^>]*>)?\s*([^<]+)<' % pidb, h)
            op = {"productPrice": brl(mpp.group(1)) if mpp else None, "productOldPrice": brl(mpo.group(1)) if mpo else None,
                  "productId": pidb}
            if op["productPrice"] is None:
                continue
        n += 1
        preco = fnum(op.get("productPrice"))
        antigo = fnum(op.get("productOldPrice"))
        nome = None
        pid, sku = (mn.group(1), mn.group(2)) if mn else (op.get("productId"), None)
        # disponibilidade: schema.org do produto (1º itemprop="availability"); "Esgotado"/out-of-stock no bloco do produto
        mav = re.search(r'itemprop="availability"[^>]*href="[^"]*/(InStock|OutOfStock|PreOrder|SoldOut)"', h)
        disp = None
        if mav:
            disp = mav.group(1) in ("InStock", "PreOrder")
        if re.search(r'(?is)<p class="availability out-of-stock"', h):
            disp = False
        mt = (re.search(r'(?is)<div class="product-name">\s*(?:<!--.*?-->\s*)?<h1[^>]*>(.*?)</h1>', h)
              or re.search(r"(?is)<h1[^>]*>(.*?)</h1>", h))
        nome = html_txt(mt.group(1)) if mt else (html.unescape(mn.group(3)) if mn else u)
        bloco = bloco_produto_html(h, nome)
        # opções personalizadas: <dt><label>Título</label></dt><dd> ... <select>/<input price=...>
        eixos, opcionais = [], []
        for dt, dd in re.findall(r"(?is)<dt[^>]*>\s*<label[^>]*>(.*?)</label>.*?</dt>\s*<dd[^>]*>(.*?)</dd>", h):
            tit = html_txt(dt).replace("*", "").strip()
            vals = [(html_txt(lab), fnum(pr)) for _v, pr, lab in re.findall(r'(?is)<option value="(\d+)"[^>]*price="([\d.]+)"[^>]*>(.*?)</option>', dd)]
            vals += [(html_txt(lab), fnum(pr)) for pr, lab in re.findall(r'(?is)<input[^>]*type="(?:checkbox|radio)"[^>]*price="([\d.]+)"[^>]*>.*?<label[^>]*>(.*?)</label>', dd)]
            vals = [(re.sub(r"\s*\+\s*R\$\s*[\d.,]+", "", l).strip(), pr or 0.0) for l, pr in vals]
            obrig = "required" in dd or "*" in dt
            nt = norm(tit)
            if not vals and nt.startswith("tamanho"):
                t = html_txt(dd)
                if t:
                    eixos.append((tit, [(t, 0.0)]))
                continue
            if nt.startswith("tamanho") or (obrig and any(pr for _, pr in vals) and not nt.startswith("cor")):
                eixos.append((tit, vals))
            else:
                for l, pr in vals:
                    if pr:
                        opcionais.append({"grupo": tit, "item": l, "preco": pr})
        if not any(norm(t).startswith("tamanho") for t, _ in eixos):
            mtam = re.search(r"\*\s*Tamanho\s+([0-9.,xX ]{7,40})", bloco)
            if mtam:
                eixos.insert(0, ("Tamanho", [(mtam.group(1).strip(), 0.0)]))
        base = {}
        itens = norm(" ".join(o["item"] for o in opcionais))
        if "aquecedor" in itens:
            base["aquecedor_incluso"] = False
        if "cromo" in itens:
            base["cromo_incluso"] = False
        spec = {}
        for lab in ("Material", "Comprimento", "Largura", "Lugares", "Configuração", "Forma", "Profundidade"):
            ms = re.search(r"(?is)<th[^>]*>\s*%s\s*</th>\s*<td[^>]*>(.*?)</td>" % lab, h)
            if ms:
                spec[lab] = html_txt(ms.group(1))
        if spec.get("Lugares"):
            base["lugares"] = extrai_lugares(spec["Lugares"])
        if spec.get("Material"):
            base["material"] = extrai_material(spec["Material"])
        # tabela "Ver Parcelas" (span#exibirPrecoParcelado, oculta): "2x de" / "R$4.295,00 Sem juros" em spans separados
        mtab = re.search(r'(?is)<span id="exibirPrecoParcelado".*?(?=<div class="extra-info"|<div class="caixaPrecoeBoleto"|$)', h)
        p_tab = _parcelas_html(mtab.group(0)[:20000]) if mtab else None
        p_blo = None if p_tab else _parcelas_html(bloco)
        parc = [str(x) for x in (p_tab, p_blo) if x]  # a tabela do produto vence o texto do bloco
        pix = _pix_html(bloco, preco) if preco else None
        combos = [([], 0.0)]
        for tit, vals in eixos:
            combos = [(pre + ["%s: %s" % (tit, l)], ac + pr) for pre, ac in combos for l, pr in vals][:48]
        for partes, ac in combos:
            c.add(pid, nome, u, preco + ac if preco else None, antigo + ac if antigo else None,
                  (pix + ac) if (pix and not partes) else None,
                  max(int(x) for x in parc) if parc else None, disp, sku=sku, variante=" | ".join(partes), texto=bloco,
                  attrs_base=base, extra={"opcionais": opcionais or None, "especificacoes": spec or None,
                                          "acrescimo_opcao": round(ac, 2) if partes else None})
    return n


# ---- 3) WooCommerce Store API ----
def _woo_preco(pr, chave):
    if not pr or pr.get(chave) in (None, ""):
        return None
    if pr.get("currency_minor_unit") is not None:
        mu = int(pr["currency_minor_unit"])
    else:  # Store API antiga (sem currency_minor_unit): a loja informa se o valor vem em centavos
        mu = int(pr.get("_minor_unit_padrao", 2))
    v = fnum(pr.get(chave))
    return v / (10 ** mu) if v is not None else None


def _woo_parcelas(price_html):
    """Parcelas sem juros do price_html da Store API. Mesma leitura da página (_parcelas_html): a regra antiga
    ('10 x' + até 40 caracteres sem ponto + 'sem juros') perdia toda parcela de R$ 1.000 ou mais, porque o
    separador de milhar é um ponto ('Em até 10 x de R$ 1.779,84 sem juros': 53 de 159 linhas da Aurum vazias)."""
    return _parcelas_html(price_html or "")


_RX_CONF = re.compile(r"(?im)^\s*configura[cç][aã]o\s+([A-Za-zÀ-ú][\wÀ-ú ]{1,25}?)\s*:\s*$")


def _blocos_configuracao(texto):
    """[(nome, bloco)] para descrições com 'Configuração Básica:' / 'Configuração Completa:' em linhas próprias."""
    t = str(texto or "")
    ms = list(_RX_CONF.finditer(t))
    out = []
    for k, m in enumerate(ms):
        fim = ms[k + 1].start() if k + 1 < len(ms) else len(t)
        out.append((m.group(1).strip(), t[m.start():fim]))
    return out


def _texto_com_configuracao(texto, blocos, escolhido):
    """Texto sem os blocos das outras configurações (o que vem antes do 1º bloco e os opcionais ficam)."""
    t = str(texto or "")
    for k, bl in blocos:
        if k != escolhido:
            fim = len(bl)
            mo = _RX_OPC_ROTULO.search(bl)  # 'ITEM OPCIONAL:' depois do último bloco continua no texto
            if mo:
                fim = mo.start()
            t = t.replace(bl[:fim], "\n", 1)
    return t


def _ficha_icones_html(h):
    """Pares título/descrição dos blocos de ícone do Elementor ('Dimensões' / '2.40 x 2.20 x 0.95', 'Acomodações' /
    '7 Lugares') como linhas 'Título: valor'. Só os do produto (o 1º grupo antes de 'Compra Garantida')."""
    pares = re.findall(r'(?is)class="elementor-icon-box-title"[^>]*>(.*?)</h\d>\s*(?:</div>\s*)?'
                       r'(?:<p|<div)[^>]*class="elementor-icon-box-description"[^>]*>(.*?)</(?:p|div)>', h or "")
    out = []
    for t, d in pares:
        t, d = re.sub(r"\s+", " ", html_txt(t)).strip(), re.sub(r"\s+", " ", html_txt(d)).strip()
        if re.search(r"dimens|medida|tamanho|acomoda|lugares|capacidade|litros", norm(t)) and d and len(d) < 60:
            out.append("%s: %s" % (t, d))
    return "\n".join(dict.fromkeys(out))


def ad_woo_store_api(c):
    base = c.cfg["endpoint"].rstrip("/")
    mu_padrao = int(c.p.get("minor_unit", 2))
    path = c.p.get("api_path", "/wp-json/wc/store/v1/products")
    prods, pg = [], 1
    while True:
        r = c.f.get(base + path, params={"per_page": 100, "page": pg})
        d = r.json()
        if not isinstance(d, list):
            raise RuntimeError("resposta inesperada da Store API")
        prods += d
        tot = int(r.headers.get("X-WP-TotalPages") or 0)
        if len(d) < 100 or (tot and pg >= tot) or pg >= 30:
            break
        pg += 1
    variaveis = [p for p in prods if p.get("type") == "variable"]
    var_por_pai = {}
    if variaveis:
        pg = 1
        try:
            while True:
                r = c.f.get(base + path, params={"type": "variation", "per_page": 100, "page": pg})
                d = r.json()
                for v in d if isinstance(d, list) else []:
                    var_por_pai.setdefault(v.get("parent"), []).append(v)
                tot = int(r.headers.get("X-WP-TotalPages") or 0)
                if not isinstance(d, list) or len(d) < 100 or (tot and pg >= tot) or pg >= 30:
                    break
                pg += 1
        except (requests.HTTPError, ValueError) as e:
            c.f.notas.append("variações indisponíveis via type=variation (%s)" % str(e)[:60])
    falta = [p for p in variaveis if p.get("id") not in var_por_pai]
    for p in falta[: int(c.p.get("max_var_individual", 40))]:  # fallback: busca cada variação pelo id
        for vv in (p.get("variations") or [])[:12]:
            try:
                v = c.f.json(base + path + "/%s" % vv["id"])
                var_por_pai.setdefault(p["id"], []).append(v)
            except (requests.HTTPError, ValueError, Bloqueado):
                break
    # Pix e parcelas: a Store API não traz; lê o % do Pix e as parcelas sem juros uma vez, na página de um produto
    regra_pix = None
    if c.p.get("pix_pct_html"):
        p0 = next((p for p in prods if classifica(html.unescape(p.get("name") or "")) in CATS_PRINCIPAIS and p.get("permalink")), None)
        if p0:
            pr0 = dict(p0.get("prices") or {}, _minor_unit_padrao=mu_padrao)
            regra_pix = _pix_da_loja(c, p0["permalink"], _woo_preco(pr0, "price"))
    for p in prods:
        nome = html.unescape(p.get("name") or "")
        cats = " ".join(html.unescape(x.get("name", "")) for x in p.get("categories") or [])
        sd, ds = html_txt(p.get("short_description")), html_txt(p.get("description"))
        texto = corta_relacionados(nome + "\n" + sd + "\n" + ds)
        conf_sel, hfj, hf_parc, hf_gar = None, None, None, None
        # Hidro Fiber: a página traz o configurador (JSON "hf-...-data") com cada configuração, o preço e o que inclui
        if c.p.get("config_html") and p.get("permalink") and p.get("type") != "variable":
            try:
                hp = c.f.texto(p["permalink"])
                mj = re.search(r'(?s)<script type="application/json" id="hf-[\w-]*-data">(.*?)</script>', hp)
                hfj = json.loads(mj.group(1)) if mj else None
                # bloco de preço "hf-product-price-wrapper": "Pague por Pix em até 12x R$ 675,00 sem juros · Em até 12x"
                # e "Garantia de 5 Anos" (a Store API não traz nenhum dos dois: 46 de 46 linhas sem parcelas, r3 #21/#22)
                iw = hp.find("hf-product-price-wrapper")
                bloco_hf = html_txt(hp[iw:iw + 3000]) if iw >= 0 else ""
                hf_parc = _parcelas_html(bloco_hf)
                hf_gar = extrai_garantia(str((hfj or {}).get("shellWarranty") or "") and
                                         "garantia de " + str(hfj.get("shellWarranty"))) or extrai_garantia(bloco_hf[:600])
            except (requests.HTTPError, Bloqueado, ValueError):
                hfj = None
        # descrição com várias "Configuração X:" num produto simples: sem o configurador, usa só o 1º bloco
        # (senão soma jatos/motores/itens de todas)
        blocos_conf = _blocos_configuracao(texto)
        if len(blocos_conf) >= 2 and p.get("type") != "variable":
            conf_ok = blocos_conf[0][0]
            texto_conf = _texto_com_configuracao(texto, blocos_conf, conf_ok)
            conf_sel = (conf_ok, "primeira configuracao da descricao")
        else:
            texto_conf = texto
        # WT Fibras: medidas e lugares só no bloco de ícones da página (Elementor "Dimensões 2.40 x 2.20 x 0.95")
        if c.p.get("ficha_html") and p.get("permalink"):
            try:
                hp = c.f.texto(p["permalink"])
                fic = _ficha_icones_html(hp)
                if fic:
                    texto = texto + "\n" + fic
                    texto_conf = texto_conf + "\n" + fic
            except (requests.RequestException, Bloqueado):
                pass
        attrs = {a.get("name"): ", ".join(t.get("name", "") for t in a.get("terms") or []) for a in p.get("attributes") or []}
        base_a = {}
        forma_attr = None
        for k, v in attrs.items():
            if re.search(r"lugar|pessoa", norm(k)):
                base_a["lugares"] = extrai_lugares(v + " lugares") if re.search(r"\d", v) else extrai_lugares(v)
            if re.search(r"material", norm(k)) and extrai_material(v):
                base_a["material"] = extrai_material(v)
            # Lineaplas: atributo "Dimensões: 1,8 x 1,8 x 0,71" (323 de 326 linhas ficavam sem medida)
            if re.search(r"dimens|medida", norm(k)):
                md = list(dict.fromkeys(extrai_medidas(v)))
                if len(md) == 1:
                    base_a["comprimento_m"], base_a["largura_m"], base_a["profundidade_m"] = md[0]
            if re.search(r"^forma(to)?$", norm(k)):
                forma_attr = extrai_forma("forma: " + v, so_rotulo=True)
        extra = {"categorias_loja": cats[:300], "atributos_loja": attrs or None}
        if forma_attr:
            extra["forma"] = forma_attr
        # Gold/Aurum: "SPA ISLA COM HIDROMASSAGEM SIZE 8 PESSOAS" só no nome da imagem do produto
        img_txt = " | ".join(str(im.get("alt") or "") + " " + str(im.get("name") or "") for im in p.get("images") or [])
        lug_img = extrai_lugares(img_txt, False) if re.search(r"pessoa|lugar", norm(img_txt)) else None
        if lug_img:
            extra["lugares_imagem"] = lug_img
        # forma que o nome e a ficha não dizem: categoria da loja ou nome/alt das imagens ("spa-octogonal.jpg")
        if not forma_attr and not extrai_forma(nome):
            fe = forma_evidencia(cats) or forma_evidencia(img_txt)
            if fe:
                extra["forma"] = fe
                extra["forma_origem"] = "categoria da loja" if forma_evidencia(cats) else "nome da imagem do produto"
        if conf_sel:
            extra.update(configuracao=conf_sel[0], configuracao_origem=conf_sel[1], configuracoes_na_descricao=[k for k, _ in blocos_conf])

        def _pix_parc(preco, parc):
            if not regra_pix:
                return None, parc
            px = round(preco * (1 - regra_pix["pct"] / 100.0), 2) if (preco and regra_pix.get("pct")) else None
            return px, (parc or regra_pix.get("parcelas"))
        if regra_pix and regra_pix.get("pct"):
            extra["pix_origem"] = "percentual da loja (%s%% lido em %s)" % (regra_pix["pct"], regra_pix["url"])
        if regra_pix and regra_pix.get("parcelas"):
            extra["parcelas_origem"] = "regra da loja (lida em %s) quando o produto não informa" % regra_pix["url"]
        vs = var_por_pai.get(p.get("id")) if p.get("type") == "variable" else None
        if vs:
            for v in vs:
                vt = v.get("variation") or ", ".join("%s: %s" % (a.get("name"), a.get("value")) for a in v.get("attributes") or [])
                vt = html.unescape(vt or v.get("name") or "")
                pr = dict(v.get("prices") or {}, _minor_unit_padrao=mu_padrao)
                px, pa = _pix_parc(_woo_preco(pr, "price"), _woo_parcelas(v.get("price_html") or p.get("price_html")))
                c.add(p["id"], nome, v.get("permalink") or p.get("permalink"), _woo_preco(pr, "price"), _woo_preco(pr, "regular_price"),
                      px, pa, v.get("is_in_stock"),
                      sku=v.get("sku") or p.get("sku") or v.get("id"), variante=vt, texto=texto, attrs_base=base_a,
                      extra=dict(extra, variacao_id=v.get("id")), cat_extra=cats)
        else:
            pr = dict(p.get("prices") or {}, _minor_unit_padrao=mu_padrao)
            if p.get("is_purchasable") is False and not fnum(pr.get("price")):
                pr["price"] = None  # modo catálogo: preço 0 = não publicado
            px, pa = _pix_parc(_woo_preco(pr, "price"), _woo_parcelas(p.get("price_html")))
            if pa is None and hf_parc:
                pa = hf_parc
                extra["parcelas_origem"] = "bloco de preço da página (hf-product-price-wrapper)"
            if hf_gar and base_a.get("garantia_casco_anos") is None:
                base_a["garantia_casco_anos"] = hf_gar
            opc_hf = [{"grupo": "opções da página", "item": o.get("label"), "preco": fnum(o.get("price"))}
                      for o in (hfj or {}).get("options") or [] if fnum(o.get("price"))]
            confs = (hfj or {}).get("configurations") or []
            if confs:
                # uma linha por configuração, com o preço e a composição publicados no configurador da página
                cab = blocos_conf[0][1] if blocos_conf else ""
                cab = (texto.split(cab)[0] if cab else texto) + "\n" + str(hfj.get("baseDescription") or "")
                for cf in confs:
                    inc = "\n".join(cf.get("inclusions") or [])
                    ex_c = dict(extra, configuracao=cf.get("label"), configuracao_padrao=bool(cf.get("default")) or None,
                                configuracao_origem="configurador da página", opcionais=opc_hf or None)
                    for k in ("configuracoes_na_descricao",):
                        ex_c.pop(k, None)
                    c.add(p["id"], nome, p.get("permalink"), fnum(cf.get("price")),
                          _woo_preco(pr, "regular_price") if cf.get("default") else None, None, pa, p.get("is_in_stock"),
                          sku="%s-%s" % (p.get("sku") or p.get("id"), cf.get("id")), variante="Configuração: %s" % cf.get("label"),
                          texto=cab + "\n" + inc, attrs_base=base_a, extra=ex_c, cat_extra=cats)
                continue
            if hfj:
                pv = fnum(((hfj.get("pricing") or {}).get("currentValue")))
                if pv and _woo_preco(pr, "price") and abs(pv - _woo_preco(pr, "price")) > 0.01:
                    extra["preco_configurador_pagina"] = pv  # o configurador da página mostra outro preço: conferir
                    c.f.notas.append("preço do configurador difere da loja em %s" % urlparse(p.get("permalink")).path[:50])
                if opc_hf:
                    extra["opcionais"] = opc_hf
            c.add(p["id"], nome, p.get("permalink"), _woo_preco(pr, "price"), _woo_preco(pr, "regular_price"), px,
                  pa, p.get("is_in_stock"), sku=p.get("sku") or p.get("id"), variante="",
                  texto=texto_conf, attrs_base=base_a, extra=extra, cat_extra=cats)
    return len(prods)


# ---- 4) Shopify products.json ----
def ad_shopify_products_json(c):
    base = c.cfg["endpoint"].rstrip("/")
    prods, pg = [], 1
    while pg <= 10:
        d = c.f.json(base + "/products.json", params={"limit": 250, "page": pg})
        lote = d.get("products") or []
        prods += lote
        if len(lote) < 250:
            break
        pg += 1
        time.sleep(3)
    regra_pix = None
    if c.p.get("pix_pct_html"):
        p0 = next((p for p in prods if classifica(p.get("title") or "") in CATS_PRINCIPAIS and p.get("variants")), None)
        if p0:
            regra_pix = _pix_da_loja(c, base + "/products/" + p0.get("handle", ""), brl(p0["variants"][0].get("price")))
    for p in prods:
        nome = p.get("title")
        texto = corta_relacionados(nome + "\n" + html_txt(p.get("body_html")))
        tags = p.get("tags")
        tags = ", ".join(tags) if isinstance(tags, list) else (tags or "")
        url = base + "/products/" + p.get("handle", "")
        nomes_opc = [o.get("name") for o in p.get("options") or []]
        for v in p.get("variants") or []:
            vt = v.get("title") or ""
            if vt.lower() == "default title":
                vt = ""
            elif nomes_opc:
                partes = [v.get("option%d" % i) for i in (1, 2, 3)]
                vt = " | ".join("%s: %s" % (nomes_opc[i], x) for i, x in enumerate(partes) if x and i < len(nomes_opc))
            pv = brl(v.get("price"))
            ex = {"tipo_loja": p.get("product_type"), "tags": tags[:300], "fornecedor": p.get("vendor"), "variant_id": v.get("id")}
            px = pa = None
            if regra_pix:
                if regra_pix.get("pct") and pv:
                    px = round(pv * (1 - regra_pix["pct"] / 100.0), 2)
                    ex["pix_origem"] = "percentual da loja (%s%% lido em %s)" % (regra_pix["pct"], regra_pix["url"])
                if regra_pix.get("parcelas"):
                    pa = regra_pix["parcelas"]
                    ex["parcelas_origem"] = "regra da loja (lida em %s)" % regra_pix["url"]
            c.add(p["id"], nome, url + ("?variant=%s" % v["id"] if vt else ""), pv, brl(v.get("compare_at_price")),
                  px, pa, v.get("available"), sku=v.get("sku") or v.get("id"), variante=vt, texto=texto,
                  extra=ex, cat_extra=(p.get("product_type") or "") + " " + tags)
    return len(prods)


# ---- 5) Tray web_api ----
def _tray_pag(details, preco=None):
    """Pix e parcelas do payment_option_details da web_api. O campo 'tax' vem 0 mesmo quando a loja cobra juros
    (Aquaplas: 'Cartão - Vindi', plots 10, value 666,50, tax 0,00 para um produto de R$ 5.990: 10 x 666,50 = 6.665,
    e a página diz 'com juros'). Só vale como sem juros quando parcelas x valor <= preço (+1%)."""
    pix, parc = None, None
    for d in details or []:
        if d.get("type") == "pix" and str(d.get("plots")) == "1":
            pix = fnum(d.get("value"))
        if d.get("type") == "credit_card" and fnum(d.get("tax")) == 0:
            n, v = int(d.get("plots") or 0), fnum(d.get("value"))
            if preco and v and n > 1 and n * v > preco * 1.01 + 0.05:
                continue  # parcela com juros embutidos
            parc = max(parc or 0, n) or None
    return pix, (parc if parc and parc > 1 else None)


def _tray_parcelas_pagamento(h):
    """Maior nº de parcelas sem juros da aba 'Formas de Pagamento' da Tray (/mvc/store/product/payment_options):
    cada forma tem as colunas 'à vista', '2 X' ... '12 X' e um valor por coluna com 'sem Tarifa' ou 'com tarifa
    incluso' / 'com tarifa de 2.79 % a.m.'. Devolve (n, lida): n = maior N > 1 sem tarifa/juros (None se não há)."""
    t = norm(html_txt(h or ""))
    if not re.search(r"\d{1,2}\s*x\b", t):
        return None, False
    melhor = None
    for bloco in re.split(r"parcela minima de r\$\s*[\d.,]+", t):
        # "vista" sozinho: com a página lida em outra codificação o "à" vira lixo e o "a vista" some; perder uma
        # coluna desalinha tudo (a coleta de 31/10 gravou 4x "sem juros" na Aquaplas assim)
        cols = re.findall(r"\bvista\b|\b(\d{1,2})\s*x\b", bloco)
        sts = re.findall(r"r\$\s*[\d.,]+\s*(sem tarifa|sem juros|com tarifa|com juros)", bloco)
        if len(cols) != len(sts):
            continue  # colunas e valores não batem: não adivinha
        for col, st in zip(cols, sts):
            if col and int(col) > 1 and st.startswith("sem"):
                melhor = max(melhor or 0, int(col))
    return melhor, True


def _tray_parcelas_loja(c, base, h, id_var=None):
    """Lê a aba de pagamento da Tray do produto (data-url / data-tab-url do #formasPagto da página). (n, lida)."""
    m = re.search(r'data-(?:tab-)?url="(/mvc/store/product/payment_options\?[^"]+)"', h or "")
    if not m:
        return None, False
    u = urljoin(base + "/", html.unescape(m.group(1)).replace("%s", str(id_var or "")))
    try:
        return _tray_parcelas_pagamento(c.f.texto(u))
    except (requests.RequestException, Bloqueado):
        return None, False


def _itens_serie_tray(h):
    """Tabela 'Itens de Série' da Aquaplas (aba Especificações Técnicas): colunas = versões (Basic, Standard,
    Premium; Imersão, Standard). [(versao, ...)], [(item, [valor por versão])]: True = check, texto = valor da
    célula ('3', '1/2 CV'), None = célula vazia (a versão não tem)."""
    mt = re.search(r'(?is)<table[^>]*class="[^"]*tabela-comparativa[^"]*"[^>]*>(.*?)</table>', h or "")
    if not mt:
        return None
    trs = re.findall(r"(?is)<tr[^>]*>(.*?)</tr>", mt.group(1))
    if len(trs) < 2:
        return None

    def cel(x):
        if re.search(r"(?i)icon-check|check", x) and not html_txt(x).strip():
            return True
        v = re.sub(r"\s+", " ", html_txt(x)).strip()
        return v or None
    cab = [re.sub(r"\s+", " ", html_txt(x)).strip() for x in re.findall(r"(?is)<t[hd][^>]*>(.*?)</t[hd]>", trs[0])]
    versoes = cab[1:]
    linhas = []
    for tr in trs[1:]:
        cs = re.findall(r"(?is)<t[hd][^>]*>(.*?)</t[hd]>", tr)
        if len(cs) != len(cab):
            continue
        linhas.append((re.sub(r"\s+", " ", html_txt(cs[0])).strip(), [cel(x) for x in cs[1:]]))
    return (versoes, linhas) if versoes and linhas else None


def _attrs_itens_serie(tab, variante):
    """Atributos da versão escolhida pela tabela de itens de série (vence a descrição, que é a mesma das versões).
    Versão = coluna cujo nome é um valor da variante ('Tipo: Basic'). Sem coluna que bata, só as linhas iguais em
    todas as versões. Aquecedor/cromo fora da coluna da versão = não incluso (a tabela é a lista de série)."""
    versoes, linhas = tab
    vals = {norm(x.split(":", 1)[-1]) for x in re.split(r"\s*\|\s*", variante or "") if x.strip()}
    idx = next((k for k, v in enumerate(versoes) if norm(v) in vals), None)
    if idx is None and len(versoes) == 1:
        idx = 0
    out, extra, txt = {}, {}, []
    tem = {"aquecedor_incluso": None, "cromo_incluso": None}
    mini_vazio = False
    for item, cels in linhas:
        if idx is not None:
            v = cels[idx]
        elif all(x == cels[0] for x in cels):
            v = cels[0]
        else:
            v = "?"  # difere entre versões e a versão não é conhecida
        ni = norm(item)
        for chave, rx in (("aquecedor_incluso", r"aquec"), ("cromo_incluso", r"cromo|\bled")):
            if re.search(rx, ni):
                if v == "?":
                    tem[chave] = "?"
                elif tem[chave] is not True:
                    tem[chave] = bool(v)
                if chave == "cromo_incluso" and v not in (None, "?"):
                    cp = re.search(r"(\d{1,2})\s*pontos?", ni + " " + (v if isinstance(v, str) else ""))
                    if cp:
                        extra["cromo_pontos"] = int(cp.group(1))
        if v in (None, "?"):
            if v is None and re.search(r"(?:mini|micro)[\s-]?jat", ni):
                mini_vazio = True
            continue
        if v is True:
            txt.append(item)
        elif re.fullmatch(r"\d{1,2}", v):
            txt.append("%s %s" % (v, item))
        else:
            txt.append("%s %s" % (item, v))
    t = "\n".join(txt)
    j, mj = extrai_jatos(t)
    if j is not None:
        out["jatos"] = j
    if mj is not None:
        out["mini_jatos"] = mj
    elif mini_vazio:
        out["mini_jatos"] = 0
    mo, cv = extrai_motores(t), extrai_cv(t)
    if mo:
        out["motores"] = mo
    if cv:
        out["potencia_cv"] = cv
    for k, v in tem.items():
        out[k] = False if v is None else (None if v == "?" else v)
    extra["itens_serie"] = ("coluna %s da tabela Itens de Série" % versoes[idx]) if idx is not None else \
        "tabela Itens de Série (só itens iguais em todas as versões)"
    return out, extra


def ad_tray_web_api(c):
    base = c.cfg["endpoint"].rstrip("/")
    prods, pg = [], 1
    while True:
        d = c.f.json(base + "/web_api/products", params={"limit": 50, "page": pg})
        prods += [x["Product"] for x in d.get("Products") or []]
        tot = int((d.get("paging") or {}).get("total") or 0)
        if pg * 50 >= tot or pg >= int(c.p.get("max_paginas", 40)):
            if pg * 50 < tot:
                c.f.notas.append("corte: %d de %d produtos" % (pg * 50, tot))
            break
        pg += 1
    rx = c.p.get("nome_regex")
    sel = []
    for p in prods:
        if str(p.get("available")) != "1" and str(p.get("available_in_store", "1")) == "0":
            continue
        if fnum(p.get("price")) and fnum(p.get("price")) > 1e6:
            continue
        if rx and not re.search(rx, norm(p.get("name")), re.I):
            continue
        u0 = (p.get("url") or {}).get("https") if isinstance(p.get("url"), dict) else (p.get("url") or "")
        p["_cat_url"] = "" if c.p.get("so_nome") else urlparse(u0 or "").path.replace("-", " ").replace("/", " ")
        if classifica(p.get("name"), p["_cat_url"]) not in c.cats:
            continue
        sel.append(p)
    # descrição (ficha) e variações: só dos produtos mantidos
    lim_det = int(c.p.get("max_detalhes", 300))
    for i, p in enumerate(sel):
        nome = p.get("name")
        url = (p.get("url") or {}).get("https") if isinstance(p.get("url"), dict) else p.get("url")
        desc = ""
        if i < lim_det:
            try:
                dd = c.f.json(base + "/web_api/products/%s" % p["id"])
                desc = html_txt((dd.get("Product") or {}).get("description") or "")
            except (requests.HTTPError, ValueError):
                pass
        elif i == lim_det:
            c.f.notas.append("corte: ficha de %d de %d produtos" % (lim_det, len(sel)))
        texto = corta_relacionados(nome + "\n" + desc)
        props = p.get("Properties") or {}
        props_t = {k: ", ".join(v) if isinstance(v, list) else str(v) for k, v in (props.items() if isinstance(props, dict) else [])}
        base_a = {}
        for k, v in props_t.items():
            if re.search(r"lugar|pessoa", norm(k)):
                base_a["lugares"] = extrai_lugares(v + (" lugares" if re.fullmatch(r"\d+", v.strip()) else ""))
            if re.search(r"material", norm(k)) and extrai_material(v):
                base_a["material"] = extrai_material(v)
        opcionais, tamanhos = [], []
        for ai in p.get("AdditionalInfos") or []:
            itens_ai = []
            for o in ai.get("options") or []:
                oo = o.get("options") if isinstance(o, dict) else None
                if oo and oo.get("name"):
                    itens_ai.append({"grupo": ai.get("name"), "item": oo.get("name"), "preco": fnum(oo.get("value")) or 0.0})
            # grupo "TAMANHOS E MATERIAIS"/"MEDIDAS" com medidas nos itens: é escolha obrigatória, vira variante
            if (not tamanhos and re.search(r"tamanho|medida", norm(ai.get("name") or ""))
                    and itens_ai and sum(1 for x in itens_ai if extrai_medidas(x["item"])) >= max(1, len(itens_ai) // 2)):
                tamanhos = itens_ai
                continue
            opcionais += [x for x in itens_ai if x["preco"]]
        it = norm(" ".join(o["item"] or "" for o in opcionais))
        if "aquecedor" in it:
            base_a["aquecedor_incluso"] = False
        if "cromo" in it:
            base_a["cromo_incluso"] = False
        extra = {"marca": p.get("brand") or None, "modelo": p.get("model") or None, "propriedades": props_t or None,
                 "opcionais": opcionais or None, "prazo": p.get("availability") or None}
        preco = fnum(p.get("price"))
        promo = fnum(p.get("promotional_price"))
        pix, parc = _tray_pag(p.get("payment_option_details"), promo if promo and preco and promo < preco else preco)
        variantes = []
        if str(p.get("has_variation")) == "1" and p.get("Variant"):
            try:
                dv = c.f.json(base + "/web_api/products/variants", params={"product_id": p["id"], "limit": 50})
                variantes = [x["Variant"] for x in dv.get("Variants") or []]
            except (requests.HTTPError, ValueError):
                variantes = []
        # Aquaplas: a página do produto traz a tabela "Itens de Série" por versão (Basic/Standard/Premium) e a aba
        # "Formas de Pagamento" (parcelas sem juros de verdade). A descrição da web_api é a mesma das três versões.
        tab, parc_pag = None, (None, False)
        if (c.p.get("itens_serie_html") or c.p.get("pagamento_tray")) and i < lim_det and url:
            try:
                hp = c.f.texto(url)
            except (requests.RequestException, Bloqueado):
                hp = ""
            if c.p.get("itens_serie_html"):
                tab = _itens_serie_tray(hp)
            if c.p.get("pagamento_tray"):
                parc_pag = _tray_parcelas_loja(c, base, hp, (variantes[0].get("id") if variantes else ""))
        if parc_pag[1]:
            parc = parc_pag[0]
            extra["parcelas_origem"] = "aba Formas de Pagamento da página (maior N sem tarifa)"

        def _base_v(vt):
            if not tab:
                return base_a, {}
            a_t, ex_t = _attrs_itens_serie(tab, vt)
            return dict(base_a, **{k: v for k, v in a_t.items() if v is not None}), ex_t
        if variantes:
            for v in variantes:
                vp, vpr = fnum(v.get("price")), fnum(v.get("promotional_price"))
                skus = v.get("Sku") or []
                vt = " | ".join("%s: %s" % (s.get("type"), s.get("value")) for s in skus if isinstance(s, dict))
                por = vpr if vpr and vp and vpr < vp else vp
                vpix, vparc = _tray_pag(v.get("payment_option_details"), por)
                if parc_pag[1]:
                    vparc = parc_pag[0]
                b_v, ex_v = _base_v(vt)
                c.add(p["id"], nome, url, por, vp if por != vp else None, vpix or None, vparc if parc_pag[1] else (vparc or parc),
                      str(v.get("available")) == "1", sku=v.get("reference") or v.get("id"), variante=vt, texto=texto,
                      attrs_base=b_v, extra=dict(extra, variante_id=v.get("id"), **ex_v), cat_extra=p["_cat_url"])
        elif tamanhos:
            por = promo if promo and preco and promo < preco else preco
            for tm in tamanhos:
                vt = re.sub(r"\s+", " ", str(tm["item"])).strip()
                b_v, ex_v = _base_v(vt)
                c.add(p["id"], nome, url, round(por + tm["preco"], 2) if por else None, None, None, parc,
                      str(p.get("available")) == "1", sku=p.get("id"), variante=vt, texto=texto, attrs_base=b_v,
                      extra=dict(extra, acrescimo_opcao=tm["preco"], grupo_variante=tm["grupo"], **ex_v), cat_extra=p["_cat_url"])
        else:
            por = promo if promo and preco and promo < preco else preco
            b_v, ex_v = _base_v("")
            c.add(p["id"], nome, url, por, preco if por != preco else None, pix, parc, str(p.get("available")) == "1",
                  sku=p.get("id"), variante="", texto=texto, attrs_base=b_v, extra=dict(extra, **ex_v), cat_extra=p["_cat_url"])
    return len(sel)


def _tray_disponivel(h):
    """Disponibilidade na página Tray sem dataLayer antigo (tema GA4): JSON-LD do Product (offers.availability)
    e, sem ele, o botão Comprar do produto (button-buy / data-tray-tst='button_buy_product') ou o aviso de
    indisponível. 'YES'/'NO' como o dataLayer antigo; None quando a página não diz."""
    for x in ld_json_blocos(h):
        if ld_tipo(x, "Product"):
            of = x.get("offers")
            of = of[0] if isinstance(of, list) and of else of
            av = str((of or {}).get("availability") or "") if isinstance(of, dict) else ""
            if av.strip():
                return "YES" if re.search(r"InStock|PreOrder|LimitedAvailability|InStoreOnly", av) else "NO"
    if re.search(r"""id=["']button-buy["']|data-tray-tst=["']button_buy_product["']""", h or ""):
        return "YES"
    if re.search(r"(?i)produto\s+indispon[ií]vel|avise-me quando chegar", html_txt(h or "")[:20000]):
        return "NO"
    return None


# ---- 6) Tray HTML dataLayer (listSku) ----
def ad_tray_html_datalayer(c):
    urls = sitemap_urls(c, c.cfg["endpoint"], c.p.get("url_regex"))
    n = 0
    for u in urls[: int(c.p.get("max_paginas", 300))]:
        try:
            h = c.f.texto(u)
        except requests.HTTPError:
            continue
        m = re.search(r"dataLayer\s*=\s*\[(\{.*?\})\]\s*;?\s*</script>", h, re.S)
        dl = None
        if m:
            try:
                dl = json.loads(m.group(1))
            except ValueError:
                dl = None
        if not dl or not dl.get("nameProduct"):
            # temas Tray com GA4 (dataLayerGa4.push view_item): items[0] + input#preco_atual
            mg = re.search(r'"items"\s*:\s*\[(\{[^\[\]]*?"item_id"[^\[\]]*?\})\]', h)
            if not mg:
                continue
            try:
                it = json.loads(mg.group(1))
            except ValueError:
                continue
            mpa = re.search(r'id="preco_atual"[^>]*value="([\d.,]+)"|value="([\d.,]+)"[^>]*id="preco_atual"', h)
            pa = brl(mpa.group(1) or mpa.group(2)) if mpa else None
            dl = dict(nameProduct=it.get("item_name"), idProduct=it.get("item_id"), priceSell=pa or it.get("price"),
                      price=it.get("price"), category=it.get("item_category"), model=it.get("item_brand"),
                      availability=_tray_disponivel(h))
        nome = dl.get("nameProduct")
        if not nome:
            continue
        n += 1
        t = html_txt(re.sub(r"(?is)<(header|nav|footer|aside)[^>]*>.*?</\1>", " ", h))
        i = t.find("Descrição Geral")
        j = t.find("Deixe seu comentário")
        desc = t[i:j] if 0 <= i < j else bloco_produto_html(h, nome)
        desc = corta_relacionados(desc)[:6000]
        mo = re.search(r"OPCIONAIS:\s*([^\n]*(?:\n[^\n]*)?)", desc)
        extra = {"modelo": dl.get("model"), "categoria_loja": dl.get("category"), "opcionais_texto": mo.group(1).strip()[:400] if mo else None}
        for trecho, fm in (c.p.get("forma_por_url") or {}).items():
            if trecho in u:
                extra["forma"] = fm
                extra["forma_origem"] = "linha da loja na URL"
        if c.p.get("forma_por_url") and not extra.get("forma"):
            # Linha Dupla/Especial misturam formas (Itatiaia 1,60 x 1,60 é de canto pela foto): forma não declarada
            extra["forma_nao_declarada"] = True
        parc_pag = (None, False)
        if c.p.get("pagamento_tray"):
            pu = urlparse(u)
            parc_pag = _tray_parcelas_loja(c, "%s://%s" % (pu.scheme, pu.netloc), h, "")
            if parc_pag[1]:
                extra["parcelas_origem"] = "aba Formas de Pagamento da página (maior N sem tarifa)"
        lst = dl.get("listSku") or []
        # descrição com um bloco por kit ("CONF. KIT - 9 · 6 JATOS ... · 2 MOTOBOMBAS DE 1CV"): cada variante "Kit 09"
        # usa só o seu bloco; kit sem bloco ou com bloco repetido fica sem jatos/motores (não soma a página inteira)
        blocos, rx_kit = {}, r"(?i)conf\.?\s*kit\s*-\s*0?(\d{1,2})\b"
        marcas = list(re.finditer(rx_kit, desc))
        for k, mk in enumerate(marcas):
            fim = marcas[k + 1].start() if k + 1 < len(marcas) else len(desc)
            bl = re.split(r"(?i)opcionais\s*:", desc[mk.start():fim])[0]
            blocos.setdefault(int(mk.group(1)), []).append(bl)
        desc_geral = (desc[:marcas[0].start()] + "\n" + (re.search(r"(?is)opcionais\s*:.*", desc[marcas[-1].start():]) or [""])[0]) if marcas else desc
        if lst:
            for v in lst:
                texto_v, ex_v = nome + "\n" + desc, dict(extra, id_sku=v.get("idSku"))
                base_v = {}
                mkv = re.search(r"(?i)\bkit\s*0?(\d{1,2})\b", v.get("nameSku") or "")
                if blocos and mkv:
                    bl = blocos.get(int(mkv.group(1))) or []
                    if len(bl) == 1:
                        texto_v = nome + "\n" + bl[0] + "\n" + desc_geral
                        ex_v["kit_mapeamento"] = "provavel (numero do kit na descricao = numero no SKU)"
                        # item listado no bloco do kit vence o "OPCIONAIS: ... aquecedor" da descrição geral
                        for chave, irx in (("aquecedor_incluso", r"aquecedor(?:es)?|aquecimento"),
                                           ("cromo_incluso", r"cromoterapia|cromo|leds?\b|iluminacao")):
                            if _flag_item(nome + " " + (v.get("nameSku") or ""), bl[0], irx) is True:
                                base_v[chave] = True
                    else:
                        texto_v = nome + "\n" + desc_geral
                        ex_v["kit_mapeamento"] = "sem bloco na descricao (configuracao desconhecida)" if not bl else "bloco repetido na descricao (configuracao desconhecida)"
                c.add(dl.get("idProduct"), nome, u, fnum(v.get("sellPrice")) or fnum(v.get("price")),
                      fnum(v.get("price")) if fnum(v.get("sellPrice")) and fnum(v.get("price")) and fnum(v.get("price")) > fnum(v.get("sellPrice")) else None,
                      None, parc_pag[0], v.get("availability") == "YES", sku=v.get("idSku"), variante=v.get("nameSku") or "",
                      texto=texto_v, extra=ex_v, attrs_base=base_v)
        else:
            por1 = fnum(dl.get("priceSell")) or fnum(dl.get("price"))
            bl6 = bloco_produto_html(h, nome, 6000)
            px1 = _pix_html(bl6, por1) if por1 else None
            if px1 is None and por1:
                # "R$ 2.428,51 à vista" sem dizer Pix: guarda à parte (não vira preco_pix; a forma não é dita)
                mav = re.search(_RS + r"\s*(?:a|à)\s+vista", _junta_valor(bl6), re.I)
                av = brl(mav.group(1)) if mav else None
                if av and 0.8 * por1 <= av < por1:
                    extra = dict(extra, preco_a_vista=av)
            c.add(dl.get("idProduct"), nome, u, por1, fnum(dl.get("price")), px1,
                  parc_pag[0] if parc_pag[1] else _parcelas_html(bl6),
                  (dl.get("availability") == "YES") if dl.get("availability") in ("YES", "NO") else None, sku=dl.get("idProduct"),
                  variante="", texto=nome + "\n" + desc, extra=extra)
    return n


# ---- 7) VTEX catálogo público ----
# Cluster/selo do catálogo VTEX que esconde o preço no site ("Skus ocultação preço front-end" da Quero-Quero: a página
# mostra "Exclusivo loja física: Contate um vendedor!", sem preço e sem botão de compra)
_RX_VTEX_SEM_VENDA = re.compile(r"oculta\w*\s+(?:de\s+)?pre[cç]o|exclusivo\s+(?:da\s+|na\s+)?loja|loja\s+f[ií]sica|"
                                r"contate\s+um\s+vendedor|venda\s+s[oó]\s+(?:na|em)\s+loja|sob\s+consulta", re.I)
# bandeiras de cartão comuns e carteiras: parcelas "sem juros" do cartão próprio da loja (Cartão VerdeCard, 10x)
# não valem para o cliente comum (8x)
_RX_CARTAO_COMUM = re.compile(r"visa|master|elo\b|american|amex|hiper|diners|discover|jcb|cabal|aura\b|google pay|apple pay|"
                              r"samsung pay|credito|crédito", re.I)


def _vtex_oferta(p, it, o):
    """(preco, de, pix, parcelas, disponivel, motivo_sem_preco) de um item do catálogo VTEX, com as regras da página:
    indisponível (IsAvailable false / estoque 0: a página mostra "Avise-me", sem preço) e venda só na loja física
    (cluster de preço oculto) não têm preço publicado: preco None, disponivel False. O preço parado do catálogo vai
    para atributos_json.preco_catalogo_vtex (só referência)."""
    lp, pr = fnum(o.get("ListPrice")), fnum(o.get("Price"))
    inst = o.get("Installments") or []
    comuns = [x for x in inst if _RX_CARTAO_COMUM.search(x.get("PaymentSystemName") or "")]
    sj = [x["NumberOfInstallments"] for x in (comuns or inst) if not x.get("InterestRate")]
    pix = [x.get("Value") for x in inst if "pix" in (x.get("PaymentSystemName") or "").lower() and x.get("NumberOfInstallments") == 1]
    clusters = " | ".join(str(v) for k in ("productClusters", "clusterHighlights") for v in (p.get(k) or {}).values())
    motivo = None
    if _RX_VTEX_SEM_VENDA.search(clusters):
        motivo = "venda só na loja física (catálogo: %s); o site não mostra preço" % _RX_VTEX_SEM_VENDA.search(clusters).group(0)
    elif o.get("IsAvailable") is False or not (o.get("AvailableQuantity") or 0) > 0:
        motivo = "indisponível no site (Avise-me), sem preço publicado"
    if motivo:
        return None, None, None, None, False, motivo
    return (pr, lp if lp and pr and lp > pr else None, pix[0] if pix and pr and pix[0] < pr else None,
            max(sj) if sj else None, True, None)


def ad_vtex_catalog(c):
    base = c.cfg["endpoint"].rstrip("/")
    vistos, n = set(), 0
    for cam in c.p.get("caminhos") or []:
        ini = 0
        while ini < 2500:
            r = c.f.get(base + cam, params={"_from": ini, "_to": ini + 49}, tolerar=(206,))
            try:
                lote = r.json()
            except ValueError:
                break
            if not lote:
                break
            for p in lote:
                if p.get("productId") in vistos:
                    continue
                vistos.add(p.get("productId"))
                nome = p.get("productName") or ""
                cat_txt = " ".join(p.get("categories") or [])
                if classifica(nome, "" if c.p.get("so_nome") else cat_txt) not in c.cats:
                    continue
                n += 1
                specs = {k: ", ".join(p.get(k) or []) for k in p.get("allSpecifications") or []}
                texto = corta_relacionados(nome + "\n" + html_txt(p.get("description") or "") + "\n" +
                                           "\n".join("%s: %s" % kv for kv in specs.items()))
                for it in p.get("items") or []:
                    sel = (it.get("sellers") or [{}])[0]
                    o = sel.get("commertialOffer") or {}
                    vt = it.get("name") if len(p.get("items") or []) > 1 else ""
                    pr, de, px, parc, disp, motivo = _vtex_oferta(p, it, o)
                    ex = {"marca": p.get("brand"), "vendedor": sel.get("sellerName"), "ean": it.get("ean"),
                          "categoria_loja": (p.get("categories") or [""])[0]}
                    if motivo:
                        ex.update(motivo_sem_preco=motivo, preco_catalogo_vtex=fnum(o.get("Price")) or None)
                    c.add(p.get("productId"), nome, p.get("link"), pr, de, px, parc, disp, sku=it.get("itemId"),
                          variante=vt or "", texto=texto, extra=ex, cat_extra=cat_txt)
            if len(lote) < 50:
                break
            ini += 50
    return n


# ---- 8) Nuvemshop: sitemap + LS.variants ----
def ad_nuvemshop_sitemap_variants(c):
    urls = sitemap_urls(c, c.cfg["endpoint"], r"/produtos/")
    rx = c.p.get("url_regex")
    if rx:
        urls = [u for u in urls if re.search(rx, u, re.I)]
    n = 0
    for u in urls[: int(c.p.get("max_paginas", 300))]:
        try:
            h = c.f.texto(u)
        except requests.HTTPError:
            continue
        m = re.search(r"LS\.variants\s*=\s*(\[.*?\]);\s", h, re.S)
        try:
            variants = json.loads(m.group(1)) if m else []
        except ValueError:
            variants = []
        if not variants:  # temas novos: atributo data-variants no formulário do produto (não o 1º JSON-LD, que pode ser do carrossel)
            mdv = re.search(r"data-variants=(?:\"([^\"]*)\"|'([^']*)')", h)
            try:
                variants = json.loads(html.unescape(mdv.group(1) or mdv.group(2))) if mdv else []
            except ValueError:
                variants = []
        mt = re.search(r'<meta property="og:title" content="([^"]*)"', h)
        nome = html.unescape(mt.group(1)) if mt else None
        if not nome:
            continue
        n += 1
        md = re.search(r'(?is)<div[^>]*class="[^"]*(?:product-description|description)[^"]*"[^>]*>(.*?)</div>\s*</div>', h)
        desc = html_txt(md.group(1)) if md else ""
        ogd = re.search(r'<meta property="og:description" content="([^"]*)"', h)
        texto = corta_relacionados(nome + "\n" + (desc or (html.unescape(ogd.group(1)) if ogd else "")))
        bl = bloco_produto_html(h, nome)
        mo = re.search(r"(?:Opções Adicionais|Opcionais)\s*(.{0,600}?)(?:Total com opcionais|Comprar|$)", bl)
        extra = {"opcionais_texto": mo.group(1).strip()[:500] if mo else None}
        if not variants:
            c.add(None, nome, u, None, None, None, None, None, variante="", texto=texto, extra=extra)
            continue
        for v in variants:
            opts = [v.get("option%d" % j) for j in range(3) if v.get("option%d" % j)]
            parc = None
            try:
                inst = json.loads(v.get("installments_data") or "{}")
                for _gw, tab in inst.items():
                    sj = [int(k) for k, x in tab.items() if x.get("without_interests")]
                    if sj:
                        parc = max(sj)
                        break
            except (ValueError, AttributeError):
                pass
            pix = brl(v.get("price_with_payment_discount_short"))
            por = None if v.get("contact") else fnum(v.get("price_number"))
            c.add(v.get("product_id") or u, nome, u, por, fnum(v.get("compare_at_price_number")),
                  pix if pix and por and pix < por else None, parc, bool(v.get("available")), sku=v.get("sku") or v.get("id"),
                  variante=" | ".join(opts), texto=texto, extra=dict(extra, variante_id=v.get("id"), sob_consulta=bool(v.get("contact"))))
    return n


# ---- 9) Vnda: listagens (todos, variante padrão) + data-variant (rodízio) ----
def _slug(t):
    return re.sub(r"[^a-z0-9]+", "-", norm(t)).strip("-")


def _vnda_boxes(h):
    """[(id numérico, href)] dos cards da listagem, na ordem da página."""
    out = []
    partes = h.split('data-product-box="')
    for pt in partes[1:]:
        mi = re.match(r"(\d+)\"", pt)
        mh = re.search(r'href="(/produto/[^"]+)"', pt)
        if mi and mh:
            out.append((mi.group(1), mh.group(1)))
    return out


def _vnda_casa_box(nome, href):
    """O card é do item do dataLayer? (primeiras palavras do nome no slug da URL)"""
    pal = [w for w in _slug(nome).split("-") if len(w) > 2][:3]
    sl = href.rsplit("/", 1)[-1]
    return bool(pal) and sum(1 for w in pal if w in sl) >= min(2, len(pal))


def _vnda_listagem(c, base):
    """Itens da listagem com o id numérico do Vnda (o mesmo do sufixo -<id> da URL) e o link real do card.
    O dataLayer (preço, referência, SKU da variante padrão) e os cards (id, href) seguem a mesma ordem na página."""
    itens, sem_card = {}, 0
    for cam in c.p.get("listagens") or []:
        pg = 1
        while pg <= 15:
            try:
                h = c.f.texto(base + cam + ("?page=%d" % pg if pg > 1 else ""))
            except requests.HTTPError:
                break
            boxes = _vnda_boxes(h)
            dl = []
            for m in re.finditer(r'\{"item_name":"((?:[^"\\]|\\.)*)","item_reference":"([^"]*)","item_id":"([^"]*)","price":([\d.]+),'
                                 r'"item_variant":"([^"]*)","item_variant_id":(\d+)', h):
                dl.append(dict(nome=json.loads('"%s"' % m.group(1)), ref=m.group(2), preco=float(m.group(4)), sku=m.group(5),
                               var_id=m.group(6), cam=cam))
            achou = 0
            usados = set()
            for k, it in enumerate(dl):
                par = boxes[k] if len(boxes) == len(dl) and _vnda_casa_box(it["nome"], boxes[k][1]) else None
                if par is None:  # ordem não bate: procura o card pelo nome no slug
                    par = next((b for b in boxes if b[0] not in usados and _vnda_casa_box(it["nome"], b[1])), None)
                if par is None:
                    sem_card += 1
                    continue
                usados.add(par[0])
                if par[0] not in itens:
                    itens[par[0]] = dict(it, vnda_id=par[0], href=base + par[1])
                    achou += 1
            if achou == 0 or not re.search(r"\?page=%d" % (pg + 1), h):
                break
            pg += 1
    if sem_card:
        c.f.notas.append("listagem: %d item(ns) sem card/URL própria (não gravados)" % sem_card)
    return itens


_RX_VNDA_ID = re.compile(r"-(\d+)/?(?:\?.*)?$")


def _vnda_kit(texto_variante, sku=""):
    """Variante com kit de instalação (base + suporte nivelador), que a BBB não inclui: (True/False/None, acréscimo)."""
    t = norm(texto_variante)
    mk = re.search(r"kit[^:|]*:\s*([^|]+)", t)
    val = mk.group(1) if mk else ""
    if re.search(r"\bsem kit\b", val or t):
        return False, None
    if val and re.search(r"com kit|\+\s*r\$|\boff\b", val):
        ma = re.search(r"\+\s*r\$\s*([\d.,]+)", val)
        return True, (brl(ma.group(1)) if ma else None)
    s_ = str(sku or "").lower()
    if s_.endswith("semkit"):
        return False, None
    if s_.endswith("combokit") or s_.endswith("oferta"):
        return True, None
    return None, None


def ad_vnda_data_variant(c):
    base = c.cfg["endpoint"].rstrip("/")
    itens = _vnda_listagem(c, base)
    sm_urls = []
    try:
        sm_urls = sitemap_urls(c, base + "/sitemap.xml", r"/produto/")
    except (Bloqueado, requests.RequestException):
        pass
    todas = list(dict.fromkeys([it["href"] for it in itens.values()] + sm_urls))
    todas = [u for u in todas if classifica(u.split("/produto/")[-1].replace("-", " ")) in c.cats]
    # rodízio diário das páginas de detalhe (respeitando o Crawl-delay)
    lim = int(c.p.get("max_detalhes", 80))
    todas.sort()
    dia = datetime.date.fromisoformat(c.data).toordinal()
    if todas and len(todas) > lim:
        off = (dia * lim) % len(todas)
        hoje = (todas[off:] + todas[:off])[:lim]
        c.f.notas.append("rodízio: detalhe de %d de %d produtos hoje; os demais só com a variante padrão da listagem" % (lim, len(todas)))
    else:
        hoje = todas
    ids_detalhados = set()
    for u in hoje:
        try:
            h = c.f.texto(u)
        except (requests.HTTPError,) as e:
            continue
        except Corte:
            c.f.notas.append("corte por tempo no detalhe")
            break
        ld = next((x for x in ld_json_blocos(h) if ld_tipo(x, "Product")), {})
        m = re.search(r'data-variant="([^"]*)"', h)
        try:
            variants = json.loads(html.unescape(m.group(1))) if m else []
        except ValueError:
            variants = []
        nome = ld.get("name") or (variants[0].get("name") if variants else None)
        if not nome:
            continue
        # id estável = número no fim da URL do produto (o mesmo do data-product-box da listagem)
        mid = _RX_VNDA_ID.search(u)
        vid = mid.group(1) if mid else None
        b = html_txt(re.sub(r"(?is)<(header|nav|footer|aside)[^>]*>.*?</\1>", " ", h))
        mi = re.search(r"Itens d[oa] .{0,40}?Característica\s*Descrição(.{0,2000}?)(?:Planta Baixa|Ficha de Dados|Veja mais|NOTAS)", b, re.S)
        texto = corta_relacionados(nome + "\n" + (ld.get("description") or "") + "\n" + (mi.group(1) if mi else ""))
        combo = bool(re.search(r"\bcombo\b", norm(nome)))
        for v in variants:
            props = {pp.get("name"): pp.get("value") for pp in (v.get("properties") or {}).values() if pp and pp.get("value")}
            vt = " | ".join("%s: %s" % kv for kv in props.items()) or (v.get("name") or "")
            inst = [x for x in (v.get("installments") or []) if not x.get("interest")]
            sku = v.get("sku") or ""
            kit, acr = _vnda_kit(vt, sku)
            ex = {"fonte_preco": "data-variant", "inclui_kit_instalacao": kit, "kit_acrescimo": acr, "combo": combo or None}
            c.add(vid or u, nome, u, fnum(v.get("sale_price")) or fnum(v.get("price")), fnum(v.get("price")), None,
                  max(x["number"] for x in inst) if inst else None, v.get("available"), sku=sku, variante=vt, texto=texto,
                  extra=ex)
        if vid and variants:
            ids_detalhados.add(vid)
    for vid, it in itens.items():
        if vid in ids_detalhados:
            continue
        vt = VARIANTE_HIST.get((c.cfg["fonte_id"], it["sku"])) or ("padrao: " + it["sku"])
        kit, acr = _vnda_kit(vt, it["sku"])
        c.add(vid, it["nome"], it["href"], it["preco"], None, None, None, None, sku=it["sku"], variante=vt, texto=it["nome"],
              extra={"fonte_preco": "listagem (variante padrão)", "inclui_kit_instalacao": kit, "kit_acrescimo": acr,
                     "combo": bool(re.search(r"\bcombo\b", norm(it["nome"]))) or None, "referencia": it["ref"]},
              metodo="vnda_data_variant:listagem")
    if c.data <= "2026-11-07":
        c.f.notas.append("migração: produto_id passou a ser o id numérico do Vnda (sufixo da URL) em 31/10/2026; "
                         "novo/retirado desta fonte nesta semana não é real")
    return len(set(list(itens.keys()) + list(ids_detalhados)))


# ---- 10) Sitemap/listagem + JSON-LD Product (genérico) ----
def ad_sitemap_jsonld(c):
    ep = c.cfg["endpoint"]
    rx = c.p.get("url_regex")
    if c.p.get("listagem"):
        urls = []
        pag = c.p.get("paginacao")  # ex.: "pagina" -> ?pagina=N
        for lst in (c.p.get("listagens") or [ep]):
            for n_pg in range(1, int(c.p.get("max_pg_listagem", 8)) + 1 if pag else 2):
                lu = lst if n_pg == 1 else lst + ("&" if "?" in lst else "?") + "%s=%d" % (pag, n_pg)
                try:
                    h = c.f.texto(lu)
                except requests.HTTPError:
                    break
                antes = len(set(urls))
                for x in ld_json_blocos(h):
                    if ld_tipo(x, "ItemList"):
                        for el in x.get("itemListElement") or []:
                            u = el.get("url") or (el.get("item") or {}).get("url") if isinstance(el, dict) else None
                            if u:
                                urls.append(urljoin(lu, u))
                urls += [urljoin(lu, html.unescape(u)) for u in re.findall(r'href="([^"]*%s[^"]*)"' % (rx or "/produto/"), h)]
                if len(set(urls)) == antes:
                    break
        urls = list(dict.fromkeys(u.split("#")[0] for u in urls))
    else:
        urls = sitemap_urls(c, ep, rx)
    lim = int(c.p.get("max_paginas", 300))
    if len(urls) > lim and c.p.get("rodizio"):
        # rodízio diário: cada dia começa num ponto diferente da lista; o catálogo inteiro roda em poucos dias
        urls.sort()
        tot = len(urls)
        off = (datetime.date.fromisoformat(c.data).toordinal() * lim) % tot
        urls = (urls[off:] + urls[:off])[:lim]
        c.f.notas.append("rodízio: %d de %d páginas hoje; catálogo completo a cada %d dias" % (lim, tot, -(-tot // lim)))
    elif len(urls) > lim:
        c.f.notas.append("corte: %d de %d páginas" % (lim, len(urls)))
        urls = urls[:lim]
    n, bloqueios = 0, 0
    for u in urls:
        try:
            h = c.f.texto(u)
        except requests.HTTPError:
            continue
        except Bloqueado:
            bloqueios += 1
            if bloqueios >= 2:
                raise
            continue
        prods = [x for x in ld_json_blocos(h) if ld_tipo(x, "Product")]
        if not prods:
            continue
        # o produto da página (não o do carrossel de relacionados): URL que bate, senão o nome do <h1>/og:title
        def _url_ld(x):
            o = x.get("offers")
            o = (o[0] if isinstance(o, list) and o else o) if o else {}
            return str(x.get("url") or (o.get("url") if isinstance(o, dict) else "") or "")
        chave_u = u.rstrip("/").split("/")[-1][:40]
        mh = re.search(r'(?is)<h1[^>]*>(.*?)</h1>', h) or re.search(r'<meta property="og:title" content="([^"]*)"', h)
        titulo = norm(html_txt(mh.group(1)))[:40] if mh else ""
        prod = (next((x for x in prods if chave_u and chave_u in _url_ld(x)), None)
                or next((x for x in prods if titulo and norm(x.get("name") or "")[:40] == titulo), None) or prods[0])
        nome = html.unescape(prod.get("name") or "")
        n += 1
        texto = corta_relacionados(nome + "\n" + html_txt(prod.get("description") or "") + "\n" + bloco_produto_html(h, nome, 4000))
        offers = prod.get("offers") or []
        offers = offers if isinstance(offers, list) else [offers]
        mv = re.search(r'data-product_variations="([^"]*)"', h)
        variacoes = []
        if mv:
            try:
                variacoes = json.loads(html.unescape(mv.group(1)))
            except ValueError:
                variacoes = []
        sku = prod.get("sku") or prod.get("productID")
        extra = {"marca": (prod.get("brand") or {}).get("name") if isinstance(prod.get("brand"), dict) else prod.get("brand")}
        if not extrai_forma(nome):
            # forma que o nome não diz: categoria/tag do próprio produto (Acqualine Triângulo Mineiro:
            # product_cat-banheiras-de-canto-basicas) ou nome do arquivo da imagem principal
            img = prod.get("image")
            img = img[0] if isinstance(img, list) and img else img
            img = (img.get("url") or img.get("contentUrl") or "") if isinstance(img, dict) else str(img or "")
            cls = _classes_produto_html(h)
            fe = forma_evidencia(cls) or forma_evidencia(urlparse(img).path.rsplit("/", 1)[-1])
            if fe:
                extra["forma"] = fe
                extra["forma_origem"] = "categoria do produto" if forma_evidencia(cls) else "nome da imagem do produto"
        # Product.size do JSON-LD (Aquasol: "2,00 x 2,00 x 0,67", a variante Tamanho da página): é a medida do produto
        base_ld = {}
        md_ld = list(dict.fromkeys(extrai_medidas(str(prod.get("size") or ""))))
        if len(md_ld) == 1:
            base_ld["comprimento_m"], base_ld["largura_m"], base_ld["profundidade_m"] = md_ld[0]
        if isinstance(variacoes, list) and variacoes:
            for v in variacoes:
                vt = " | ".join("%s: %s" % (k.replace("attribute_", "").replace("pa_", ""), val) for k, val in (v.get("attributes") or {}).items() if val)
                dp, dr = fnum(v.get("display_price")), fnum(v.get("display_regular_price"))
                c.add(sku or u, nome, u, dp, dr if dr and dp and dr > dp else None, None, None, v.get("is_in_stock"),
                      sku=v.get("sku") or v.get("variation_id"), variante=vt, texto=texto, extra=dict(extra, variacao_id=v.get("variation_id")), attrs_base=base_ld)
            continue
        ofs = [o for o in offers if isinstance(o, dict)]
        if len(ofs) > 1 and any(o.get("name") or o.get("sku") for o in ofs):
            for o in ofs:
                c.add(sku or u, nome, o.get("url") or u, brl(o.get("price") or o.get("lowPrice")), None, None, None,
                      "InStock" in str(o.get("availability")), sku=o.get("sku") or sku, variante=o.get("name") or o.get("sku") or "",
                      texto=texto, extra=extra, attrs_base=base_ld)
            continue
        o = ofs[0] if ofs else {}
        preco = brl(o.get("price") or o.get("lowPrice"))
        if o.get("highPrice") and brl(o.get("highPrice")) != preco:
            extra["preco_max_variacoes"] = brl(o.get("highPrice"))
        bloco = bloco_produto_html(h, nome, 6000)
        pix = _pix_html(bloco, preco) if preco else None
        parc = _parcelas_html(bloco)
        de, de_igual = None, False
        if preco and not c.p.get("jsonld_preco_e_pix"):
            pct = _pix_percentual_html(bloco) if pix is None else None  # "Ganhe 5% de desconto no Pix" (ABC)
            ps = o.get("priceSpecification") if isinstance(o.get("priceSpecification"), dict) else {}
            cartao = _por_ja_e_pix(bloco, preco, pix, pct, brl(ps.get("maxPrice")) if ps.get("maxPrice") else None)
            if cartao:
                # o "Por" já é o Pix: não desconta de novo; o preço de cartão vira o "por" e o "De" do bloco vai para preco_de
                extra.update(preco_por_origem="cartão (bloco do produto); o 'Por' da página já é o preço do Pix",
                             pix_origem="'Por' da página" + (" (= %s%% sobre o cartão)" % pct if pct else ""))
                pix, preco = preco, cartao
                mo = re.search(r"(?i)\bde\s*:?\s*" + _RS, _junta_valor(html_txt(bloco)))
                de = brl(mo.group(1)) if mo else None
                de_igual = True
            elif pix is None and pct:
                pix = round(preco * (1 - pct / 100.0), 2)
                extra["pix_origem"] = "desconto do Pix publicado na página (%s%%)" % pct
        if c.p.get("jsonld_preco_e_pix") and preco:
            # Aquasol (Climba): o offers.price do JSON-LD é o preço Pix; o preço de cartão é o "preco-atual" do bloco
            # do produto (o 1º antes de "Mais formas de pagamento")
            mc = (re.search(r'(?is)class="preco-atual[^"]*"[^>]*>\s*' + _RS, h)
                  or re.search(_RS + r"\s*\n?\s*Mais formas de pagamento", bloco))
            cartao = brl(mc.group(1)) if mc else None
            px = _pix_html(bloco, cartao) if cartao else None
            if cartao and px and abs(px - preco) < 0.02 and cartao > preco:
                extra.update(preco_por_origem="cartão (bloco do produto); JSON-LD offers.price = Pix")
                preco, pix = cartao, px
                mo = re.search(r'(?is)class="preco-(?:antigo|de)[^"]*"[^>]*>\s*(?:<[^>]+>\s*)*' + _RS, h)
                de = brl(mo.group(1)) if mo else None
            elif cartao is None:
                c.f.notas.append("preço de cartão não achado em %s" % urlparse(u).path[:60])
        c.add(sku or u, nome, u, preco, de, pix, parc, ("InStock" in str(o.get("availability"))) if o else None, sku=sku,
              variante="", texto=texto, extra=extra, attrs_base=base_ld, de_igual_ok=de_igual)
    return n


# ---- 11) MadeiraMadeira: vitrine do lojista ----
def ad_madeiramadeira_seller(c):
    ep = c.cfg["endpoint"]
    n, pg = 0, 1
    while pg <= 10:
        h = c.f.texto(ep + ("?page=%d" % pg if pg > 1 else ""))
        m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', h, re.S)
        if not m:
            raise RuntimeError("sem __NEXT_DATA__ na vitrine do lojista")
        pp = json.loads(m.group(1))["props"]["pageProps"]
        raws = (pp.get("resultsState") or {}).get("rawResults") or []
        hits = raws[0].get("hits") if raws else []
        nb = raws[0].get("nbPages", 1) if raws else 1
        for hit in hits or []:
            n += 1
            nome = hit.get("nome")
            mb = (hit.get("mini_buy_box") or [{}])[0]
            larg, alt, prof = fnum(hit.get("largura")), fnum(hit.get("altura")), fnum(hit.get("profundidade"))
            base = {}
            if larg and alt and 0.3 < larg < 4.5 and 0.3 < alt < 4.5:
                base.update(comprimento_m=max(larg, alt), largura_m=min(larg, alt), profundidade_m=prof if prof and prof < 2 else None)
            texto = corta_relacionados(nome + "\n" + html_txt(hit.get("descricao") or ""))
            # parcelas sem juros e Pix: parcelamento detalhado do buy box (o mesmo que a página do produto mostra)
            pdet = mb.get("parcelamento_detalhado") or {}
            usj = ((pdet.get("ultima_parcela_semjuros") or {}).get("parcela") or {}).get("parcela")
            parc = int(usj) if usj and int(usj) > 1 else None
            pix = fnum(hit.get("preco_promocional"))
            obs_pix = "preco_pix = preço à vista do marketplace (preco_promocional)"
            try:
                ip = ((pdet.get("metodos") or {}).get("pix") or {}).get("indice")
                tab = (pdet.get("indice_parcelamentos") or [])[int(ip)]["parcelamento"]["1"]
                if fnum(tab.get("valor")):
                    pix, obs_pix = fnum(tab.get("valor")), "preco_pix = 1x no Pix do parcelamento do buy box"
            except (TypeError, ValueError, IndexError, KeyError):
                pass
            c.add(hit.get("id_produto"), nome, urljoin("https://www.madeiramadeira.com.br", hit.get("url") or ""),
                  fnum(hit.get("preco_por")), fnum(hit.get("preco_de")), pix, parc,
                  not hit.get("out_of_stock"), sku=hit.get("id_produto"), variante="", texto=texto, attrs_base=base,
                  extra={"vendedor": hit.get("nome_seller"), "marca": hit.get("marca"), "preco_12x": fnum(hit.get("preco_12")),
                         "pct_desconto_avista": mb.get("percentual_avista"), "atributos_loja": hit.get("atributos"),
                         "obs_pix": obs_pix},
                  cat_extra=hit.get("nivel_2") or "", dominio="madeiramadeira.com.br")
        if pg >= nb:
            break
        pg += 1
    return n


# ---- 12) WordPress REST (lista) + HTML do produto com preço em texto (Versátil) ----
def ad_wp_rest_html(c):
    ep = c.cfg["endpoint"]
    itens, pg = [], 1
    while pg <= 10:
        r = c.f.get(ep, params={"per_page": 100, "page": pg, "_fields": "id,link,title,modified"})
        d = r.json()
        itens += d
        tot = int(r.headers.get("X-WP-TotalPages") or 1)
        if pg >= tot:
            break
        pg += 1
    n = 0
    for it in itens:
        u = it.get("link")
        nome = html_txt((it.get("title") or {}).get("rendered") or "")
        if classifica(nome) not in c.cats:
            continue
        try:
            h = c.f.texto(u)
        except requests.HTTPError:
            continue
        n += 1
        # tabela de versões (Premium | Luxo): lê a coluna da versão do nome do produto; a tabela sai do texto livre
        versao = _versatil_versao(h, nome)
        bl = bloco_produto_html(re.sub(r"(?is)<table.*?</table>", " ", h) if versao else h, nome)
        tb = norm(bl)
        m = re.search(r"r\$\s*([\d.]+(?:,\d{2})?)\s*a vista\s*ou\s*r\$\s*([\d.]+(?:,\d{2})?)\s*em\s*(\d{1,2})\s*x", tb)
        avista = cart = parc = None
        if m:
            avista, cart, parc = brl(m.group(1)), brl(m.group(2)), int(m.group(3))
            sj = re.search(r"em\s*%d\s*x\s*de\s*r\$\s*[\d.,]+\s*sem juros" % parc, tb)
            parc = parc if sj else None
        else:
            mp = re.search(r"r\$\s*([\d.]+,\d{2})", tb)
            cart = brl(mp.group(1)) if mp else None
        extra = {"preco_a_vista": avista, "modificado_em": it.get("modified")}
        base = {}
        if versao:
            col, tab = versao
            base, ex_v = _versatil_attrs(tab)
            extra.update(ex_v, versao_tabela=col)
            nt = norm(bl)
            mi = re.search(r"\*\s*([^*\n]{0,40}instalacao inclusa[^\n]{0,160})", nt)
            if mi and extra.get("instalacao_inclusa"):
                extra["instalacao_condicao"] = mi.group(1).strip()[:200]
        c.add(it.get("id"), nome, u, cart, None, None, parc, None, sku=it.get("id"), variante="", texto=bl,
              attrs_base=base, extra=extra)
    return n


def _versatil_versao(h, nome):
    """(coluna, {linha: valor}) da tabela de versões cujo cabeçalho é a versão citada no nome ('Afrodite Luxo')."""
    mt = re.search(r"(?is)<table[^>]*>(.*?)</table>", h or "")
    if not mt:
        return None
    th = [html_txt(x) for x in re.findall(r"(?is)<th[^>]*>(.*?)</th>", mt.group(1))]
    if len(th) < 2:
        return None
    nn = norm(nome)
    col = None
    for k, x in enumerate(th):
        if k and norm(x) and re.search(r"\b%s\b" % re.escape(norm(x)), nn):
            col = k
    if col is None:
        return None
    tab = {}
    for tr in re.findall(r"(?is)<tr[^>]*>(.*?)</tr>", mt.group(1)):
        tds = [html_txt(x) for x in re.findall(r"(?is)<td[^>]*>(.*?)</td>", tr)]
        if len(tds) > col:
            tab[tds[0]] = tds[col]
    return th[col], tab


def _versatil_attrs(tab):
    """Atributos da coluna da versão: jatos (soma das linhas de jatos), motobomba, aquecedor, cromo, instalação."""
    base, ex = {}, {}
    jatos, mini = 0, 0
    tem_jato = False
    for k, v in tab.items():
        nk, nv = norm(k), norm(v)
        num = re.match(r"^(\d{1,2})$", nv)
        if "jato" in nk:
            tem_jato = True
            if num:
                if "mini" in nk:
                    mini += int(num.group(1))
                else:
                    jatos += int(num.group(1))
        elif re.search(r"moto ?bomba|motor", nk):
            if nv and nv not in ("-", "nao", "\u2013"):
                base["motores"] = 1
                cv = extrai_cv(nv)
                if cv:
                    base["potencia_cv"] = cv
        elif re.search(r"aquecedor|aquecimento", nk):
            base["aquecedor_incluso"] = True if nv.startswith("sim") else (False if nv in ("nao", "-", "") else None)
        elif re.search(r"cromo|led|ilumina", nk):
            base["cromo_incluso"] = True if nv.startswith("sim") else (False if nv in ("nao", "-", "") else None)
        elif "instalac" in nk:
            ex["instalacao_inclusa"] = True if nv.startswith("sim") else (False if nv in ("nao", "-", "") else None)
    if tem_jato:
        base["jatos"] = jatos or None
        base["mini_jatos"] = mini or None
    return base, ex


# ---- 13) OpenCart HTML (Cooper Fibra) ----
def _opencart_produto(c, u, hp, pid=None):
    mt = re.search(r"(?is)<h1[^>]*>(.*?)</h1>", hp)
    if not mt:
        return False
    nome = html_txt(mt.group(1))
    pid = pid or (re.search(r'name="product_id" value="(\d+)"', hp) or re.search(r"product_id=(\d+)", hp) or [None, u])[1]
    mp = (re.search(r'(?is)<h2 class="price[^"]*">(.*?)</h2>', hp) or re.search(r"(?is)<ul class=\"list-unstyled\">\s*<li>\s*<h2>(.*?)</h2>", hp)
          or re.search(r"(?is)<h2>\s*(R\$[^<]+)</h2>", hp))
    preco = brl(html_txt(mp.group(1))) if mp else None
    # preço "de": <span class="price-old"> ou riscado por estilo inline (O Magnata: <span style="text-decoration: line-through;">)
    zona = hp[mt.start():mt.start() + 8000]  # bloco do produto a partir do <h1> (os relacionados vêm depois)
    mo = re.search(r'(?is)<span class="price-old">(.*?)</span>|<span[^>]*line-through[^>]*>(.*?)</span>', zona)
    de = brl(html_txt(mo.group(1) or mo.group(2))) if mo else None
    parc = re.search(r"(?is)at[eé]\s*(?:<[^>]+>\s*)*(\d{1,2})x(?:\s*<[^>]+>)*\s*de.{0,200}?sem juros", hp)
    # Pix: "À vista por <b>R$ 9.990,00</b> (no <b>PIX</b> com 10% de Desconto)", só no bloco do preço
    pix = _pix_html(zona, preco) if preco else None
    md = re.search(r'(?is)<div class="tab-pane active" id="tab-description">(.*?)</div>', hp)
    texto = corta_relacionados(nome + "\n" + (html_txt(md.group(1)) if md else bloco_produto_html(hp, nome, 3000)))
    extra = {}
    # forma pela pasta das fotos do produto no catálogo da loja (".../Banheiras/Banheira Freestanding/Banheira Saturnia/...")
    fotos = re.findall(r'(?is)<a class="thumbnail" href="([^"]+)"', hp)
    if any(re.search(r"(?i)/banheiras? freestanding/", x) for x in fotos[:6]):
        extra.update(forma="Freestanding", forma_origem="pasta das fotos do produto na loja")
    elif any(re.search(r"(?i)/banheiras? vitorianas?/", x) for x in fotos[:6]):
        extra.update(forma="Vitoriana", forma_origem="pasta das fotos do produto na loja")
    mm = re.search(r"(?is)<li>\s*Marca:\s*(?:<a[^>]*>)?\s*([^<]+)", hp)
    if mm:
        extra["marca"] = html_txt(mm.group(1))
    md_ = re.search(r"(?is)Disponibilidade:\s*(?:<[^>]+>\s*)*([^<\n]{2,40})", zona)
    disp = None
    if md_:
        dv = norm(md_.group(1))
        disp = True if re.search(r"em estoque|disponivel|imediat|pronta entrega", dv) else (
            False if re.search(r"esgotad|fora de estoque|indisponivel|sem estoque", dv) else None)
        if disp is None:
            extra["disponibilidade_texto"] = md_.group(1).strip()[:40]  # "2-3 dias", "Sob encomenda": não decide
    return c.add(pid, nome, u, preco, de, pix, parc.group(1) if parc else None, disp,
                 sku=pid, variante="", texto=texto, extra=extra)


def ad_opencart_html(c):
    base = c.cfg["endpoint"].rstrip("/")
    if c.p.get("sitemap"):  # OpenCart com URL amigável: feed/google_sitemap lista os produtos
        urls = sitemap_urls(c, c.p["sitemap"], c.p.get("url_regex"))
        n = 0
        for u in urls[: int(c.p.get("max_paginas", 300))]:
            try:
                hp = c.f.texto(u)
            except requests.HTTPError:
                continue
            if 'name="product_id"' not in hp:
                continue
            n += 1
            _opencart_produto(c, u, hp)
        return n
    h = c.f.texto(base + "/")
    cats = list(dict.fromkeys(html.unescape(x) for x in re.findall(r'href="([^"]*route=product/category&(?:amp;)?path=[\d_]+)"', h)))
    prods = set(re.findall(r"product_id=(\d+)", h))
    for cu in cats[:40]:
        try:
            hc = c.f.texto(urljoin(base + "/", cu))
        except (requests.HTTPError, Bloqueado):
            continue
        prods |= set(re.findall(r"product_id=(\d+)", hc))
    n = 0
    for pid in sorted(prods, key=int):
        u = base + "/index.php?route=product/product&product_id=" + pid
        try:
            hp = c.f.texto(u)
        except (requests.HTTPError, Bloqueado):
            continue
        mt = re.search(r"(?is)<h1[^>]*>(.*?)</h1>", hp)
        if not mt:
            continue
        nome = html_txt(mt.group(1))
        mp = re.search(r"(?is)<ul class=\"list-unstyled\">\s*<li>\s*<h2>(.*?)</h2>", hp) or re.search(r"(?is)<h2>\s*(R\$[^<]+)</h2>", hp)
        preco = brl(html_txt(mp.group(1))) if mp else None
        md = re.search(r'(?is)<div class="tab-pane active" id="tab-description">(.*?)</div>', hp)
        texto = corta_relacionados(nome + "\n" + (html_txt(md.group(1)) if md else bloco_produto_html(hp, nome, 3000)))
        n += 1
        c.add(pid, nome, u, preco, None, None, None, None, sku=pid, variante="", texto=texto)
    return n


# ---- 14) Cliquemania: listadeprodutos.php ----
def _cliquemania_item(h, preco):
    """Página do item (Personalizze): bloco 'Descrição' (Formato, Nº de pessoas, jatos, potência), Pix e opcionais pagos."""
    t = html_txt(re.sub(r"(?is)<(header|nav|footer|aside)[^>]*>.*?</\1>", " ", h))
    md = re.search(r"(?s)\nDescri[cç][aã]o\n(.*?)(?:\nEspecifica[^\n]{0,3}o t[^\n]{0,10}cnica|\nV[ií]deos\n|\nProdutos Relacionados|"
                   r"\nComent[aá]rios\n|\nTamb[eé]m Viram|$)", t)
    desc = md.group(1)[:5000] if md else ""
    i_desc = md.start() if md else len(t)
    topo = t[:i_desc]
    pix = _pix_html(topo, preco) if preco else None
    opc = []
    mo = re.search(r"(?s)OP[CÇ][OÕ]ES ADICIONAIS\n(.*?)(?:\nCompre em quantidade|\nQtde|\nComprar)", topo)
    if mo:
        linhas = [x.strip() for x in mo.group(1).split("\n") if x.strip()]
        for k in range(len(linhas) - 1):
            mp = re.match(r"^\+\s*R\$\s*([\d.,]+)$", linhas[k + 1])
            if mp and not linhas[k].startswith("+"):
                opc.append({"grupo": "OPÇÕES ADICIONAIS", "item": linhas[k].strip('"')[:80], "preco": brl(mp.group(1))})
    return desc, pix, opc


def _cliquemania_tamanho_padrao(h):
    """Medida da opção 'TAMANHO: 1.53 x 0.81 x 0.54' com '+ R$0,00' (a configuração do preço anunciado)."""
    t = html_txt(re.sub(r"(?is)<(header|nav|footer|aside)[^>]*>.*?</\1>", " ", h))
    for m in re.finditer(r"(?im)^TAMANHO\s*:\s*([^\n]{5,40})\n\+\s*R\$\s*0,00\s*$", t):
        md = extrai_medidas(m.group(1))
        if md and md[0][1] is not None:
            return md[0]
    return None


def ad_cliquemania_lista(c):
    h = c.f.texto(c.cfg["endpoint"])
    n = 0
    linhas = []
    for tr in re.findall(r"(?is)<tr[^>]*>(.*?)</tr>", h):
        tds = re.findall(r"(?is)<td[^>]*>(.*?)</td>", tr)
        if len(tds) < 6:
            continue
        mu = re.search(r'href="([^"]+)"', tds[2])
        nome, cat, desc = html_txt(tds[2]), html_txt(tds[3]), html_txt(tds[4])
        preco = brl(html_txt(tds[5]))
        if classifica(nome, cat) not in c.cats:
            continue
        if any(x[0] == html_txt(tds[1]) for x in linhas):
            continue  # o mesmo item aparece em mais de uma categoria da lista
        linhas.append((html_txt(tds[1]), nome, urljoin(c.cfg["endpoint"], html.unescape(mu.group(1))) if mu else None, preco, cat, desc))
    # página do item (Pix, Formato, Nº de pessoas, jatos da ficha): rodízio diário quando passa do limite
    detalhe = set()
    if c.p.get("detalhe_itens"):
        urls = sorted(set(x[2] for x in linhas if x[2] and x[3]))
        lim = int(c.p.get("max_detalhes", 200))
        if len(urls) > lim:
            off = (datetime.date.fromisoformat(c.data).toordinal() * lim) % len(urls)
            urls = (urls[off:] + urls[:off])[:lim]
            c.f.notas.append("rodízio: página do item de %d de %d produtos hoje (Pix, formato e pessoas só nesses)" % (lim, len(set(x[2] for x in linhas if x[2]))))
        detalhe = set(urls)
    # 1ª passada: páginas do item do dia (Pix, ficha, opcionais, tamanho padrão)
    det = {}
    for sku, nome, url, preco, cat, desc in linhas:
        if url not in detalhe:
            continue
        try:
            h = c.f.texto(url)
            det[url] = _cliquemania_item(h, preco) + (_cliquemania_tamanho_padrao(h),)
        except (requests.HTTPError, Bloqueado):
            pass
        except Corte:
            detalhe = set()
            c.f.notas.append("corte por tempo nas páginas de item")
    # Pix dos itens fora do rodízio do dia: percentual da loja, lido nas páginas de item de hoje quando é o mesmo em
    # todas (Personalizze: 2%; 208 de 424 linhas ficavam sem Pix; reconferência r4 preços #24). O valor "Pix" dos cards
    # de listagem não serve: o rótulo vem trocado (o número maior aparece como Pix).
    preco_url = {x[2]: x[3] for x in linhas}
    pcts = [round((1 - d[1] / preco_url[u]) * 100, 1) for u, d in det.items() if d[1] and preco_url.get(u) and d[1] < preco_url[u]]
    pct_loja = None
    if len(pcts) >= 2 and max(pcts) - min(pcts) <= 0.2 and 0 < min(pcts) and max(pcts) <= 15:
        pct_loja = round(sorted(pcts)[len(pcts) // 2], 1)
    for sku, nome, url, preco, cat, desc in linhas:
        n += 1
        texto, pix, extra = corta_relacionados(nome + "\n" + desc), None, {"categoria_loja": cat}
        base = {}
        if url in det:
            d_desc, pix, opc, tam = det[url]
            if d_desc:
                texto = nome + "\n" + d_desc
                extra["fonte_atributos"] = "pagina do item"
            if opc:
                extra["opcionais"] = opc
            if pix:
                extra["pix_origem"] = "pagina do item"
            if tam:
                # medida padrão: a 1ª opção "TAMANHO: ... + R$ 0,00" (a ficha lista todas as medidas da linha)
                base["comprimento_m"], base["largura_m"], base["profundidade_m"] = tam
                extra["medida_origem"] = "opção TAMANHO sem acréscimo"
        elif pct_loja is not None and preco:
            pix = round(preco * (1 - pct_loja / 100.0), 2)
            extra["pix_origem"] = "percentual da loja (%s%% lido nas páginas de item de hoje)" % pct_loja
        itens_opc = norm(" ".join(o["item"] for o in extra.get("opcionais") or []))
        if "aquecedor" in itens_opc and _flag_item(nome, "", r"aquecedor(?:es)?|aquecimento") is not True:
            base["aquecedor_incluso"] = False
        if "cromo" in itens_opc and _flag_item(nome, "", r"cromoterapia|cromo|leds?\b|iluminacao") is not True:
            base["cromo_incluso"] = False
        c.add(sku, nome, url, preco, None, pix, None, None, sku=sku, variante="", texto=texto, attrs_base=base,
              extra=extra, cat_extra=cat)
    return n


# ---- 15) Hostinger (Zyro) store API ----
def ad_hostinger_store_api(c):
    ep = c.cfg["endpoint"]
    site = c.p.get("site", "")
    off, n = 0, 0
    while off < 2000:
        d = c.f.json(ep, params={"offset": off, "limit": 100})
        lote = d.get("products") or []
        for p in lote:
            nome = p.get("title") or ""
            texto = corta_relacionados(nome + "\n" + (p.get("subtitle") or "") + "\n" + html_txt(p.get("description") or ""))
            if classifica(nome + " " + (p.get("subtitle") or "")) not in c.cats:
                continue
            n += 1
            # seções "Linha Slim" / "Linha Master" / "Opcionais" (additional_info): cada variante usa a lista da sua linha
            secoes = [((ai.get("title") or "").strip(), html_txt(ai.get("description") or "")) for ai in p.get("additional_info") or []]
            opc_txt = "\n".join("Opcionais:\n" + d for t_, d in secoes if re.match(r"(?i)opcion", t_))
            linhas_sec = [(t_, d) for t_, d in secoes if not re.match(r"(?i)opcion", t_)]
            for v in p.get("variants") or []:
                vt_full = norm(v.get("title") or "")
                sec_v = [d for t_, d in linhas_sec if norm(re.sub(r"(?i)^linhas?\s+", "", t_)) and
                         re.search(r"\b%s\b" % re.escape(norm(re.sub(r"(?i)^linhas?\s+", "", t_))), vt_full)]
                if not sec_v and len(linhas_sec) == 1 and len(p.get("variants") or []) == 1:
                    sec_v = [linhas_sec[0][1]]
                texto_v = texto + ("\n" + "\n".join(sec_v) if sec_v else "") + ("\n" + opc_txt if opc_txt else "")
                if len(p.get("variants") or []) == 1 and v.get("title"):
                    texto_v = nome + "\nOpções: " + v["title"] + "\n" + texto_v  # variante única: "Slim, Gel Coat" (material)
                pr = (v.get("prices") or [{}])[0]
                dig = int(((pr.get("currency") or {}).get("decimal_digits")) or 2)
                amt = fnum(pr.get("amount"))
                sale = fnum(pr.get("sale_amount"))
                por = (sale or amt) / 10 ** dig if (sale or amt) else None
                de = amt / 10 ** dig if (sale and amt) else None
                vt = v.get("title") if len(p.get("variants") or []) > 1 else ""
                c.add(p.get("id"), nome, site + "/" + (p.get("slug") or ""), por, de, None, None, v.get("is_available"),
                      sku=v.get("sku") or v.get("id"), variante=vt or "", texto=texto_v,
                      extra={"subtitulo": p.get("subtitle"), "secao_itens": "linha da variante" if sec_v else None})
        if len(lote) < 100:
            break
        off += 100
    return n


ADAPTADORES = {
    "magento2_graphql": ad_magento2_graphql,
    "magento1_html": ad_magento1_html,
    "woo_store_api": ad_woo_store_api,
    "shopify_products_json": ad_shopify_products_json,
    "tray_web_api": ad_tray_web_api,
    "tray_html_datalayer": ad_tray_html_datalayer,
    "vtex_catalog": ad_vtex_catalog,
    "nuvemshop_sitemap_variants": ad_nuvemshop_sitemap_variants,
    "vnda_data_variant": ad_vnda_data_variant,
    "sitemap_jsonld": ad_sitemap_jsonld,
    "madeiramadeira_seller": ad_madeiramadeira_seller,
    "wp_rest_html": ad_wp_rest_html,
    "opencart_html": ad_opencart_html,
    "cliquemania_lista": ad_cliquemania_lista,
    "hostinger_store_api": ad_hostinger_store_api,
}


# ---------------------------------------------------------------------------
# Execução por concorrente, saneamento e gravação
# ---------------------------------------------------------------------------
def coleta_um(cfg, data_coleta, prazo_global):
    t0 = time.time()
    prazo = min(prazo_global, t0 + float((cfg.get("params") or {}).get("max_s", ORCAMENTO_TOTAL_S)))
    fonte = Fonte(cfg, prazo)
    col = Coletor(cfg, fonte, data_coleta)
    status, erro, n_prod = "ok", None, 0
    try:
        n_prod = ADAPTADORES[cfg["metodo"]](col) or 0
        if fonte.notas and any(x.startswith(("corte", "rodízio")) for x in fonte.notas):
            status = "parcial"
    except Bloqueado as e:
        status, erro = ("bloqueado" if not col.linhas else "parcial"), str(e)
    except Corte as e:
        status, erro = "parcial", str(e)
    except Exception as e:  # noqa: BLE001 — uma fonte com erro não derruba a coleta
        status = "parcial" if col.linhas else "erro"
        erro = "%s: %s" % (type(e).__name__, str(e)[:300])
        traceback.print_exc()
    col.finaliza()
    if not col.linhas and status == "ok":
        status, erro = "erro", (erro or "nenhum produto coletado")
    notas = "; ".join(dict.fromkeys(fonte.notas))
    log = dict(data_coleta=data_coleta, coletado_em=agora().isoformat(timespec="seconds"), empresa_slug=cfg["empresa_slug"],
               metodo=cfg["metodo"], status=status, produtos=int(len(set((r["produto_id"] or r["url"]) for r in col.linhas)) or n_prod),
               variantes=len(col.linhas), erro=" | ".join(x for x in (erro, notas) if x) or None,
               duracao_s=round(time.time() - t0, 1), fonte_id=cfg["fonte_id"], requisicoes=fonte.n_req)
    return col.linhas, log


def saneia(df, hist):
    df["status_preco"] = "ok"
    df["motivo_status"] = None
    sem = df["preco_por"].isna()
    df.loc[sem, "status_preco"] = "sem_preco"
    df.loc[sem, "motivo_status"] = "preço não publicado"
    # motivo dito pelo adaptador (VTEX: indisponível com "Avise-me", venda só na loja física)
    mot = df["atributos_json"].fillna("").astype(str).str.extract(r'"motivo_sem_preco": "([^"]*)"')[0]
    df.loc[sem & mot.notna(), "motivo_status"] = mot[sem & mot.notna()]
    principal = df["categoria"].isin(CATS_PRINCIPAIS)
    baixo = principal & ~sem & (df["preco_por"] < 500)
    alto = principal & ~sem & (df["preco_por"] > 100000)
    df.loc[baixo, ["status_preco", "motivo_status"]] = ["suspeito", "preço < R$ 500 para banheira/spa/ofurô"]
    df.loc[alto, ["status_preco", "motivo_status"]] = ["suspeito", "preço > R$ 100.000 para banheira/spa/ofurô"]
    aberta = ~sem & df["atributos_json"].fillna("").astype(str).str.contains('"variante_personalizada": true', regex=False)
    df.loc[aberta, ["status_preco", "motivo_status"]] = ["variante_aberta", "variante 'Personalize': preço de entrada de configuração aberta"]
    if CHECAR_VARIACAO and hist is not None and len(hist):
        h = hist.copy()
        h["preco_por"] = pd.to_numeric(h["preco_por"], errors="coerce")
        h = h[h["data_coleta"] < df["data_coleta"].iloc[0]].dropna(subset=["preco_por"])
        if len(h):
            h = h.sort_values("data_coleta").groupby(["fonte_id", "produto_id", "variante"], dropna=False).tail(1)
            h = h[["fonte_id", "produto_id", "variante", "preco_por", "data_coleta"]].rename(
                columns={"preco_por": "_ant", "data_coleta": "_dant"})
            for k in ("fonte_id", "produto_id", "variante"):
                h[k] = h[k].fillna("").astype(str)
            m = df[["fonte_id", "produto_id", "variante"]].fillna("").astype(str).merge(h, how="left", on=["fonte_id", "produto_id", "variante"])
            var = (df["preco_por"].values / m["_ant"].values - 1.0)
            flag = (~pd.isna(var)) & (abs(var) > 0.30) & (df["status_preco"].values == "ok")
            df.loc[flag, "status_preco"] = "suspeito"
            df.loc[flag, "motivo_status"] = ["variação de %+.0f%% contra %s" % (v * 100, d) for v, d in zip(var[flag], m["_dant"].values[flag])]
    return df


def tipa(df):
    for k in ("preco_de", "preco_por", "preco_pix", "comprimento_m", "largura_m", "profundidade_m", "potencia_cv", "garantia_casco_anos"):
        df[k] = pd.to_numeric(df[k], errors="coerce").astype("float64")
    for k in ("parcelas_sem_juros", "lugares", "jatos", "mini_jatos", "motores"):
        df[k] = pd.to_numeric(df[k], errors="coerce").round().astype("Int64")
    for k in ("disponivel", "com_hidro", "aquecedor_incluso", "cromo_incluso"):
        df[k] = df[k].map(lambda x: x if x in (True, False) else pd.NA).astype("boolean")
    for k in ("data_coleta", "coletado_em", "empresa", "empresa_slug", "canal", "dominio", "produto_id", "sku", "variante", "nome",
              "url", "categoria", "material", "atributos_json", "metodo", "status_preco", "motivo_status", "fonte_id"):
        df[k] = df[k].map(lambda x: None if x is None or (isinstance(x, float) and math.isnan(x)) else str(x)).astype("object")
    df["variante"] = df["variante"].fillna("")
    return df[COLS]


def tabela_concorrentes():
    cols = ["empresa_slug", "empresa", "tipo", "cidade_uf", "relevancia", "plataforma", "metodo", "endpoint", "tem_preco_publico",
            "ativo", "observacao", "fonte_id", "canal"]
    df = pd.DataFrame([{k: c.get(k) for k in cols} for c in CONCORRENTES])
    df["tem_preco_publico"] = df["tem_preco_publico"].astype(bool)
    df["ativo"] = df["ativo"].astype(bool)
    for k in cols:
        if k not in ("tem_preco_publico", "ativo"):
            df[k] = df[k].map(lambda x: None if x is None else str(x)).astype("object")
    return df


def main():
    t0 = time.time()
    prazo = t0 + ORCAMENTO_TOTAL_S
    data_coleta = agora().date().isoformat()
    ativos = [c for c in CONCORRENTES if c.get("ativo") and c["metodo"] in ADAPTADORES]
    if SO_FONTES:
        ativos = [c for c in ativos if c["fonte_id"] in SO_FONTES]
    # fontes lentas (crawl-delay) começam primeiro
    ativos.sort(key=lambda c: 0 if c["metodo"] in ("vnda_data_variant", "magento1_html", "nuvemshop_sitemap_variants") else 1)
    print("Coleta %s: %d fontes ativas" % (data_coleta, len(ativos)), flush=True)
    try:
        hist = nekt.load_table(layer_name="bbb_banheiras", table_name="precos_coleta")
        h = hist[hist["metodo"].astype(str).str.startswith("vnda") & hist["sku"].notna()]
        for fid, sku, var in h[["fonte_id", "sku", "variante"]].fillna("").itertuples(index=False):
            if var and not var.startswith("padrao:"):
                VARIANTE_HIST[(fid, sku)] = var
    except Exception as e:  # noqa: BLE001 — primeira execução: tabela ainda não existe
        print("histórico indisponível (%s): sem checagem de variação" % type(e).__name__)
        hist = None
    linhas, logs = [], []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(coleta_um, c, data_coleta, prazo): c for c in ativos}
        for f in as_completed(futs):
            c = futs[f]
            try:
                ls, lg = f.result()
            except Exception as e:  # noqa: BLE001
                ls, lg = [], dict(data_coleta=data_coleta, coletado_em=agora().isoformat(timespec="seconds"), empresa_slug=c["empresa_slug"],
                                  metodo=c["metodo"], status="erro", produtos=0, variantes=0, erro=str(e)[:300], duracao_s=None,
                                  fonte_id=c["fonte_id"], requisicoes=None)
            linhas += ls
            logs.append(lg)
            print("  %-26s %-9s prod=%-4s var=%-5s %6.1fs %s" % (lg["fonte_id"], lg["status"], lg["produtos"], lg["variantes"],
                                                                  lg["duracao_s"] or 0, (lg["erro"] or "")[:110]), flush=True)
    df = pd.DataFrame(linhas, columns=COLS)
    if len(df):
        df = saneia(df, hist)
        df = tipa(df)
        nekt.save_table(df=df, layer_name="bbb_banheiras", table_name="precos_coleta", mode="append")
    dlog = pd.DataFrame(logs)
    for k in ("produtos", "variantes", "requisicoes"):
        dlog[k] = pd.to_numeric(dlog[k], errors="coerce").astype("Int64")
    dlog["duracao_s"] = pd.to_numeric(dlog["duracao_s"], errors="coerce").astype("float64")
    for k in ("data_coleta", "coletado_em", "empresa_slug", "metodo", "status", "erro", "fonte_id"):
        dlog[k] = dlog[k].map(lambda x: None if x is None or (isinstance(x, float) and math.isnan(x)) else str(x)).astype("object")
    nekt.save_table(df=dlog, layer_name="bbb_banheiras", table_name="coleta_log", mode="append")
    nekt.save_table(df=tabela_concorrentes(), layer_name="bbb_banheiras", table_name="concorrentes", mode="overwrite")
    print("Total: %d linhas, %d fontes, %.0f s" % (len(df), len(logs), time.time() - t0), flush=True)
    return df, dlog


if __name__ == "__main__" or not LOCAL:
    _df, _log = main()
