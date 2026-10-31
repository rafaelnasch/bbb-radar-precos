#!/usr/bin/env python3
"""
BBB Banheiras | Casamento de produtos v2 (equivalências BBB x concorrentes).

Roda no notebook "python-base" da Nekt (pandas). Um arquivo, sem import de módulo local.
Entradas  (camada bbb_banheiras): precos_coleta (histórico, append diário), revisao_pares (manual).
Saídas    (camada bbb_banheiras): equivalencias (overwrite; todas as classes),
                                   comparativo_diario (histórico; só exato e equivalente; regravado com
                                   overwrite: rodar de novo no mesmo dia substitui aquele dia, não duplica),
                                   precos_suspeitos (overwrite).
Modo local: NEKT_LOCAL=1 troca o SDK por um stub que lê/grava <NEKT_LOCAL_DIR>/<tabela>.csv
            (padrão: pasta local_tables ao lado deste arquivo).

O QUE MUDOU CONTRA A V1 (matching.py), um item por erro sistemático da verificação de 31/10/2026:
 1. Variante certa: cada linha de precos_coleta é uma variante (nível, kit, medida, material).
    Para cada variante da BBB escolhe-se, em cada produto do concorrente, a variante cuja configuração
    (medida, jatos, motores, potência, material, itens inclusos) é a mais próxima, e usa-se o preço DELA.
 2. com_hidro é filtro duro (BBB sem hidro só casa com concorrente explicitamente sem hidro).
 3. Lugares diferentes eliminam o par. Motores e potência têm peso alto na nota e limitam a classe:
    razão de motores >= 2 (ex.: 1 x 2, 2 x 4, 1 x 3) não passa de "referencia".
 4. Sem bônus por nome de modelo (nome não entra na nota).
 5. Itens inclusos: o preço do concorrente é normalizado para a configuração da BBB
    (conc_preco_ajustado). Item que o concorrente inclui e a BBB cobra à parte (aquecedor,
    cromoterapia, acionador digital) é DESCONTADO pelo preço do opcional da BBB; item que a BBB
    inclui de série e o concorrente cobra à parte é SOMADO pelo preço do opcional do concorrente.
    Item com inclusão não verificada não é ajustado e fica dito em ajuste_descricao.
 6. Cada variante de medida da BBB é uma linha (bbb_variante): tamanho casa com tamanho.
 +  Produto do concorrente que tem faixa de preço por variante mas cuja coleta não trouxe a variante
    (atributos_json.preco_entrada_variantes) não passa de "referencia": o preço de entrada não é
    o da configuração comparada (era a origem do erro 1).

CORREÇÕES DA CONFERÊNCIA ADVERSARIAL DE 31/10/2026 (verificacao_pares.md: 11 de 20 equivalentes caíram):
 a. Concorrente sem jatos ou sem motores nunca passa de "referencia". Lugares não informados (em
    qualquer lado) impedem "exato" e só permitem "equivalente" com medida ±5% e jatos e motores conhecidos.
 b. Nota mínima 70 para "equivalente" (NOTA_MIN_EQUIV).
 c. distancia: dado faltante custa o pior caso (jatos 40, motores 30, potência 10, medida 40,
    material 10). A variante com configuração conhecida e mais próxima vence (ofurô JP Premium x Essencial).
 d. Faixa de mercado (entrada, intermediaria, premium) por empresa: mediana de preço/mediana do mercado
    no grupo categoria x lugares; marca ou linha premium (Jacuzzi, Doka/Immersi, Victoria+Albert,
    My Place/Axell) é "premium" direto. Coluna "faixa" em equivalencias e comparativo_diario. O índice
    principal compara a BBB só com a faixa dela; o resumo traz também o índice de cada faixa e o de todas.
    Coluna "faixa_bbb" (a faixa da BBB no dia) para o painel filtrar o índice principal em SQL.
 e. Saem do índice (viram "referencia"): instalação inclusa, variante com kit de instalação
    ("Com Kit + R$", "% OFF + R$", COMBOKIT, "Padrão"), variante aberta ("Personalize"), níveis
    Slim/Master/Premium da Floripa Norte. "Sem/s/ <item>" no nome ou variante zera a flag do item
    (não se desconta item que o produto não tem).
 f. Forma: atributo da loja quando existir (vitoriana/freestanding no nome continuam vencendo).
 g. Preço de vitrine 99999/9999,99 ("sob consulta") não é preço: a linha não entra.

Classes (só exato e equivalente entram no índice):
  exato:       mesmo tipo e lugares (conhecidos nos dois); mesmo material (conhecido nos dois);
               comprimento ±5% e largura ±10%; jatos (grandes+mini) com diferença <= max(2; 20%); mesmo
               nº de motores; potência total com razão <= 1,5; forma não diverge; produto disponível;
               e passa em tudo que "equivalente" exige.
  equivalente: mesmo tipo; jatos e motores do concorrente conhecidos; lugares iguais (ou não informados,
               com medida ±5%); comprimento ±10% e largura ±20%; material não diverge; razão de motores
               < 2 e de jatos < 2; nota >= 70; sem kit, instalação, Personalize ou nível sem configuração.
  referencia:  passou nos filtros duros e tem nota >= 50.
  Filtros duros (eliminam): categoria; com_hidro; lugares conhecidos e diferentes; comprimento
               fora de ±20% ou largura fora de ±25%; par rejeitado em revisao_pares.
  revisao_pares: "rejeitado" nunca vira par; "aprovado" vira "exato" se passar nos filtros duros.
Máximo de 3 produtos por concorrente por variante BBB (os melhores).
Saneamento: linhas com status_preco != "ok" não entram; preço que variou > 30% contra a coleta
anterior do mesmo produto/variante vira "suspeito" e vai para precos_suspeitos.
"""
import json
import math
import os
import re
import statistics
import unicodedata
from datetime import datetime, timedelta, timezone

import pandas as pd

CAMADA = "bbb_banheiras"
LOCAL = os.environ.get("NEKT_LOCAL") == "1"
BRT = timezone(timedelta(hours=-3))
JANELA_DIAS = 7          # concorrente sem coleta nos últimos 7 dias fica fora do dia
SALTO_SUSPEITO = 0.30    # variação contra a coleta anterior
MAX_POR_CONC = int(os.environ.get("EQUIV_MAX_POR_CONC") or 3)  # 0 = sem corte (só para teste)
NOTA_MIN_REF = 50
NOTA_MIN_EQUIV = 70      # abaixo disso o par não passa de "referencia" (conferência de 31/10: 37% dos equivalentes tinham < 70)
# Faixa de mercado: razão entre o preço típico da empresa e a mediana do mercado no mesmo grupo (categoria x lugares).
# Limites simétricos em escala log (0,80 e 1/0,80): abaixo = entrada, acima = premium.
FAIXA_LIM_ENTRADA = 0.80
FAIXA_LIM_PREMIUM = 1.25
FAIXA_MIN_GRUPO = 5      # grupo (categoria x lugares) só conta com >= 5 produtos de >= 3 empresas
FAIXAS = ("entrada", "intermediaria", "premium")
# Marcas e linhas premium: o produto é "premium" seja qual for a loja que revende.
MARCAS_PREMIUM = r"jacuzzi|\bdoka\b|immersi|victoria\s*\+?\s*albert|my place|axell"
CATS_ESCOPO = ("banheira", "spa", "ofuro", "infantil")
# Base de preço da BBB: site de referência primeiro; o 2º domínio só completa SKU que falta no 1º.
# bbbcampinas (loja Campinas, vínculo não verificado) e marketplaces ficam fora da base.
BBB_DOMINIOS_BASE = ("banheirasbombanho.com.br", "bbbbanheiras.com.br")
ITENS = (("aquecedor", "aquecedor_incluso", "aquecedor"), ("cromo", "cromo_incluso", "cromoterapia"),
         ("acionador", None, "acionador digital"))

if LOCAL:
    class _NektLocal:
        """Stub local com a mesma assinatura do SDK: tabela = <dir>/<tabela>.csv (ou .parquet)."""

        def __init__(self):
            self.dir = os.environ.get("NEKT_LOCAL_DIR") or os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "local_tables")
            os.makedirs(self.dir, exist_ok=True)

        def load_table(self, layer_name, table_name):
            pq = os.path.join(self.dir, f"{table_name}.parquet")
            cs = os.path.join(self.dir, f"{table_name}.csv")
            if os.path.exists(pq):
                return pd.read_parquet(pq)
            if os.path.exists(cs):
                return pd.read_csv(cs, dtype={"produto_id": str, "sku": str, "variante": str,
                                              "bbb_sku": str, "conc_produto_id": str, "data_coleta": str})
            raise FileNotFoundError(f"tabela {layer_name}.{table_name} não existe em {self.dir}")

        def save_table(self, df, layer_name, table_name, mode="overwrite"):
            cs = os.path.join(self.dir, f"{table_name}.csv")
            if mode == "append" and os.path.exists(cs):
                df = pd.concat([self.load_table(layer_name, table_name), df], ignore_index=True)
            df.to_csv(cs, index=False)
            return cs

    nekt = _NektLocal()
else:
    import nekt  # SDK do notebook Nekt

# ------------------------------------------------------------------ coerção de tipos (CSV/BigQuery)


def _nulo(v):
    return v is None or (isinstance(v, float) and math.isnan(v)) or (isinstance(v, str) and v.strip().lower() in ("", "nan", "none", "null", "<na>")) or v is pd.NA


def f_(v):
    if _nulo(v):
        return None
    try:
        x = float(v)
        return None if math.isnan(x) else x
    except (TypeError, ValueError):
        return None


def i_(v):
    x = f_(v)
    return int(round(x)) if x is not None else None


def b_(v):
    if _nulo(v):
        return None
    if isinstance(v, bool):
        return v
    t = str(v).strip().lower()
    if t in ("true", "1", "1.0", "sim", "t"):
        return True
    if t in ("false", "0", "0.0", "nao", "não", "f"):
        return False
    return None


def s_(v):
    return "" if _nulo(v) else str(v).strip()


def j_(v):
    try:
        return json.loads(v) if not _nulo(v) else {}
    except (TypeError, ValueError):
        return {}


def mediana(v):
    return statistics.median(v) if v else None


def brl(v):
    return "n/d" if v is None else ("R$ " + f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))


# ------------------------------------------------------------------ preparo


def _low(t):
    t = unicodedata.normalize("NFKD", str(t or "").lower())
    return "".join(ch for ch in t if not unicodedata.combining(ch))


def forma_de(t):
    t = _low(t)
    if "vitorian" in t:
        return "vitoriana"
    if re.search(r"freestanding|contempor|com pes|pes cromados|\bilha\b", t):
        return "freestanding"
    if "redond" in t or "circular" in t:
        return "redonda"
    if re.search(r"\bde canto\b|\bcanto\b", t):
        return "canto"
    if "quadrad" in t:
        return "quadrada"
    if "oval" in t:
        return "oval"
    if "retangular" in t:
        return "retangular"
    return None


def forma_por_medida(L, W):
    """Forma deduzida pela medida quando a loja não informa: razão C/L <= 1,05 = quadrada ou redonda;
    >= 1,15 = alongada (retangular, oval). Entre os dois não decide."""
    if not L or not W:
        return None
    r = max(L, W) / min(L, W)
    if r <= 1.05:
        return "quadrada_ou_redonda"
    if r >= 1.15:
        return "alongada"
    return None


def _grupo_forma(f):
    if f in ("quadrada", "redonda", "quadrada_ou_redonda"):
        return "compacta"
    if f in ("retangular", "oval", "vitoriana", "alongada"):
        return "alongada"
    return None  # canto, freestanding: o contorno não decide


def forma_diverge(b, c):
    """Forma de um lado contra a do outro, usando a forma deduzida pela medida no lado que não informa.
    Retangular x 2,00x2,00 (Imola x Aurum Sergipe) e Redonda x 1,80x1,40 (Desejo x Imperatriz) divergem."""
    fb, fc = b["forma"] or b.get("forma_med"), c["forma"] or c.get("forma_med")
    gb, gc = _grupo_forma(fb), _grupo_forma(fc)
    return bool(gb and gc and gb != gc)


def _pontos(t):
    m = re.search(r"(\d{1,2})\s*(?:pontos?|spots?|leds?)\b", t)
    return int(m.group(1)) if m else None


def _opcionais(a):
    """Aceita os dois formatos: dict {aquecedor: preço} (legado) ou lista [{item, preco}] (coleta v2)."""
    op = a.get("opcionais")
    out = {}
    if isinstance(op, dict):
        return {k: f_(v) for k, v in op.items()}
    for it in op or []:
        if not isinstance(it, dict):
            continue
        t, v = _low(it.get("item")), f_(it.get("preco"))
        if v is None or str(it.get("tipo") or "").upper() == "PERCENT":
            continue
        k = ("aquecedor" if "aquecedor" in t else "cromo" if "cromo" in t else
             "acionador" if "acionador" in t else t[:40])
        if k == "cromo":
            # mais de um opcional de cromo (2 e 4 pontos): guarda todos; o desconto escolhe pelo nº de pontos
            out.setdefault("_cromo_opcoes", []).append((_pontos(t), v))
            out["cromo"] = min(out.get("cromo", v), v)  # sem nº de pontos, vale o menor opcional de cromo
            continue
        out.setdefault(k, v)
    return out


RX_KIT_INSTAL = re.compile(r"kit\s+instala")
RX_SEM_KIT = re.compile(r"sem\s+kit|semkit")
RX_INSTAL_INCLUSA = re.compile(r"(?:\bcom|\bc/|\be|\+)\s*instalac|instalac\w*\s+(?:complet|inclus|gratis|gratuit)")
RX_VAR_ABERTA = re.compile(r"personaliz|sob\s+medida|consulte|monte\s+o\s+seu")
RX_NIVEL_FLORIPA = re.compile(r"^\s*(slim|master|premium)\b")


def _preco_placeholder(v):
    """Preço de vitrine "99999", "9999,99" etc.: a loja usa como "sob consulta". Não é preço."""
    return v is not None and v >= 9999 and re.fullmatch(r"9{4,}(\.9+)?", f"{v:.2f}".rstrip("0").rstrip(".")) is not None


def _sem_item(texto, item_rx):
    """True quando o nome/variante nega o item ("sem aquecedor", "s/ aquecedor", "s/aquecedor")."""
    return bool(re.search(r"(?:\bsem|\bs/)\s*(?:" + item_rx + ")", texto))


def normalizar(row):
    a = j_(row.get("atributos_json"))
    jg, mj = i_(row.get("jatos")), i_(row.get("mini_jatos"))
    jt = None if jg is None and mj is None else (jg or 0) + (mj or 0)
    mot, cv = i_(row.get("motores")), f_(row.get("potencia_cv"))
    nome, variante = s_(row.get("nome")), s_(row.get("variante"))
    opc = _opcionais(a)
    # acionador: coleta v2 não grava acionador_incluso; deriva do nome ou da lista de opcionais pagos
    ac = b_(a.get("acionador_incluso"))
    if ac is None:
        tn = _low(nome + " " + variante)
        if re.search(r"(com|c/|\+|e)\s*acionador (digital|eletronico)", tn):
            ac = True
        elif "acionador" in opc:
            ac = False
    # forma: atributo da loja quando existir; senão nome (vitoriana/freestanding no nome vencem o atributo,
    # porque descrevem o tipo de instalação e o atributo da loja só traz o contorno)
    fn = forma_de(nome)
    fa = forma_de(a.get("forma"))
    forma = fn if fn in ("vitoriana", "freestanding") else (fa or fn)
    # potência total: coleta v2 grava potencia_total_cv_estimada (motores x cv por motor)
    cv_tot = f_(a.get("potencia_total_cv_estimada"))
    if cv_tot is None and mot and cv:
        cv_tot = mot * cv
    tn = _low(nome + " | " + variante)
    tv = _low(variante)
    # bloqueios de índice (viram "referencia"): kit de instalação, instalação inclusa, variante aberta
    # ("Personalize": preço da configuração mais barata), nível sem configuração (Floripa Slim/Master/Premium)
    bloqueios = []
    if (RX_KIT_INSTAL.search(tv) and not RX_SEM_KIT.search(tv)) or "combokit" in _low(row.get("sku")) \
            or b_(a.get("inclui_kit_instalacao")):
        bloqueios.append("variante com kit de instalação no preço")
    if RX_INSTAL_INCLUSA.search(_low(nome)) or b_(a.get("instalacao_inclusa")):
        bloqueios.append("instalação inclusa no preço")
    if RX_VAR_ABERTA.search(tv) or s_(row.get("status_preco")) == "variante_aberta":
        bloqueios.append("variante aberta (Personalize / sob medida)")
    if s_(row.get("empresa_slug")) == "floripa-norte-spas" and RX_NIVEL_FLORIPA.search(tv):
        bloqueios.append("nível Slim/Master/Premium sem configuração publicada")
    marca = s_(a.get("marca"))
    premium = bool(re.search(MARCAS_PREMIUM, _low(marca + " " + nome)))
    inc = {"aquecedor": b_(row.get("aquecedor_incluso")), "cromo": b_(row.get("cromo_incluso")), "acionador": ac}
    # não descontar item que o produto não tem: negação no nome/variante vence a flag da coleta
    for k, rx in (("aquecedor", r"aquec"), ("cromo", r"cromo"), ("acionador", r"acionador")):
        if _sem_item(tn, rx):
            inc[k] = False
    entrada = bool(a.get("preco_entrada_variantes")) or (
        f_(a.get("preco_max_variacoes")) is not None and f_(row.get("preco_por")) is not None
        and f_(a.get("preco_max_variacoes")) > f_(row.get("preco_por")) + 0.01)
    return {
        "data_coleta": s_(row.get("data_coleta"))[:10], "empresa": s_(row.get("empresa")),
        "slug": s_(row.get("empresa_slug")), "canal": s_(row.get("canal")), "produto_id": s_(row.get("produto_id")) or s_(row.get("url")),
        "dominio": s_(row.get("dominio")).replace("www.", ""),
        "sku": s_(row.get("sku")), "variante": variante, "nome": nome,
        "url": s_(row.get("url")), "categoria": s_(row.get("categoria")), "preco": f_(row.get("preco_por")),
        "disponivel": b_(row.get("disponivel")), "lugares": i_(row.get("lugares")),
        "L": f_(row.get("comprimento_m")), "W": f_(row.get("largura_m")), "material": s_(row.get("material")) or None,
        "com_hidro": b_(row.get("com_hidro")), "jatos": jt or None, "motores": mot,
        "cv_total": cv_tot, "cv": cv,
        "inc": inc,
        "opcionais": opc,
        "forma": forma, "forma_med": forma_por_medida(f_(row.get("comprimento_m")), f_(row.get("largura_m"))),
        "cromo_pontos": i_(a.get("cromo_pontos")), "entrada": entrada, "bloqueios": bloqueios, "marca": marca, "marca_premium": premium,
        "status": ("placeholder" if _preco_placeholder(f_(row.get("preco_por"))) else (s_(row.get("status_preco")) or "ok")),
        "motivo_status": s_(row.get("motivo_status")),
    }


def detectar_saltos(df):
    """Marca como suspeito o preço que variou > 30% contra a coleta anterior do mesmo item."""
    d = df.copy()
    d["_preco"] = pd.to_numeric(d["preco_por"], errors="coerce")
    dom = d["dominio"].fillna("").astype(str) if "dominio" in d.columns else ""
    d["_chave"] = (d["empresa_slug"].astype(str) + "|" + d["canal"].astype(str) + "|" + dom + "|" + d["produto_id"].astype(str)
                   + "|" + d["variante"].fillna("").astype(str))
    d["data_coleta"] = d["data_coleta"].astype(str).str[:10]
    d = d.sort_values(["_chave", "data_coleta", "coletado_em"])
    d = d.drop_duplicates(["_chave", "data_coleta"], keep="last")
    d["_preco_ant"] = d.groupby("_chave")["_preco"].shift(1)
    d["_data_ant"] = d.groupby("_chave")["data_coleta"].shift(1)
    var = (d["_preco"] / d["_preco_ant"] - 1).abs()
    salto = var > SALTO_SUSPEITO
    d.loc[salto, "status_preco"] = "suspeito"
    d.loc[salto, "motivo_status"] = ("variação de " + (var[salto] * 100).round(1).astype(str) + "% contra "
                                     + d.loc[salto, "_data_ant"].astype(str) + " (" + d.loc[salto, "_preco_ant"].round(2).astype(str) + ")")
    return d


def snapshot(d):
    """Último dia de cada empresa, dentro da janela de 7 dias do dia mais recente."""
    ult = d.groupby("empresa_slug")["data_coleta"].max()
    dmax = d["data_coleta"].max()
    lim = (datetime.strptime(dmax, "%Y-%m-%d") - timedelta(days=JANELA_DIAS)).strftime("%Y-%m-%d")
    ult = ult[ult >= lim]
    return d[d.apply(lambda r: ult.get(r["empresa_slug"]) == r["data_coleta"], axis=1)], dmax


# ------------------------------------------------------------------ comparação


def rel(a, b):
    return abs(a - b) / b if (a is not None and b) else None


def filtros_duros(b, c):
    """None se elimina; senão (dl, dw)."""
    if b["categoria"] != c["categoria"]:
        return None
    if b["com_hidro"] is False and c["com_hidro"] is not False:
        return None
    if b["com_hidro"] is True and c["com_hidro"] is False:
        return None
    if b["lugares"] and c["lugares"] and b["lugares"] != c["lugares"]:
        return None
    dl = dw = None
    if b["L"] and c["L"] and b["W"] and c["W"]:
        dl, dw = rel(c["L"], b["L"]), rel(c["W"], b["W"])
        if dl > 0.20 or dw > 0.25:
            return None
    return dl, dw


def distancia(b, c, dl, dw):
    """Quanto a configuração da variante do concorrente se afasta da variante BBB (menor = mais perto).

    Dado faltante custa o pior caso plausível (não premia a variante sem dado): jatos ausentes = 40
    (100% de diferença), motores ausentes = 30 (1 motor de diferença), potência ausente = 10,
    medida ausente = 40 (pior caso dentro dos filtros duros é 32,5), material ausente = 10.
    Ex.: ofurô JP "Essencial" sem jatos lidos não vence mais o "Premium" com 4+6 jatos e 1 cv."""
    d = 0.0
    if b["jatos"]:
        d += 40 * min(1.0, abs(b["jatos"] - c["jatos"]) / b["jatos"]) if c["jatos"] else 40
    if b["motores"]:
        d += 30 * abs(b["motores"] - c["motores"]) if c["motores"] else 30
    if b["cv_total"]:
        d += 10 * min(1.0, abs(b["cv_total"] - c["cv_total"]) / b["cv_total"]) if c["cv_total"] else 10
    d += (100 * dl + 50 * dw) if dl is not None else 40
    if b["material"] and c["material"] and b["material"] != c["material"]:
        d += 15
    elif b["material"] and not c["material"]:
        d += 10
    if c["bloqueios"]:
        d += 50  # variante com kit/instalação/Personalize só é escolhida se não houver outra
    for k, _, _ in ITENS:
        if b["inc"].get(k) is not None and c["inc"].get(k) is not None and b["inc"][k] != c["inc"][k]:
            d += 5
    return d


def nota_e_classe(b, c, dl, dw):
    pts, mot, tetos = 0.0, [], []
    # medidas (30)
    if dl is not None:
        pts += 30 * (0.6 * max(0, 1 - dl / 0.20) + 0.4 * max(0, 1 - dw / 0.25))
        mot.append(f"medida {c['L']:.2f}x{c['W']:.2f} vs BBB {b['L']:.2f}x{b['W']:.2f} (C {dl*100:.0f}%, L {dw*100:.0f}%)")
    else:
        pts += 8
        mot.append("medida sem dado")
        tetos.append(("referencia", "sem medida para conferir"))
    # jatos (15)
    if b["jatos"] and c["jatos"]:
        pts += 15 * max(0, 1 - abs(b["jatos"] - c["jatos"]) / b["jatos"])
        mot.append(f"jatos {c['jatos']} vs {b['jatos']}")
    else:
        pts += 5
        mot.append("jatos sem dado")
    # motores (15) e potência (10)
    if b["motores"] and c["motores"]:
        dm = abs(b["motores"] - c["motores"])
        pts += 15 if dm == 0 else (5 if dm == 1 else 0)
        mot.append(f"motores {c['motores']} vs {b['motores']}")
    else:
        pts += 5
        mot.append("motores sem dado")
    if b["cv_total"] and c["cv_total"]:
        pts += 10 * max(0, 1 - abs(b["cv_total"] - c["cv_total"]) / b["cv_total"])
        mot.append(f"potência {c['cv_total']:g} vs {b['cv_total']:g} cv")
    else:
        pts += 3
    # material (15)
    if b["material"] and c["material"]:
        pts += 15 if b["material"] == c["material"] else 0
        mot.append(f"material {c['material']} vs {b['material']}")
    else:
        pts += 6
        mot.append("material do concorrente não verificado" if not c["material"] else "material BBB sem dado")
    # lugares (10) e forma (5)
    if b["lugares"] and c["lugares"]:
        pts += 10
        mot.append(f"lugares {c['lugares']}={b['lugares']}")
    else:
        pts += 4
        mot.append("lugares do concorrente não informados")
    if b["forma"] and c["forma"]:
        pts += 5 if b["forma"] == c["forma"] else 0
    else:
        pts += 2
    nota = int(round(min(100, pts)))

    # --- classe
    def razao(x, y):
        return max(x, y) / min(x, y)

    exato = equiv = True
    if dl is None or dl > 0.05 or dw > 0.10:
        exato = False
    if dl is None or dl > 0.10 or dw > 0.20:
        equiv = False
    if not (b["lugares"] and c["lugares"]):
        exato = False
    if not (b["material"] and c["material"] and b["material"] == c["material"]):
        exato = False
    if b["material"] and c["material"] and b["material"] != c["material"]:
        equiv = False
        tetos.append(("referencia", "material diferente"))
    if b["forma"] and c["forma"] and b["forma"] != c["forma"]:
        exato = equiv = False
        tetos.append(("referencia", "forma diferente"))
    elif not (b["forma"] and c["forma"]) and forma_diverge(b, c):
        exato = equiv = False
        tetos.append(("referencia", "forma diferente (deduzida pela medida)"))
    if b["jatos"] and c["jatos"]:
        if abs(b["jatos"] - c["jatos"]) > max(2, 0.2 * b["jatos"]):
            exato = False
        if razao(b["jatos"], c["jatos"]) >= 2:  # mesma régua dos motores (era > 1,6; ver LEIAME, teste 9)
            equiv = False
            tetos.append(("referencia", "jatos muito diferentes (razão >= 2)"))
    else:
        exato = False
    if b["motores"] and c["motores"]:
        if b["motores"] != c["motores"]:
            exato = False
        if razao(b["motores"], c["motores"]) >= 2:
            equiv = False
            tetos.append(("referencia", "nº de motores com razão >= 2"))
    else:
        exato = False
    if b["cv_total"] and c["cv_total"] and razao(b["cv_total"], c["cv_total"]) > 1.5:
        exato = False
    # (1) concorrente sem jatos ou sem motores: configuração não verificada, nunca passa de referência.
    # (a regra pedida era "sem jatos E sem motores"; com "OU" cai também o Spa Zen da Pretty Jet, 19 jatos
    # não lidos, refutado na conferência de 31/10, e só ~8% dos equivalentes saem)
    if c["jatos"] is None or c["motores"] is None:
        exato = equiv = False
        tetos.append(("referencia", "jatos ou motores do concorrente não informados"))
    # (1) lugares não informados: nunca exato; equivalente só com medida ±5% e jatos e motores conhecidos
    if not (b["lugares"] and c["lugares"]):
        exato = False
        if not (dl is not None and dl <= 0.05 and dw <= 0.05 and c["jatos"] and c["motores"]):
            equiv = False
            tetos.append(("referencia", "lugares não informados sem medida ±5% e configuração conhecida"))
    # (2) nota mínima para equivalente
    if nota < NOTA_MIN_EQUIV:
        equiv = False
        tetos.append(("referencia", f"nota {nota} < {NOTA_MIN_EQUIV}"))
    # (5)(6) kit de instalação, instalação inclusa, Personalize, nível Floripa sem configuração
    if c["bloqueios"]:
        exato = equiv = False
        tetos.extend(("referencia", m) for m in c["bloqueios"])
    if c["disponivel"] is False:
        exato = equiv = False
        tetos.append(("referencia", "concorrente indisponível"))
    if c["entrada"]:
        exato = equiv = False
        tetos.append(("referencia", "preço de entrada de produto com variantes não detalhadas"))
    exato = exato and equiv
    classe = "exato" if exato else ("equivalente" if equiv else "referencia")
    if classe == "referencia" and tetos:
        mot.append("limitado a referência: " + ", ".join(dict.fromkeys(t[1] for t in tetos)))
    return nota, classe, mot


CROMO_BBB_POR_PONTOS = {}  # nº de pontos -> preço mediano do opcional de cromo da BBB no dia (preenchido em rodar)


def _cromo_bbb(b, pontos):
    """Preço do opcional de cromo da BBB a descontar quando o concorrente inclui cromoterapia.
    Com o nº de pontos do concorrente: o opcional deste produto BBB com os mesmos pontos; senão o mesmo opcional
    em outros produtos BBB do dia (mediana). Sem pontos (ou sem opcional igual): o menor opcional de cromo do produto."""
    opcs = b["opcionais"].get("_cromo_opcoes") or []
    if pontos:
        igual = [v for p, v in opcs if p == pontos]
        if igual:
            return min(igual), f"opcional BBB de {pontos} pontos"
        if CROMO_BBB_POR_PONTOS.get(pontos):
            return CROMO_BBB_POR_PONTOS[pontos], f"opcional BBB de {pontos} pontos (mediana de outros produtos BBB)"
    v = b["opcionais"].get("cromo")
    return v, ("menor opcional de cromo da BBB" + (f"; concorrente tem {pontos} pontos" if pontos else ""))


def ajuste(b, c):
    """Normaliza o preço do concorrente para a configuração da BBB."""
    total, desc = 0.0, []
    for k, _, rot in ITENS:
        bi, ci = b["inc"].get(k), c["inc"].get(k)
        if ci is True and bi is False:
            v = b["opcionais"].get(k)
            if k == "cromo":
                v, origem = _cromo_bbb(b, c.get("cromo_pontos"))
                if v:
                    total -= v
                    desc.append(f"concorrente inclui {rot}: -{brl(v)} ({origem})")
                    continue
            if v:
                total -= v
                desc.append(f"concorrente inclui {rot}: -{brl(v)} (opcional BBB)")
            else:
                desc.append(f"concorrente inclui {rot}; BBB sem preço do opcional: não ajustado")
        elif bi is True and ci is False:
            v = c["opcionais"].get(k)
            if v:
                total += v
                desc.append(f"BBB inclui {rot}: +{brl(v)} (opcional do concorrente)")
            else:
                desc.append(f"BBB inclui {rot}; concorrente sem preço do opcional: não ajustado")
        elif ci is None and bi is False:
            desc.append(f"{rot} do concorrente não verificado: não ajustado")
    return total, ("; ".join(desc) if desc else "sem ajuste (mesmos itens)")


ORDEM = {"exato": 0, "equivalente": 1, "referencia": 2}


def casar(bbb, conc, revisao):
    rej = {(r["bbb_sku"], r["conc_slug"], r["conc_produto_id"]) for r in revisao if r["decisao"] == "rejeitado"}
    apr = {(r["bbb_sku"], r["conc_slug"], r["conc_produto_id"]) for r in revisao if r["decisao"] == "aprovado"}
    grupos = {}
    for c in conc:
        grupos.setdefault((c["categoria"], c["slug"], c["produto_id"]), []).append(c)
    por_cat = {}
    for (cat, slug, pid), vs in grupos.items():
        por_cat.setdefault(cat, []).append((slug, pid, vs))
    pares = []
    for b in bbb:
        melhores = {}
        for slug, pid, vs in por_cat.get(b["categoria"], []):
            chave = (b["sku"], slug, pid)
            if chave in rej:
                continue
            cand = []
            for c in vs:
                fd = filtros_duros(b, c)
                if fd is None:
                    continue
                cand.append((distancia(b, c, *fd), c, fd))
            if not cand:
                continue
            dist, c, (dl, dw) = min(cand, key=lambda x: (x[0], x[1]["preco"]))
            nota, classe, mot = nota_e_classe(b, c, dl, dw)
            if chave in apr:
                classe = "exato"
                mot.append("aprovado em revisao_pares")
            if classe == "referencia" and nota < NOTA_MIN_REF:
                continue
            if len(vs) > 1:
                mot.append(f"variante escolhida entre {len(vs)}: {c['variante'] or '(única)'}")
            aj, aj_desc = ajuste(b, c)
            preco_aj = round(c["preco"] + aj, 2)
            pares.append(dict(b=b, c=c, nota=nota, classe=classe, motivos="; ".join(mot), dist=dist,
                              preco_aj=preco_aj, aj_desc=aj_desc))
            melhores.setdefault(slug, []).append(pares[-1])
        # máximo 3 por concorrente
        for slug, lst in melhores.items():
            lst.sort(key=lambda p: (ORDEM[p["classe"]], -p["nota"], p["dist"]))
            for p in (lst[MAX_POR_CONC:] if MAX_POR_CONC > 0 else []):
                p["_fora"] = True
    return [p for p in pares if not p.get("_fora")]


# ------------------------------------------------------------------ faixa de mercado


def _faixa_de_razao(r):
    if r is None:
        return "intermediaria"
    return "entrada" if r < FAIXA_LIM_ENTRADA else ("premium" if r > FAIXA_LIM_PREMIUM else "intermediaria")


def faixas(bbb, conc):
    """Faixa de cada empresa: mediana, entre os produtos dela, de (preço do produto / mediana do mercado no
    grupo categoria x lugares). Mercado = concorrentes (sem a BBB). Produto de marca/linha premium
    (MARCAS_PREMIUM) é "premium" direto e não entra na razão da loja que o revende.
    Retorna ({slug: {faixa, razao, n_produtos}}, faixa de cada linha via faixa_linha)."""
    prods = {}
    for x in list(bbb) + list(conc):
        if not x["preco"] or x["bloqueios"]:
            continue
        k = (x["slug"], x["produto_id"])
        p = prods.setdefault(k, {"slug": x["slug"], "cat": x["categoria"], "lug": None, "precos": [], "premium": False})
        p["precos"].append(x["preco"])
        p["lug"] = p["lug"] or x["lugares"]
        p["premium"] = p["premium"] or x["marca_premium"]
    for p in prods.values():
        p["preco"] = mediana(p["precos"])
    grupos = {}
    for p in prods.values():
        if p["slug"] == "bbb":
            continue
        for g in ((p["cat"], p["lug"]), (p["cat"], "*")) if p["lug"] else ((p["cat"], "*"),):
            grupos.setdefault(g, []).append(p)
    med = {g: mediana([p["preco"] for p in ps]) for g, ps in grupos.items()
           if len(ps) >= FAIXA_MIN_GRUPO and len({p["slug"] for p in ps}) >= 3}
    razoes, n_prem = {}, {}
    for p in prods.values():
        if p["premium"]:
            n_prem[p["slug"]] = n_prem.get(p["slug"], 0) + 1
            continue
        g = (p["cat"], p["lug"]) if p["lug"] and (p["cat"], p["lug"]) in med else (p["cat"], "*")
        if g in med and med[g]:
            razoes.setdefault(p["slug"], []).append(p["preco"] / med[g])
    out = {}
    for slug in {p["slug"] for p in prods.values()}:
        rz = razoes.get(slug, [])
        r = mediana(rz)
        if not rz and n_prem.get(slug):
            fx = "premium"
        else:
            fx = _faixa_de_razao(r)
        out[slug] = {"faixa": fx, "razao": round(r, 2) if r else None, "n_produtos": len(rz),
                     "n_marca_premium": n_prem.get(slug, 0)}
    return out


def faixa_linha(x, fx_emp):
    if x["marca_premium"]:
        return "premium"
    return fx_emp.get(x["slug"], {}).get("faixa", "intermediaria")


# ------------------------------------------------------------------ índices


def _indice(ok):
    if ok.empty:
        return {"indice": None, "por_categoria": {}, "n_produtos": 0, "n_variantes": 0, "n_pares": 0}
    lin = ok.groupby(["bbb_sku", "bbb_variante", "bbb_categoria"]).agg(
        bbb_preco=("bbb_preco", "first"), med=("conc_preco_ajustado", "median")).reset_index()
    lin["idx"] = lin["bbb_preco"] / lin["med"] * 100
    prod = lin.groupby(["bbb_sku", "bbb_categoria"])["idx"].median().reset_index()
    return {"indice": round(float(prod["idx"].median()), 1), "n_produtos": int(len(prod)), "n_variantes": int(len(lin)),
            "n_pares": int(len(ok)),
            "por_categoria": {k: {"indice": round(float(v["idx"].median()), 1), "n_produtos": int(len(v))}
                              for k, v in prod.groupby("bbb_categoria")}}


def indices(eq, faixa_bbb=None):
    """100 = paridade com a mediana dos pares exato+equivalente. Linha = variante BBB; produto = mediana das variantes.
    principal: só concorrentes da mesma faixa da BBB; por_faixa: um índice por faixa; todas_faixas: sem filtro."""
    ok = eq[eq["classe"].isin(["exato", "equivalente"]) & (eq["conc_preco_ajustado"] > 0)].copy()
    ok["bbb_variante"] = ok["bbb_variante"].fillna("")
    out = {"faixa_bbb": faixa_bbb}
    out["principal"] = _indice(ok[ok["faixa"] == faixa_bbb]) if faixa_bbb else None
    out["por_faixa"] = {f: _indice(ok[ok["faixa"] == f]) for f in FAIXAS}
    out["todas_faixas"] = _indice(ok)
    out["produtos_com_exato"] = int(ok[ok["classe"] == "exato"]["bbb_sku"].nunique())
    out["produtos_com_exato_ou_equivalente"] = int(ok["bbb_sku"].nunique())
    return out


# ------------------------------------------------------------------ pipeline


COLS_EQ = ["data_coleta", "bbb_sku", "bbb_variante", "bbb_nome", "bbb_categoria", "bbb_preco", "conc_slug",
           "conc_empresa", "conc_produto_id", "conc_variante", "conc_nome", "conc_preco", "conc_preco_ajustado",
           "ajuste_descricao", "nota", "classe", "faixa", "faixa_bbb", "motivos", "gap_pct", "url_bbb", "url_conc"]
COLS_SUSP = ["data_coleta", "empresa_slug", "empresa", "canal", "produto_id", "sku", "variante", "nome", "url",
             "categoria", "preco_por", "status_preco", "motivo_status"]


def rodar(precos, revisao=None):
    precos = precos.copy()
    for col in ("variante", "motivo_status", "atributos_json", "canal", "coletado_em"):
        if col not in precos.columns:
            precos[col] = ""
    precos["variante"] = precos["variante"].fillna("")
    precos["coletado_em"] = precos["coletado_em"].fillna("")
    hist = detectar_saltos(precos)
    snap, dia = snapshot(hist)
    susp = snap[snap["status_preco"].astype(str) == "suspeito"].copy()
    linhas = [normalizar(r) for r in snap.to_dict("records")]
    bbb, vistos, skus_dom = [], set(), {}
    cand_bbb = [x for x in linhas if x["slug"] == "bbb" and x["canal"] == "site" and x["status"] == "ok" and x["preco"]
                and x["categoria"] in CATS_ESCOPO and (x["dominio"] in BBB_DOMINIOS_BASE or not x["dominio"])]
    rank = {d: i for i, d in enumerate(BBB_DOMINIOS_BASE)}
    cand_bbb.sort(key=lambda x: rank.get(x["dominio"], -1))
    for x in cand_bbb:  # base = site de referência da BBB, uma linha por sku x variante
        dono = skus_dom.setdefault(x["sku"], x["dominio"])
        if dono != x["dominio"] or (x["sku"], _low(x["variante"])) in vistos:
            continue  # SKU já coberto pelo domínio de referência
        vistos.add((x["sku"], _low(x["variante"])))
        bbb.append(x)
    conc = [x for x in linhas if x["slug"] != "bbb" and x["status"] == "ok" and x["preco"]
            and x["categoria"] in CATS_ESCOPO]
    por_p = {}
    for x in bbb:
        for p_, v_ in x["opcionais"].get("_cromo_opcoes") or []:
            if p_ and v_:
                por_p.setdefault(p_, []).append(v_)
    CROMO_BBB_POR_PONTOS.clear()
    CROMO_BBB_POR_PONTOS.update({p_: round(statistics.median(vs), 2) for p_, vs in por_p.items()})
    rev = []
    if revisao is not None and len(revisao):
        for r in revisao.to_dict("records"):
            rev.append({"bbb_sku": s_(r.get("bbb_sku")), "conc_slug": s_(r.get("conc_slug")),
                        "conc_produto_id": s_(r.get("conc_produto_id")), "decisao": s_(r.get("decisao")).lower()})
    pares = casar(bbb, conc, rev)
    fx_emp = faixas(bbb, conc)
    faixa_bbb = fx_emp.get("bbb", {}).get("faixa")
    rows = []
    for p in pares:
        b, c = p["b"], p["c"]
        rows.append({
            "data_coleta": dia, "bbb_sku": b["sku"], "bbb_variante": b["variante"], "bbb_nome": b["nome"],
            "bbb_categoria": b["categoria"], "bbb_preco": b["preco"], "conc_slug": c["slug"], "conc_empresa": c["empresa"],
            "conc_produto_id": c["produto_id"], "conc_variante": c["variante"], "conc_nome": c["nome"],
            "conc_preco": c["preco"], "conc_preco_ajustado": p["preco_aj"], "ajuste_descricao": p["aj_desc"],
            "nota": p["nota"], "classe": p["classe"], "faixa": faixa_linha(c, fx_emp), "faixa_bbb": faixa_bbb, "motivos": p["motivos"],
            "gap_pct": round((b["preco"] / p["preco_aj"] - 1) * 100, 1) if p["preco_aj"] > 0 else None,
            "url_bbb": b["url"], "url_conc": c["url"],
        })
    eq = pd.DataFrame(rows, columns=COLS_EQ)
    eq = eq.sort_values(["bbb_sku", "bbb_variante", "classe", "nota"], ascending=[True, True, True, False])
    resumo = {"data_coleta": dia, "variantes_bbb": len(bbb), "linhas_concorrentes": len(conc),
              "pares": int(len(eq)), "por_classe": eq["classe"].value_counts().to_dict(),
              "precos_suspeitos": int(len(susp)), "indices": indices(eq, faixa_bbb),
              "faixas_empresas": {k: v for k, v in sorted(fx_emp.items())}}
    susp = susp.rename(columns={})[COLS_SUSP] if len(susp) else pd.DataFrame(columns=COLS_SUSP)
    comp = eq[eq["classe"].isin(["exato", "equivalente"])].copy()  # histórico só com o que entra no índice
    return {"equivalencias": eq, "comparativo_diario": comp, "precos_suspeitos": susp, "resumo": resumo}


def main():
    precos = nekt.load_table(layer_name="bbb_banheiras", table_name="precos_coleta")
    try:
        revisao = nekt.load_table(layer_name="bbb_banheiras", table_name="revisao_pares")
    except Exception:
        revisao = None  # tabela manual ainda não criada
    res = rodar(precos, revisao)
    nekt.save_table(df=res["equivalencias"], layer_name="bbb_banheiras", table_name="equivalencias", mode="overwrite")
    # comparativo_diario: histórico. Regravar o mesmo dia substitui (não duplica) as linhas daquele dia.
    comp = res["comparativo_diario"]
    try:
        ant = nekt.load_table(layer_name="bbb_banheiras", table_name="comparativo_diario")
        ant = ant[ant["data_coleta"].astype(str).str[:10] != res["resumo"]["data_coleta"]]
        comp = pd.concat([ant, comp], ignore_index=True) if len(ant) else comp
    except Exception:
        pass  # primeira execução
    nekt.save_table(df=comp, layer_name="bbb_banheiras", table_name="comparativo_diario", mode="overwrite")
    nekt.save_table(df=res["precos_suspeitos"], layer_name="bbb_banheiras", table_name="precos_suspeitos", mode="overwrite")
    print(json.dumps(res["resumo"], ensure_ascii=False, default=str))
    return res


if __name__ == "__main__":
    main()
