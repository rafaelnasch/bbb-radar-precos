#!/usr/bin/env python3
"""
Gera os JSON do site a partir das tabelas do motor (modo local, CSV).

  python scripts/gerar_site.py --tabelas tabelas [--saida site/dados]

Lê precos_coleta, coleta_log, concorrentes, equivalencias e comparativo_diario; roda motor/painel.sql e
motor/alertas.sql no DuckDB (só troca `ds`.`t` por ds.t) e grava:
  resumo.json, produtos.json, produtos/<arquivo>.json, alertas.json, concorrentes.json
A coluna "faixa" (faixa de mercado) é opcional: sem ela, tudo conta como uma faixa só (intermediaria),
como o próprio SQL faz com histórico antigo.
Nada aqui inventa número: o que não está nas tabelas sai como null e o site mostra "não verificado".
"""
import argparse
import hashlib
import json
import math
import os
import re
import shutil
import statistics
import sys
import unicodedata
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import duckdb
import pandas as pd

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DS = "rafaelnasch_bbb_banheiras"
TZ_SP = timezone(timedelta(hours=-3))
LIMITE_BYTES = 5 * 1024 * 1024
FAIXA_PADRAO = "intermediaria"
CATS_PRODUTO = ("banheira", "spa", "ofuro", "infantil", "sauna", "pet")
ROTULO_ALERTA = {
    "concorrente_baixou": "Concorrente baixou o preço",
    "bbb_acima_mercado": "BBB passou a ficar acima do mercado",
    "produto_novo": "Produto novo de concorrente",
    "produto_retirado": "Produto retirado por concorrente",
    "preco_suspeito": "Preço suspeito (conferir)",
    "pix_igual_preco_por": "Pix igual ao preço por (conferir leitura)",
    "divergencia_sites_bbb": "Preço diferente nos dois sites da BBB",
    "fonte_falhou": "Fonte com erro ou bloqueio",
}
UFS = set("AC AL AP AM BA CE DF ES GO MA MT MS MG PA PB PR PE PI RJ RN RS RO RR SC SP SE TO".split())
SPECS = ("lugares", "comprimento_m", "largura_m", "profundidade_m", "material", "com_hidro", "jatos", "mini_jatos",
         "motores", "potencia_cv", "aquecedor_incluso", "cromo_incluso", "garantia_casco_anos")


# ------------------------------------------------------------------ utilidades
def num(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(v) or math.isinf(v) else v


def txt(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    s = str(x).strip()
    return s or None


def booleano(x):
    s = (txt(x) or "").lower()
    if s in ("true", "1", "sim", "yes"):
        return True
    if s in ("false", "0", "não", "nao", "no"):
        return False
    return None


def r1(x):
    return None if x is None else round(x, 1)


def r2(x):
    return None if x is None else round(x, 2)


def mediana(v):
    v = [x for x in v if x is not None]
    return statistics.median(v) if v else None


def posicao(ind):
    if ind is None:
        return "sem par na faixa da BBB"
    if ind < 95:
        return "mais barata"
    if ind <= 105:
        return "na média"
    return "mais cara"


def dominio(u):
    u = txt(u)
    if not u:
        return None
    if "://" not in u:
        u = "https://" + u
    h = (urlparse(u).hostname or "").lower()
    for p in ("www.", "loja.", "shop.", "m."):
        if h.startswith(p):
            h = h[len(p):]
    return h or None


def norm(s):
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def arquivo_sku(sku):
    base = re.sub(r"[^a-z0-9_-]+", "-", sku.lower()).strip("-")[:40] or "p"
    return f"{base}-{hashlib.md5(sku.encode()).hexdigest()[:6]}.json"


def br(v, casas=0):
    if v is None:
        return "não verificado"
    s = f"{v:,.{casas}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def brl(v):
    return "R$ " + br(v, 2) if v is not None else "não verificado"


def gravar(caminho, obj):
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    s = json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)
    if len(s.encode()) > LIMITE_BYTES:
        raise SystemExit(f"{caminho} passou de 5 MB ({len(s.encode())} bytes)")
    with open(caminho, "w", encoding="utf-8") as fh:
        fh.write(s)
    return len(s.encode())


# ------------------------------------------------------------------ leitura
def ler(dir_tab, t, obrig=True):
    p = os.path.join(dir_tab, f"{t}.csv")
    if not os.path.exists(p):
        if obrig:
            raise SystemExit(f"tabela {t} não encontrada em {dir_tab}")
        return None
    return pd.read_csv(p, dtype=str, keep_default_na=False)


def conectar(dir_tab):
    con = duckdb.connect()
    con.execute(f"CREATE SCHEMA {DS}")
    tipos = {"produto_id": "VARCHAR", "sku": "VARCHAR", "variante": "VARCHAR", "bbb_sku": "VARCHAR", "bbb_variante": "VARCHAR",
             "conc_produto_id": "VARCHAR", "conc_variante": "VARCHAR", "data_coleta": "VARCHAR", "coletado_em": "VARCHAR",
             "erro": "VARCHAR", "motivo_status": "VARCHAR", "dominio": "VARCHAR", "faixa": "VARCHAR", "faixa_bbb": "VARCHAR",
             "fonte_id": "VARCHAR", "endpoint": "VARCHAR", "url": "VARCHAR", "url_conc": "VARCHAR", "url_bbb": "VARCHAR"}
    for t in ("precos_coleta", "coleta_log", "concorrentes", "equivalencias", "comparativo_diario"):
        p = os.path.join(dir_tab, f"{t}.csv")
        if not os.path.exists(p):
            if t == "comparativo_diario":  # primeira execução: usa o dia de hoje
                p = os.path.join(dir_tab, "equivalencias.csv")
            else:
                raise SystemExit(f"tabela {t} não encontrada em {dir_tab}")
        cols = list(pd.read_csv(p, nrows=0).columns)
        ty = {k: v for k, v in tipos.items() if k in cols}
        extra = ""
        if t in ("equivalencias", "comparativo_diario"):
            # tolerância: motor antigo sem faixa de mercado
            extra = "".join(f", CAST(NULL AS VARCHAR) AS {c}" for c in ("faixa", "faixa_bbb") if c not in cols)
        con.execute(f"CREATE VIEW {DS}.{t} AS SELECT *{extra} FROM read_csv_auto('{p}', types={ty!r}, header=true)")
    return con


def sql(nome):
    s = open(os.path.join(RAIZ, "motor", nome), encoding="utf-8").read()
    return re.sub(r"`(\w+)`\.`(\w+)`", r"\1.\2", s)


# ------------------------------------------------------------------ preparo
def specs_de(row):
    out = {}
    for k in SPECS:
        v = row.get(k)
        if k in ("material",):
            out[k] = txt(v)
        elif k in ("com_hidro", "aquecedor_incluso", "cromo_incluso"):
            out[k] = booleano(v)
        else:
            out[k] = num(v)
    return out


def ultimo_por_produto(pc):
    """Linha mais recente de cada empresa x produto x variante (a do dia em que o motor a usou)."""
    d = pc.copy()
    d["_pid"] = d["produto_id"].where(d["produto_id"] != "", d["url"])
    d["_ord"] = d["data_coleta"].str[:10] + d["coletado_em"]
    d = d.sort_values("_ord").drop_duplicates(["empresa_slug", "_pid", "variante"], keep="last")
    return {(r["empresa_slug"], r["_pid"], r["variante"]): r for r in d.to_dict("records")}


def opcionais_de(row):
    try:
        a = json.loads(row.get("atributos_json") or "{}")
    except (ValueError, TypeError):
        return []
    out = []
    for o in a.get("opcionais") or []:
        if isinstance(o, dict) and o.get("item"):
            out.append({"item": o.get("item"), "preco": num(o.get("preco"))})
    return out


# ------------------------------------------------------------------ como negociar (regras, sem IA)
def diferencas(bbb, conc):
    """Lista de diferenças concretas, cada uma marcada 'bbb' (a favor da BBB), 'conc' (a favor do concorrente)
    ou 'neutro'. Só compara o que os dois lados têm informado."""
    out = []

    def cmp_num(chave, rot, unidade="", casas=0, maior_melhor=True):
        a, b = bbb.get(chave), conc.get(chave)
        if a is None or b is None or abs(a - b) < 1e-9:
            return
        lado = "bbb" if (a > b) == maior_melhor else "conc"
        out.append({"lado": lado, "texto": f"{rot}: BBB {br(a, casas)}{unidade} x concorrente {br(b, casas)}{unidade}"})

    cmp_num("jatos", "Jatos")
    cmp_num("mini_jatos", "Mini jatos")
    cmp_num("motores", "Motobombas")
    cmp_num("potencia_cv", "Potência", " cv", 1)
    cmp_num("lugares", "Lugares")
    ca, la, cb, lb = bbb.get("comprimento_m"), bbb.get("largura_m"), conc.get("comprimento_m"), conc.get("largura_m")
    if None not in (ca, la, cb, lb):
        aa, ab = max(ca, la) * min(ca, la), max(cb, lb) * min(cb, lb)
        if ab > 0 and abs(aa / ab - 1) >= 0.03:
            lado = "bbb" if aa > ab else "conc"
            out.append({"lado": lado, "texto": f"Medida: BBB {br(max(ca, la), 2)} x {br(min(ca, la), 2)} m x concorrente "
                                                f"{br(max(cb, lb), 2)} x {br(min(cb, lb), 2)} m ({br((aa / ab - 1) * 100, 0)}% de área)"})
    ma, mb = bbb.get("material"), conc.get("material")
    if ma and mb and ma != mb and "outro" not in (ma, mb):
        out.append({"lado": "neutro", "texto": f"Material: BBB {ma} x concorrente {mb}"})
    ga, gb = bbb.get("garantia_casco_anos"), conc.get("garantia_casco_anos")
    if ga is not None and gb is not None and ga != gb:
        out.append({"lado": "bbb" if ga > gb else "conc",
                    "texto": f"Garantia do casco: BBB {br(ga)} anos x concorrente {br(gb)} anos"})
    for chave, rot in (("aquecedor_incluso", "aquecedor"), ("cromo_incluso", "cromoterapia")):
        a, b = bbb.get(chave), conc.get(chave)
        if b is True and a is not True:
            out.append({"lado": "conc", "texto": f"O preço do concorrente já inclui {rot}; na BBB é opcional pago à parte "
                                                 "(quando o item foi verificado, o comparável ajustado já tira esse valor do preço dele)"})
        elif a is True and b is False:
            out.append({"lado": "bbb", "texto": f"A BBB inclui {rot}; o concorrente não"})
    return out


def nao_verificados(conc, ajuste):
    falta = [rot for k, rot in (("jatos", "jatos"), ("motores", "motobombas"), ("potencia_cv", "potência"),
                                ("comprimento_m", "medida"), ("material", "material"), ("lugares", "lugares"),
                                ("garantia_casco_anos", "garantia")) if conc.get(k) in (None, "")]
    for m in re.findall(r"([a-zçãéí ]+?) do concorrente não verificado", ajuste or ""):
        falta.append(m.strip())
    vistos, out = set(), []
    for f in falta:
        if f not in vistos:
            vistos.add(f)
            out.append(f)
    return out


def montar_negociar(var, fatos):
    pares = [p for p in var["pares"] if p["na_faixa"]] or var["pares"]
    neg = {"base": "faixa da BBB" if any(p["na_faixa"] for p in var["pares"]) else "todas as faixas",
           "comparacoes": [], "perguntas": [], "avisos": []}
    if not pares:
        neg["avisos"].append("Sem concorrente equivalente coletado hoje para esta medida: não há comparação de preço.")
    else:
        mais_barato = min(pares, key=lambda p: p["ajustado"])
        med = var["stats"]["mediana"]
        alvo_med = min(pares, key=lambda p: abs(p["ajustado"] - med)) if med else None
        alvos = [("mais barato", mais_barato)]
        if alvo_med is not None and alvo_med is not mais_barato:
            alvos.append(("na mediana", alvo_med))
        for rot, p in alvos:
            dif = diferencas(var["specs"], p["specs"])
            falta = nao_verificados(p["specs"], p["ajuste"])
            neg["comparacoes"].append({
                "rotulo": rot, "empresa": p["empresa"], "produto": p["produto"], "url": p["url"],
                "preco": p["preco"], "ajustado": p["ajustado"], "dif_pct": p["dif_pct"],
                "diferencas": dif, "nao_verificado": falta,
            })
        falta_total = set()
        for c in neg["comparacoes"]:
            falta_total |= set(c["nao_verificado"])
        if falta_total:
            neg["avisos"].append("Dado do concorrente não verificado na fonte (" + ", ".join(sorted(falta_total)) +
                                 "): pergunte ao cliente antes de comparar.")
        if var["stats"]["n_pares"] and var["stats"]["n_pares"] < 3:
            neg["avisos"].append(f"Só {var['stats']['n_pares']} par(es) na faixa da BBB: a mediana é frágil.")
    # 3 perguntas: prioriza o que falta verificar
    falta_total = set()
    for c in neg["comparacoes"]:
        falta_total |= set(c["nao_verificado"])
    prior = []
    if falta_total & {"aquecedor", "cromoterapia", "acionador digital"}:
        prior.append("itens")
    if falta_total & {"jatos", "motobombas", "potência"}:
        prior.append("motor")
    if falta_total & {"medida", "lugares"}:
        prior.append("medida")
    if falta_total & {"material", "garantia"}:
        prior.append("material")
    for k in ("itens", "motor", "material", "frete", "medida"):
        if k not in prior:
            prior.append(k)
    pergs = {p["chave"]: p["texto"] for p in fatos.get("perguntas", [])}
    neg["perguntas"] = [pergs[k] for k in prior if k in pergs][:3]
    return neg


# ------------------------------------------------------------------ principal
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tabelas", default=os.path.join(RAIZ, "tabelas"))
    ap.add_argument("--saida", default=os.path.join(RAIZ, "site", "dados"))
    ap.add_argument("--cadastro", default=os.path.join(RAIZ, "dados_base", "cadastro_concorrentes.csv"))
    ap.add_argument("--fatos", default=os.path.join(RAIZ, "dados_base", "fatos_bbb.json"))
    a = ap.parse_args()

    fatos = json.load(open(a.fatos, encoding="utf-8"))
    pc = ler(a.tabelas, "precos_coleta")
    lg = ler(a.tabelas, "coleta_log")
    cc = ler(a.tabelas, "concorrentes")
    eq = ler(a.tabelas, "equivalencias")
    for c in ("faixa", "faixa_bbb"):
        if c not in eq.columns:
            eq[c] = ""
    eq["faixa"] = eq["faixa"].replace("", FAIXA_PADRAO)
    eq["faixa_bbb"] = eq["faixa_bbb"].replace("", FAIXA_PADRAO)
    tem_faixa = "faixa" in pd.read_csv(os.path.join(a.tabelas, "equivalencias.csv"), nrows=0).columns

    dia = eq["data_coleta"].str[:10].max()
    eq = eq[eq["data_coleta"].str[:10] == dia]
    pc["data_coleta"] = pc["data_coleta"].str[:10]
    lg["data_coleta"] = lg["data_coleta"].str[:10]
    pc_dia = pc[pc["data_coleta"] == dia]
    lg_dia = lg[lg["data_coleta"] == dia]

    con = conectar(a.tabelas)
    painel = con.execute(sql("painel.sql")).df()
    alertas = con.execute(sql("alertas.sql")).df()
    painel["bbb_sku"] = painel["bbb_sku"].astype(str)
    pn = {r["bbb_sku"]: r for r in painel.to_dict("records")}

    saida = a.saida
    dir_prod = os.path.join(saida, "produtos")
    shutil.rmtree(dir_prod, ignore_errors=True)
    os.makedirs(dir_prod, exist_ok=True)
    tamanhos = {}

    # ---------------- produtos BBB
    ult = ultimo_por_produto(pc)
    bbb_site = pc_dia[(pc_dia["empresa_slug"] == "bbb") & (pc_dia["canal"] == "site")].copy()
    bbb_site["_pref"] = bbb_site["dominio"].str.replace("www.", "", regex=False).map(
        lambda d: 0 if d == "banheirasbombanho.com.br" else (1 if d == "bbbbanheiras.com.br" else 2))
    bbb_site = bbb_site[bbb_site["_pref"] < 2].sort_values(["_pref", "coletado_em"])
    bbb_var = {}
    for r in bbb_site.to_dict("records"):
        bbb_var.setdefault((r["sku"], r["variante"]), r)  # bombanho primeiro

    ok = eq[eq["classe"].isin(["exato", "equivalente"])].copy()
    ok["_aj"] = pd.to_numeric(ok["conc_preco_ajustado"], errors="coerce")
    ok = ok[ok["_aj"] > 0]
    ref = eq[eq["classe"] == "referencia"].copy()
    ref["_nota"] = pd.to_numeric(ref["nota"], errors="coerce")
    ref["_aj"] = pd.to_numeric(ref["conc_preco_ajustado"], errors="coerce")
    ref = ref[ref["_aj"] > 0]

    def par_json(p):
        chave = (p["conc_slug"], p["conc_produto_id"] or p["url_conc"], p["conc_variante"])
        cr = ult.get(chave)
        return {
            "classe": p["classe"], "empresa": p["conc_empresa"], "slug": p["conc_slug"], "faixa": p["faixa"],
            "na_faixa": p["faixa"] == p["faixa_bbb"], "produto": p["conc_nome"], "variante": txt(p["conc_variante"]),
            "url": txt(p["url_conc"]), "preco": num(p["conc_preco"]), "ajustado": num(p["conc_preco_ajustado"]),
            "ajuste": txt(p["ajuste_descricao"]), "dif_pct": r1(num(p["gap_pct"])), "nota": num(p["nota"]),
            "motivos": txt(p["motivos"]), "specs": specs_de(cr) if cr is not None else {k: None for k in SPECS},
        }

    skus = {}
    for (sku, var), r in bbb_var.items():
        skus.setdefault(sku, {"nome": r["nome"], "categoria": r["categoria"], "url": r["url"]})
    for r in eq[["bbb_sku", "bbb_nome", "bbb_categoria", "url_bbb"]].drop_duplicates("bbb_sku").to_dict("records"):
        skus.setdefault(r["bbb_sku"], {"nome": r["bbb_nome"], "categoria": r["bbb_categoria"], "url": r["url_bbb"]})

    ok_g = {k: g for k, g in ok.groupby(["bbb_sku", "bbb_variante"])}
    ref_g = {k: g for k, g in ref.groupby(["bbb_sku", "bbb_variante"])}
    eq_vars = eq[["bbb_sku", "bbb_variante", "bbb_preco", "url_bbb", "faixa_bbb"]].drop_duplicates(["bbb_sku", "bbb_variante"])
    vars_por_sku = {}
    for r in eq_vars.to_dict("records"):
        vars_por_sku.setdefault(r["bbb_sku"], {})[r["bbb_variante"]] = r
    for (sku, var), r in bbb_var.items():
        vars_por_sku.setdefault(sku, {}).setdefault(var, {"bbb_sku": sku, "bbb_variante": var, "bbb_preco": r["preco_por"],
                                                        "url_bbb": r["url"], "faixa_bbb": None})
    faixa_bbb_dia = (eq["faixa_bbb"].mode().iloc[0] if len(eq) else None) if tem_faixa else None

    lista = []
    for sku, info in sorted(skus.items()):
        if info["categoria"] not in CATS_PRODUTO and sku not in pn:
            continue
        variantes = []
        for var, vr in vars_por_sku.get(sku, {}).items():
            b = bbb_var.get((sku, var))
            specs = specs_de(b) if b is not None else {k: None for k in SPECS}
            preco = num(vr["bbb_preco"])
            g = ok_g.get((sku, var))
            pares = [par_json(p) for p in g.sort_values("_aj").to_dict("records")] if g is not None else []
            pares.sort(key=lambda p: (not p["na_faixa"], p["ajustado"]))
            na = [p["ajustado"] for p in pares if p["na_faixa"]]
            med = mediana(na)
            ind = r1(preco / med * 100) if (preco and med) else None
            med_t = mediana([p["ajustado"] for p in pares])
            gr = ref_g.get((sku, var))
            refs = [par_json(p) for p in gr.sort_values(["_nota", "_aj"], ascending=[False, True]).head(5).to_dict("records")] \
                if gr is not None else []
            v = {
                "variante": txt(var), "preco": preco, "url": txt(vr["url_bbb"]) or info["url"], "specs": specs,
                "opcionais": opcionais_de(b) if b is not None else [],
                "stats": {"n_pares": len(na), "mediana": r2(med), "menor": r2(min(na)) if na else None,
                          "maior": r2(max(na)) if na else None, "indice": ind, "posicao": posicao(ind),
                          "n_todas": len(pares), "indice_todas": r1(preco / med_t * 100) if (preco and med_t) else None},
                "pares": pares, "referencia": refs,
            }
            v["negociar"] = montar_negociar(v, fatos)
            variantes.append(v)

        def chave_var(v):
            s = v["specs"]
            area = (s["comprimento_m"] or 0) * (s["largura_m"] or 0)
            return (area == 0, area, v["preco"] or 0, v["variante"] or "")
        variantes.sort(key=chave_var)
        p = pn.get(sku)
        padrao = txt(p["bbb_variante_padrao"]) if p else None
        painel_j = None
        if p:
            painel_j = {k: (None if (isinstance(val, float) and math.isnan(val)) else val) for k, val in p.items()
                        if k not in ("data_coleta",)}
            painel_j = json.loads(json.dumps(painel_j, default=str))
        arq = arquivo_sku(sku)
        doc = {"sku": sku, "nome": info["nome"], "categoria": info["categoria"], "url": info["url"], "data_coleta": dia,
               "faixa_bbb": faixa_bbb_dia, "variante_padrao": padrao, "painel": painel_j, "variantes": variantes}
        tamanhos["produtos/" + arq] = gravar(os.path.join(dir_prod, arq), doc)
        ind = num(p["indice"]) if p else None
        vp = next((v for v in variantes if v["variante"] == padrao), variantes[0] if variantes else None)
        lista.append({"sku": sku, "nome": info["nome"], "cat": info["categoria"], "arq": arq,
                      "preco": num(p["bbb_preco"]) if p else (vp["preco"] if vp else None),
                      "indice": r1(ind), "pos": p["posicao"] if p else "sem par",
                      "n": int(p["n_pares"]) if p else 0, "nv": len(variantes)})
    lista.sort(key=lambda x: x["nome"])
    tamanhos["produtos.json"] = gravar(os.path.join(saida, "produtos.json"), {"data_coleta": dia, "produtos": lista})

    # ---------------- alertas
    alertas = alertas.astype(object).where(alertas.notna(), None)
    grupos = []
    ordem = list(ROTULO_ALERTA)
    for tipo, g in sorted(alertas.groupby("tipo"), key=lambda kv: ordem.index(kv[0]) if kv[0] in ordem else 99):
        sev = g["severidade"].tolist()
        itens = []
        for r in g.head(150).to_dict("records"):
            itens.append({"sev": r["severidade"], "empresa": r["empresa"], "slug": r["empresa_slug"], "sku": txt(r["bbb_sku"]),
                          "produto": r["produto"], "detalhe": r["detalhe"], "antes": num(r["valor_anterior"]),
                          "agora": num(r["valor_atual"]), "var": num(r["variacao_pct"]), "url": txt(r["url"])})
        grupos.append({"tipo": tipo, "rotulo": ROTULO_ALERTA.get(tipo, tipo),
                       "severidade": "alta" if "alta" in sev else ("media" if "media" in sev else "baixa"),
                       "n": len(g), "itens": itens, "cortado": len(g) > 150})
    tamanhos["alertas.json"] = gravar(os.path.join(saida, "alertas.json"),
                                      {"data_coleta": dia, "total": int(len(alertas)), "grupos": grupos})

    # ---------------- fontes e concorrentes
    cc_f = {r["fonte_id"]: r for r in cc.to_dict("records")} if "fonte_id" in cc.columns else {}
    fontes = []
    for r in lg_dia.sort_values("coletado_em").drop_duplicates("fonte_id" if "fonte_id" in lg_dia.columns else "empresa_slug",
                                                                 keep="last").to_dict("records"):
        c = cc_f.get(r.get("fonte_id"), {})
        fontes.append({"fonte_id": r.get("fonte_id") or r["empresa_slug"], "slug": r["empresa_slug"],
                       "empresa": c.get("empresa") or r["empresa_slug"], "canal": c.get("canal"),
                       "dominio": dominio(c.get("endpoint")), "status": r["status"],
                       "produtos": num(r.get("produtos")), "variantes": num(r.get("variantes")),
                       "duracao_s": num(r.get("duracao_s")), "erro": (txt(r.get("erro")) or "")[:220] or None})
    fontes.sort(key=lambda f: ({"erro": 0, "bloqueado": 1, "parcial": 2}.get(f["status"], 3), f["empresa"]))
    cont_f = {}
    for f in fontes:
        cont_f[f["status"]] = cont_f.get(f["status"], 0) + 1
    st_slug = {}
    for f in fontes:
        s = st_slug.setdefault(f["slug"], {"status": [], "produtos": 0})
        s["status"].append(f["status"])
        s["produtos"] += int(f["produtos"] or 0)
    pc_ok = pc_dia[(pc_dia["status_preco"] == "ok") & (pd.to_numeric(pc_dia["preco_por"], errors="coerce") > 0)]
    conc_com_preco = sorted(set(pc_ok["empresa_slug"]) - {"bbb"})

    dom_slug, nome_slug = {}, {}
    for r in cc.to_dict("records"):
        d = dominio(r.get("endpoint"))
        if d and r["empresa_slug"] != "bbb" and "mitiendanube" not in d and "climbacommerce" not in d:
            dom_slug.setdefault(d, r["empresa_slug"])
        nome_slug.setdefault(norm(r.get("empresa")), r["empresa_slug"])
    cad = []
    if os.path.exists(a.cadastro):
        cd = pd.read_csv(a.cadastro, dtype=str, keep_default_na=False, encoding="utf-8-sig")
        for r in cd.to_dict("records"):
            site = txt(r.get("site_canal"))
            slug = dom_slug.get(dominio(site)) if site else None
            slug = slug or nome_slug.get(norm(r.get("nome")))
            ufs = [u for u in re.findall(r"\b([A-Z]{2})\b", r.get("cidade_uf") or "") if u in UFS]
            tp = (r.get("tem_preco_publico") or "").lower()
            tem = "sim" if tp.startswith("sim") else ("não" if tp.startswith("não (") or tp == "não" else "não verificado")
            s = st_slug.get(slug) if slug else None
            st = None
            if s:
                st = "ok" if all(x == "ok" for x in s["status"]) else (
                    "parcial" if any(x in ("ok", "parcial") for x in s["status"]) else s["status"][0])
            cad.append({"id": r.get("id"), "nome": r.get("nome"), "tipo": txt(r.get("tipo")), "cidade_uf": txt(r.get("cidade_uf")),
                        "uf": ufs[-1] if ufs else None, "rel": txt(r.get("relevancia")),
                        "site": site if site and site.startswith("http") else None, "canais": txt(r.get("canais")),
                        "tem_preco": tem, "prod_cadastro": num(r.get("produtos_coletados")),
                        "slug": slug, "coleta_hoje": st, "prod_hoje": s["produtos"] if s else None,
                        "com_preco_hoje": bool(slug and slug in conc_com_preco)})
    tamanhos["concorrentes.json"] = gravar(os.path.join(saida, "concorrentes.json"),
                                           {"data_coleta": dia, "cadastro": cad, "fontes": fontes})

    # ---------------- resumo
    pv = painel.copy()
    pv["indice"] = pd.to_numeric(pv["indice"], errors="coerce")

    def med_col(col, df=pv):
        v = pd.to_numeric(df[col], errors="coerce").dropna()
        return {"indice": r1(float(v.median())) if len(v) else None, "n": int(len(v))}

    por_cat = {cat: med_col("indice", g) for cat, g in pv.groupby("bbb_categoria")}
    por_faixa = None
    if tem_faixa:
        por_faixa = {f: med_col("indice_" + f) for f in ("entrada", "intermediaria", "premium")}
        por_faixa["todas"] = med_col("indice_todas_faixas")
    cont = pv["posicao"].value_counts().to_dict()
    n_sem = sum(1 for x in lista if x["pos"] == "sem par")

    def top(df):
        out = []
        for r in df.to_dict("records"):
            out.append({"sku": str(r["bbb_sku"]), "nome": r["bbb_nome"], "cat": r["bbb_categoria"],
                        "variante": txt(r["bbb_variante_padrao"]), "preco": num(r["bbb_preco"]),
                        "mediana": num(r["mediana_preco_conc"]), "indice": num(r["indice"]), "n": int(r["n_pares"]),
                        "mais_barato": txt(r["conc_mais_barato"]), "mais_barato_preco": num(r["conc_mais_barato_preco_ajustado"])})
        return out

    com_ind = pv[pv["indice"].notna()]
    risco = top(com_ind.sort_values("indice", ascending=False).head(15))
    folga = top(com_ind.sort_values("indice", ascending=True).head(10))
    geral = med_col("indice")
    t_ini, t_fim = lg_dia["coletado_em"].min(), lg_dia["coletado_em"].max()
    n_alta = sum(g["n"] for g in grupos if g["severidade"] == "alta")

    # frase do dia (regras)
    frase = None
    if geral["indice"] is not None:
        gi = geral["indice"]
        rel = (f"{br(abs(gi - 100), 0)}% acima" if gi > 100.5 else (f"{br(abs(gi - 100), 0)}% abaixo" if gi < 99.5 else "em linha com"))
        base = "dos concorrentes da mesma faixa de preço" if tem_faixa else "dos concorrentes equivalentes"
        frase = f"Hoje o produto típico da BBB está {rel} da mediana {base}"
        cats = sorted([(k, v) for k, v in por_cat.items() if v["indice"] is not None and v["n"] >= 3], key=lambda kv: kv[1]["indice"])
        nomes = {"banheira": "banheiras", "spa": "spas", "ofuro": "ofurôs", "infantil": "infantis", "sauna": "saunas", "pet": "pet"}
        if len(cats) >= 2:
            frase += (f"; a distância é maior em {nomes.get(cats[-1][0], cats[-1][0])} (índice {br(cats[-1][1]['indice'], 1)})"
                      f" e menor em {nomes.get(cats[0][0], cats[0][0])} ({br(cats[0][1]['indice'], 1)})")
        frase += (f". {cont.get('mais cara', 0)} de {int(pv['indice'].notna().sum())} produtos comparados estão mais caros,"
                  f" {cont.get('na média', 0)} na média e {cont.get('mais barata', 0)} mais baratos.")

    resumo = {
        "data_coleta": dia, "coleta_inicio": txt(t_ini), "coleta_fim": txt(t_fim),
        "gerado_em": datetime.now(TZ_SP).isoformat(timespec="minutes"),
        "fonte": "sites públicos dos concorrentes e da BBB", "tem_faixa": tem_faixa, "faixa_bbb": faixa_bbb_dia,
        "frase": frase, "indice": geral, "por_categoria": por_cat, "por_faixa": por_faixa,
        "posicao": {"mais_cara": int(cont.get("mais cara", 0)), "na_media": int(cont.get("na média", 0)),
                    "mais_barata": int(cont.get("mais barata", 0)),
                    "sem_par_faixa": int(cont.get("sem par na faixa da BBB", 0)), "sem_par": n_sem},
        "n_produtos_bbb": len(lista), "n_linhas_coleta": int(len(pc_dia)),
        "pares": {k: int(v) for k, v in eq["classe"].value_counts().to_dict().items()},
        "fontes": {"total": len(fontes), "ok": cont_f.get("ok", 0), "parcial": cont_f.get("parcial", 0),
                   "bloqueado": cont_f.get("bloqueado", 0), "erro": cont_f.get("erro", 0),
                   "outros": sum(v for k, v in cont_f.items() if k not in ("ok", "parcial", "bloqueado", "erro"))},
        "n_concorrentes_com_preco": len(conc_com_preco), "n_cadastro": len(cad),
        "alertas": {"total": int(len(alertas)), "alta": int(n_alta), "por_tipo": {g["tipo"]: g["n"] for g in grupos}},
        "risco": risco, "folga": folga,
    }
    tamanhos["resumo.json"] = gravar(os.path.join(saida, "resumo.json"), resumo)
    tamanhos["fatos.json"] = gravar(os.path.join(saida, "fatos.json"), {
        "fatos": [{"texto": f["texto"], "fonte": f.get("fonte"), "conferir": bool(f.get("conferir"))} for f in fatos.get("fatos", [])]})

    tot = sum(tamanhos.values())
    maior = max(tamanhos.items(), key=lambda kv: kv[1])
    print(f"site/dados de {dia}: {len(lista)} produtos, {len(alertas)} alertas, {len(fontes)} fontes, {len(cad)} no cadastro")
    for k in ("resumo.json", "produtos.json", "alertas.json", "concorrentes.json", "fatos.json"):
        print(f"  {k:20s} {tamanhos[k] / 1024:8.1f} KB")
    print(f"  produtos/*.json     {len(lista)} arquivos, {sum(v for k, v in tamanhos.items() if k.startswith('produtos/')) / 1024:.0f} KB"
          f" (maior: {maior[0]} {maior[1] / 1024:.0f} KB); total {tot / 1024 / 1024:.1f} MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
