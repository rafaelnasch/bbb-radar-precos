#!/usr/bin/env python3
"""
Histórico do radar em CSV gzip por dia (só os últimos N dias).

  python scripts/historico.py remontar --tabelas tabelas
      Junta historico/<tabela>_AAAA-MM-DD.csv.gz em tabelas/<tabela>.csv (formato do modo local do motor),
      para a coleta checar saltos de preço e as consultas calcularem variação de 1 e 7 dias.

  python scripts/historico.py salvar --tabelas tabelas [--dias 30]
      Grava em historico/ o dia mais recente de cada tabela (e os dias que ainda não têm arquivo)
      e apaga os arquivos com mais de N dias contados a partir da coleta mais recente.

Tabelas guardadas: precos_coleta e comparativo_diario (as do pedido) e coleta_log (pequena; os alertas de
produto novo/retirado precisam saber se a fonte estava "ok" no dia anterior).
concorrentes.csv (tabela do motor, sobrescrita a cada coleta) fica em historico/concorrentes.csv como reserva.
"""
import argparse
import glob
import gzip
import io
import os
import re
import shutil
import sys
from datetime import date, timedelta

import pandas as pd

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HIST = os.path.join(RAIZ, "historico")
TABELAS = ("precos_coleta", "comparativo_diario", "coleta_log")
PADRAO = re.compile(r"^(?P<t>[a-z_]+)_(?P<d>\d{4}-\d{2}-\d{2})\.csv\.gz$")


def arquivos(tabela):
    out = {}
    for p in glob.glob(os.path.join(HIST, f"{tabela}_*.csv.gz")):
        m = PADRAO.match(os.path.basename(p))
        if m and m.group("t") == tabela:
            out[m.group("d")] = p
    return dict(sorted(out.items()))


def ler(p):
    return pd.read_csv(p, dtype=str, keep_default_na=False, compression="gzip")


def gravar_gz(df, p):
    """gzip determinístico (mtime 0): o mesmo conteúdo gera o mesmo arquivo, sem ruído no git."""
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    tmp = p + ".tmp"
    with open(tmp, "wb") as fh:
        with gzip.GzipFile(filename="", mode="wb", fileobj=fh, compresslevel=9, mtime=0) as gz:
            gz.write(buf.getvalue().encode("utf-8"))
    os.replace(tmp, p)


def remontar(dir_tab):
    os.makedirs(dir_tab, exist_ok=True)
    for t in TABELAS:
        arqs = arquivos(t)
        if not arqs:
            print(f"{t}: sem histórico")
            continue
        df = pd.concat([ler(p) for p in arqs.values()], ignore_index=True)
        df.to_csv(os.path.join(dir_tab, f"{t}.csv"), index=False)
        print(f"{t}: {len(arqs)} dia(s), {len(df)} linhas ({min(arqs)} a {max(arqs)})")
    cad = os.path.join(HIST, "concorrentes.csv")
    if os.path.exists(cad):  # a coleta sobrescreve; serve de reserva se a coleta cair
        shutil.copy(cad, os.path.join(dir_tab, "concorrentes.csv"))
        print("concorrentes: copiada de historico/")
    rev = os.path.join(RAIZ, "motor", "revisao_pares.csv")
    if os.path.exists(rev):
        shutil.copy(rev, os.path.join(dir_tab, "revisao_pares.csv"))
        print("revisao_pares: copiada de motor/")


def salvar(dir_tab, dias):
    os.makedirs(HIST, exist_ok=True)
    datas_max = []
    cad = os.path.join(dir_tab, "concorrentes.csv")
    if os.path.exists(cad):
        shutil.copy(cad, os.path.join(HIST, "concorrentes.csv"))
    for t in TABELAS:
        p = os.path.join(dir_tab, f"{t}.csv")
        if not os.path.exists(p):
            print(f"{t}: tabela não existe em {dir_tab}")
            continue
        df = pd.read_csv(p, dtype=str, keep_default_na=False)
        if not len(df) or "data_coleta" not in df.columns:
            continue
        df["_d"] = df["data_coleta"].str[:10]
        dmax = df["_d"].max()
        datas_max.append(dmax)
        ja = arquivos(t)
        for d, g in df.groupby("_d"):
            if d == dmax or d not in ja:
                gravar_gz(g.drop(columns="_d"), os.path.join(HIST, f"{t}_{d}.csv.gz"))
                print(f"{t}: gravado {d} ({len(g)} linhas)")
    if not datas_max:
        return
    corte = (date.fromisoformat(max(datas_max)) - timedelta(days=dias - 1)).isoformat()
    for t in TABELAS:
        for d, p in arquivos(t).items():
            if d < corte:
                os.remove(p)
                print(f"{t}: apagado {d} (antes de {corte})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("acao", choices=["remontar", "salvar"])
    ap.add_argument("--tabelas", default=os.path.join(RAIZ, "tabelas"))
    ap.add_argument("--dias", type=int, default=30)
    a = ap.parse_args()
    if a.acao == "remontar":
        remontar(a.tabelas)
    else:
        salvar(a.tabelas, a.dias)
    return 0


if __name__ == "__main__":
    sys.exit(main())
