#!/usr/bin/env python3
"""
CREDI — Gerador automático do Dashboard Meta Ads
Lê a planilha do Google Sheets (Stract) e gera o HTML atualizado.
"""

import pandas as pd
import json
import re
import hashlib
import requests
from datetime import date, timedelta
from pathlib import Path

# ── CONFIG ────────────────────────────────────────────
SHEET_ID = "1L3bUusX8nwynFcBxWW_9l-ouLVwOEy5IMuA8w5CI7xA"
SHEET_TAB = "meta-ads"
OUTPUT_FILE = "index.html"
TEMPLATE_FILE = "template_base.html"

SHEET_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_TAB}"
SHEET_URL_GA = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=breakdown-gender-age"
SHEET_URL_RG = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=breakdown-regiao"
SHEET_URL_PT = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=breakdown-platform"


# ── DOWNLOAD DE IMAGENS ───────────────────────────────
def download_thumb(url, img_dir):
    if not url or str(url) == "nan":
        return ""
    try:
        ext = ".png" if ".png" in url.lower() else ".jpg"
        fname = hashlib.md5(url.encode()).hexdigest()[:16] + ext
        fpath = img_dir / fname
        if not fpath.exists():
            r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200:
                fpath.write_bytes(r.content)
            else:
                return ""
        return "imgs/" + fname
    except Exception:
        return ""


# ── LER PLANILHA ──────────────────────────────────────
def load_sheet():
    print(f"Lendo planilha...")
    df = pd.read_csv(SHEET_URL)

    col_map = {
        "Date": "date",
        "Campaign Name": "campaign",
        "Adset Name": "adset",
        "Ad Name": "ad",
        "Thumbnail URL": "thumb",
        "Spend (Cost, Amount Spent)": "spend",
        "Impressions": "impressions",
        "Clicks": "clicks",
        "Action Link Clicks": "link_clicks",
        "Action Messaging Conversations Started (Onsite Conversion)": "leads",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for c in ["spend", "leads", "impressions", "clicks", "link_clicks"]:
        if c in df.columns:
            df[c] = pd.to_numeric(
                df[c].astype(str).str.replace(",", ".", regex=False),
                errors="coerce"
            ).fillna(0)

    df["product"] = df["campaign"].apply(
        lambda c: "FGTS" if "FGTS" in str(c).upper() else "CLT"
    )
    df["ym"] = df["date"].dt.to_period("M")
    df = df.dropna(subset=["date"])

    last = df["date"].max()
    print(f"OK: {len(df)} linhas | {df['date'].min().date()} -> {last.date()}")
    print(f"Spend total: R${df['spend'].sum():,.2f} | Leads: {int(df['leads'].sum()):,}")
    return df


# ── DADOS DIÁRIOS ─────────────────────────────────────
def build_daily(df):
    daily = df.groupby("date").agg(
        spend=("spend", "sum"),
        leads=("leads", "sum"),
        impressions=("impressions", "sum"),
        clicks=("clicks", "sum"),
        link_clicks=("link_clicks", "sum"),
    ).reset_index().sort_values("date")

    daily_clt = df[df["product"] == "CLT"].groupby("date").agg(
        spend=("spend", "sum"),
        leads=("leads", "sum"),
        impressions=("impressions", "sum"),
        link_clicks=("link_clicks", "sum"),
    ).reset_index()

    daily_fgts = df[df["product"] == "FGTS"].groupby("date").agg(
        spend=("spend", "sum"),
        leads=("leads", "sum"),
        impressions=("impressions", "sum"),
        link_clicks=("link_clicks", "sum"),
    ).reset_index()

    all_days = sorted(daily["date"].unique())[-60:]

    out = {k: [] for k in [
        "days", "spend", "leads", "cpl", "ctr", "cpm",
        "cltL", "fgtsL", "cltS", "fgtsS", "cltCPL", "fgtsCPL",
        "cltCTR", "fgtsCTR",
    ]}

    for d in all_days:
        r = daily[daily["date"] == d].iloc[0]
        cr = daily_clt[daily_clt["date"] == d]
        fr = daily_fgts[daily_fgts["date"] == d]

        cl = int(cr["leads"].sum()) if len(cr) else 0
        fl = int(fr["leads"].sum()) if len(fr) else 0
        cs = round(float(cr["spend"].sum()), 2) if len(cr) else 0
        fs = round(float(fr["spend"].sum()), 2) if len(fr) else 0
        cl_imp = float(cr["impressions"].sum()) if len(cr) else 0
        fl_imp = float(fr["impressions"].sum()) if len(fr) else 0
        cl_lc = float(cr["link_clicks"].sum()) if len(cr) else 0
        fl_lc = float(fr["link_clicks"].sum()) if len(fr) else 0

        tl = int(r["leads"])
        ts = float(r["spend"])
        imp = float(r["impressions"])
        lc = float(r["link_clicks"])

        out["days"].append(pd.Timestamp(d).strftime("%d/%m"))
        out["spend"].append(round(ts, 2))
        out["leads"].append(tl)
        out["cpl"].append(round(ts / tl, 2) if tl > 0 else None)
        out["ctr"].append(round(lc / imp * 100, 2) if imp > 0 else None)
        out["cpm"].append(round(ts / imp * 1000, 2) if imp > 0 else None)
        out["cltL"].append(cl)
        out["fgtsL"].append(fl)
        out["cltS"].append(cs)
        out["fgtsS"].append(fs)
        out["cltCPL"].append(round(cs / cl, 2) if cl > 0 else None)
        out["fgtsCPL"].append(round(fs / fl, 2) if fl > 0 else None)
        out["cltCTR"].append(round(cl_lc / cl_imp * 100, 2) if cl_imp > 0 else None)
        out["fgtsCTR"].append(round(fl_lc / fl_imp * 100, 2) if fl_imp > 0 else None)

    last_day = out["days"][-1] if out["days"] else "—"
    return out, last_day, all_days


# ── KPIs POR PERÍODO ──────────────────────────────────
def build_kpis(df, all_days):
    last = pd.Timestamp(all_days[-1])
    kpis = {}

    for n in [1, 7, 14, 30]:
        start = last - pd.Timedelta(days=n - 1)
        p = df[(df["date"] >= start) & (df["date"] <= last)]
        clt = p[p["product"] == "CLT"]
        fgts = p[p["product"] == "FGTS"]

        tS = float(p["spend"].sum())
        tL = int(p["leads"].sum())
        imp = float(p["impressions"].sum())
        lc = float(p["link_clicks"].sum())
        cltS = float(clt["spend"].sum())
        cltL = int(clt["leads"].sum())
        cltImp = float(clt["impressions"].sum())
        cltLc = float(clt["link_clicks"].sum())
        fgtsS = float(fgts["spend"].sum())
        fgtsL = int(fgts["leads"].sum())
        fgtsImp = float(fgts["impressions"].sum())
        fgtsLc = float(fgts["link_clicks"].sum())

        kpis[str(n)] = {
            "spend": round(tS, 2), "leads": tL,
            "cpl": round(tS / tL, 2) if tL else None,
            "ctr": round(lc / imp * 100, 2) if imp else None,
            "cpm": round(tS / imp * 1000, 2) if imp else None,
            "cltSpend": round(cltS, 2), "cltLeads": cltL,
            "cltCpl": round(cltS / cltL, 2) if cltL else None,
            "cltCtr": round(cltLc / cltImp * 100, 2) if cltImp else None,
            "fgtsSpend": round(fgtsS, 2), "fgtsLeads": fgtsL,
            "fgtsCpl": round(fgtsS / fgtsL, 2) if fgtsL else None,
            "fgtsCtr": round(fgtsLc / fgtsImp * 100, 2) if fgtsImp else None,
        }
        print(f"   {n}d: R${tS:,.0f} | {tL:,} leads | CTR {lc/imp*100:.2f}% | CPM R${tS/imp*1000:.2f}" if imp else f"   {n}d: sem dados")

    for ym_str in ["2026-04", "2026-03", "2026-02", "2026-01", "2025-12", "2025-11", "2025-10", "2025-09"]:
        try:
            ym = pd.Period(ym_str, "M")
            p = df[df["ym"] == ym]
            if len(p) == 0:
                continue
            clt = p[p["product"] == "CLT"]
            fgts = p[p["product"] == "FGTS"]
            tS = float(p["spend"].sum())
            tL = int(p["leads"].sum())
            imp = float(p["impressions"].sum())
            lc = float(p["link_clicks"].sum())
            cltS = float(clt["spend"].sum())
            cltL = int(clt["leads"].sum())
            cltImp = float(clt["impressions"].sum())
            cltLc = float(clt["link_clicks"].sum())
            fgtsS = float(fgts["spend"].sum())
            fgtsL = int(fgts["leads"].sum())
            fgtsImp = float(fgts["impressions"].sum())
            fgtsLc = float(fgts["link_clicks"].sum())
            kpis[ym_str] = {
                "spend": round(tS, 2), "leads": tL,
                "cpl": round(tS / tL, 2) if tL else None,
                "ctr": round(lc / imp * 100, 2) if imp else None,
                "cpm": round(tS / imp * 1000, 2) if imp else None,
                "cltSpend": round(cltS, 2), "cltLeads": cltL,
                "cltCpl": round(cltS / cltL, 2) if cltL else None,
                "cltCtr": round(cltLc / cltImp * 100, 2) if cltImp else None,
                "fgtsSpend": round(fgtsS, 2), "fgtsLeads": fgtsL,
                "fgtsCpl": round(fgtsS / fgtsL, 2) if fgtsL else None,
                "fgtsCtr": round(fgtsLc / fgtsImp * 100, 2) if fgtsImp else None,
            }
        except Exception as e:
            print(f"   {ym_str}: erro {e}")

    return kpis


# ── CAMPANHAS POR PERÍODO ─────────────────────────────
def build_camps_period(df, start_dt, end_dt, all_months):
    p = df[(df["date"] >= pd.Timestamp(start_dt)) & (df["date"] <= pd.Timestamp(end_dt))]
    if len(p) == 0:
        return []

    camps = p.groupby(["campaign", "product"]).agg(
        spend=("spend", "sum"), leads=("leads", "sum"),
        impressions=("impressions", "sum"), link_clicks=("link_clicks", "sum"),
    ).reset_index()
    camps["cpl"] = (camps["spend"] / camps["leads"]).where(camps["leads"] > 0).round(2)
    camps["cpm"] = (camps["spend"] / camps["impressions"] * 1000).where(camps["impressions"] > 0).round(2)
    camps["ctr"] = (camps["link_clicks"] / camps["impressions"] * 100).where(camps["impressions"] > 0).round(2)
    camps = camps.sort_values("leads", ascending=False).head(12)

    cur_ym = pd.Period(end_dt, "M")
    cur_idx = list(all_months).index(cur_ym) if cur_ym in all_months else len(all_months) - 1
    spk_months = all_months[max(0, cur_idx - 5):cur_idx + 1]

    out = []
    for _, r in camps.iterrows():
        adsets = p[p["campaign"] == r["campaign"]].groupby("adset").agg(
            spend=("spend", "sum"), leads=("leads", "sum"),
            impressions=("impressions", "sum"), link_clicks=("link_clicks", "sum"),
        ).reset_index()
        adsets["cpl"] = (adsets["spend"] / adsets["leads"]).where(adsets["leads"] > 0).round(2)
        adsets["cpm"] = (adsets["spend"] / adsets["impressions"] * 1000).where(adsets["impressions"] > 0).round(2)
        adsets["ctr"] = (adsets["link_clicks"] / adsets["impressions"] * 100).where(adsets["impressions"] > 0).round(2)
        adsets = adsets.sort_values("leads", ascending=False)

        spk = []
        for sm in spk_months:
            cm = df[(df["ym"] == sm) & (df["campaign"] == r["campaign"])]
            ts2 = float(cm["spend"].sum())
            tl2 = float(cm["leads"].sum())
            spk.append(round(ts2 / tl2, 2) if tl2 > 0 else None)

        conjs = []
        for _, a in adsets.iterrows():
            conjs.append({
                "n": str(a["adset"]),
                "spend": round(float(a["spend"]), 2),
                "leads": int(a["leads"]),
                "cpl": float(a["cpl"]) if pd.notna(a["cpl"]) else None,
                "cpm": float(a["cpm"]) if pd.notna(a["cpm"]) else None,
                "ctr": float(a["ctr"]) if pd.notna(a["ctr"]) else None,
            })

        out.append({
            "n": str(r["campaign"]),
            "product": str(r["product"]),
            "spend": round(float(r["spend"]), 2),
            "leads": int(r["leads"]),
            "cpl": float(r["cpl"]) if pd.notna(r["cpl"]) else None,
            "cpm": float(r["cpm"]) if pd.notna(r["cpm"]) else None,
            "ctr": float(r["ctr"]) if pd.notna(r["ctr"]) else None,
            "spk": spk,
            "conjs": conjs,
        })
    return out


def build_camps(df, all_days):
    all_months = sorted(df["ym"].unique())
    last = pd.Timestamp(all_days[-1])
    result = {}

    for n in [1, 7, 14, 30]:
        start = last - pd.Timedelta(days=n - 1)
        result[str(n)] = build_camps_period(df, start, last, all_months)
        print(f"   {n}d: {len(result[str(n)])} campanhas")

    for ym_str in ["2026-04", "2026-03", "2026-02", "2026-01", "2025-12", "2025-11", "2025-10", "2025-09"]:
        try:
            ym = pd.Period(ym_str, "M")
            if ym not in all_months:
                continue
            start = ym.start_time
            end = min(ym.end_time, last)
            result[ym_str] = build_camps_period(df, start, end, all_months)
            print(f"   {ym_str}: {len(result[ym_str])} campanhas")
        except Exception as e:
            print(f"   {ym_str}: erro {e}")

    return result


# ── DADOS MENSAIS ─────────────────────────────────────
def build_monthly(df):
    months = sorted(df["ym"].unique())
    data = {k: [] for k in ["meses", "lbl", "cltS", "fgtsS", "cplG", "cltCPL", "fgtsCPL", "cltL", "fgtsL"]}

    for m in months:
        p = df[df["ym"] == m]
        clt = p[p["product"] == "CLT"]
        fgts = p[p["product"] == "FGTS"]
        cs = round(float(clt["spend"].sum()), 2)
        fs = round(float(fgts["spend"].sum()), 2)
        cl = int(clt["leads"].sum())
        fl = int(fgts["leads"].sum())
        ts = cs + fs
        tl = cl + fl

        data["meses"].append(str(m))
        data["lbl"].append(pd.Period(m, "M").strftime("%b/%y").capitalize())
        data["cltS"].append(cs)
        data["fgtsS"].append(fs)
        data["cplG"].append(round(ts / tl, 2) if tl > 0 else None)
        data["cltCPL"].append(round(cs / cl, 2) if cl > 0 else None)
        data["fgtsCPL"].append(round(fs / fl, 2) if fl > 0 else None)
        data["cltL"].append(cl)
        data["fgtsL"].append(fl)

    return data


# ── DIAS POR MÊS ─────────────────────────────────────
def build_mes_days(df):
    result = {}
    for ym in df["ym"].unique():
        days = sorted(df[df["ym"] == ym]["date"].unique())
        result[str(ym)] = [pd.Timestamp(d).strftime("%d/%m") for d in days]
    return result


# ── CRIATIVOS COM IMAGENS ─────────────────────────────
def build_ads(df, img_dir):
    df_ads = df[
        df["thumb"].notna() &
        (df["thumb"].astype(str) != "") &
        (df["thumb"].astype(str) != "nan")
    ].copy()

    ads_agg = df_ads.groupby(["ad", "product", "thumb"]).agg(
        leads=("leads", "sum"), spend=("spend", "sum")
    ).reset_index().sort_values("leads", ascending=False)

    result = {"CLT": [], "FGTS": []}
    for prod, n_top in [("CLT", 8), ("FGTS", 6)]:
        subset = ads_agg[ads_agg["product"] == prod].drop_duplicates(subset="ad").head(n_top)
        for _, r in subset.iterrows():
            local = download_thumb(str(r["thumb"]), img_dir)
            tL = int(r["leads"])
            tS = float(r["spend"])
            result[prod].append({
                "n": str(r["ad"]),
                "leads": tL,
                "cpl": round(tS / tL, 2) if tL > 0 else None,
                "thumb": local,
            })
    print(f"   CLT: {len(result['CLT'])} criativos | FGTS: {len(result['FGTS'])} criativos")
    return result



# ── LER BREAKDOWNS ────────────────────────────────────
def load_breakdown(url, dim_col, dim_name):
    """Lê uma aba de breakdown e retorna DataFrame limpo."""
    try:
        df = pd.read_csv(url)
        col_map = {
            "Date": "date",
            "Spend (Cost, Amount Spent)": "spend",
            "Action Messaging Conversations Started (Onsite Conversion)": "leads",
            "Impressions": "impressions",
            dim_col: dim_name,
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        for c in ["spend", "leads", "impressions"]:
            if c in df.columns:
                df[c] = pd.to_numeric(
                    df[c].astype(str).str.replace(",", ".", regex=False),
                    errors="coerce"
                ).fillna(0)
        df = df.dropna(subset=["date"])
        return df
    except Exception as e:
        print(f"   Erro ao ler {url}: {e}")
        return pd.DataFrame()


def build_breakdown_period(df, dim_col, start_dt, end_dt, top_n=15):
    """Agrega dados de breakdown para um período."""
    if df.empty:
        return []
    p = df[(df["date"] >= pd.Timestamp(start_dt)) & (df["date"] <= pd.Timestamp(end_dt))]
    if len(p) == 0:
        return []
    
    agg = p.groupby(dim_col).agg(
        spend=("spend", "sum"),
        leads=("leads", "sum"),
        impressions=("impressions", "sum"),
    ).reset_index()
    agg["cpl"] = (agg["spend"] / agg["leads"]).where(agg["leads"] > 0).round(2)
    agg["cpm"] = (agg["spend"] / agg["impressions"] * 1000).where(agg["impressions"] > 0).round(2)
    agg = agg[agg["spend"] > 0].sort_values("leads", ascending=False).head(top_n)
    
    out = []
    for _, r in agg.iterrows():
        out.append({
            "n": str(r[dim_col]),
            "spend": round(float(r["spend"]), 2),
            "leads": int(r["leads"]),
            "impressions": int(r["impressions"]),
            "cpl": float(r["cpl"]) if pd.notna(r["cpl"]) else None,
            "cpm": float(r["cpm"]) if pd.notna(r["cpm"]) else None,
        })
    return out


def build_gender_period(df_ga, start_dt, end_dt):
    """Separa gênero e idade."""
    if df_ga.empty:
        return {"age": [], "gender": []}
    p = df_ga[(df_ga["date"] >= pd.Timestamp(start_dt)) & (df_ga["date"] <= pd.Timestamp(end_dt))]
    
    age_order = ["18-24", "25-34", "35-44", "45-54", "55-64", "65+"]
    
    age_agg = p.groupby("age").agg(spend=("spend","sum"), leads=("leads","sum"), impressions=("impressions","sum")).reset_index()
    age_agg["cpl"] = (age_agg["spend"]/age_agg["leads"]).where(age_agg["leads"]>0).round(2)
    age_agg["cpm"] = (age_agg["spend"]/age_agg["impressions"]*1000).where(age_agg["impressions"]>0).round(2)
    age_agg = age_agg[age_agg["spend"] > 0]
    age_agg["_order"] = age_agg["age"].apply(lambda x: age_order.index(x) if x in age_order else 99)
    age_agg = age_agg.sort_values("_order")
    
    gen_agg = p.groupby("gender").agg(spend=("spend","sum"), leads=("leads","sum"), impressions=("impressions","sum")).reset_index()
    gen_agg["cpl"] = (gen_agg["spend"]/gen_agg["leads"]).where(gen_agg["leads"]>0).round(2)
    gen_agg["cpm"] = (gen_agg["spend"]/gen_agg["impressions"]*1000).where(gen_agg["impressions"]>0).round(2)
    gen_agg = gen_agg[gen_agg["spend"] > 0].sort_values("leads", ascending=False)
    
    def to_list(df, dim):
        out = []
        for _, r in df.iterrows():
            out.append({
                "n": str(r[dim]),
                "spend": round(float(r["spend"]), 2),
                "leads": int(r["leads"]),
                "impressions": int(r["impressions"]),
                "cpl": float(r["cpl"]) if pd.notna(r["cpl"]) else None,
                "cpm": float(r["cpm"]) if pd.notna(r["cpm"]) else None,
            })
        return out
    
    return {"age": to_list(age_agg, "age"), "gender": to_list(gen_agg, "gender")}


def build_breakdowns(df_ga, df_pt, all_days):
    """Gera todos os breakdowns para todos os períodos."""
    last = pd.Timestamp(all_days[-1])
    all_months_ga = sorted(df_ga["ym"].unique()) if not df_ga.empty and "ym" in df_ga.columns else []
    result = {}

    for n in [1, 7, 14, 30]:
        start = last - pd.Timedelta(days=n - 1)
        gd = build_gender_period(df_ga, start, last)
        pt = build_breakdown_period(df_pt, "platform", start, last, top_n=15)
        result[str(n)] = {"age": gd["age"], "gender": gd["gender"], "platform": pt}
        print(f"   {n}d: idade={len(gd['age'])} gênero={len(gd['gender'])} região={len(rg)} plataforma={len(pt)}")

    for ym_str in ["2026-04", "2026-03", "2026-02", "2026-01", "2025-12", "2025-11", "2025-10", "2025-09"]:
        try:
            ym = pd.Period(ym_str, "M")
            start = ym.start_time
            end = min(ym.end_time, last)
            gd = build_gender_period(df_ga, start, end)
            pt = build_breakdown_period(df_pt, "platform", start, end, top_n=15)
            result[ym_str] = {"age": gd["age"], "gender": gd["gender"], "platform": pt}
        except Exception as e:
            print(f"   {ym_str}: erro {e}")

    return result


# ── INJETAR NO HTML ───────────────────────────────────
def inject_data(template_path, daily, last_day, monthly, camps, mes_days, kpis, ads_data, breakdown_data):
    html = Path(template_path).read_text(encoding="utf-8")

    def replace_js_const(html, const_name, value):
        pattern = rf"(const {const_name}\s*=\s*)({{[\s\S]*?}}|\"[^\"]*\"|\[[^\]]*\]);"
        replacement = f"const {const_name} = {json.dumps(value, ensure_ascii=False)};"
        new_html, count = re.subn(pattern, replacement, html, count=1)
        if count == 0:
            print(f"  AVISO: nao encontrou: {const_name}")
        return new_html

    html = replace_js_const(html, "DAILY", daily)
    html = replace_js_const(html, "MONTHLY", monthly)
    html = replace_js_const(html, "CAMPS_MES", camps)
    html = replace_js_const(html, "MES_DAYS", mes_days)
    html = replace_js_const(html, "KPIS_PERIODO", kpis)
    html = replace_js_const(html, "ADS_DATA", ads_data)
    html = replace_js_const(html, "BREAKDOWN_DATA", breakdown_data)

    html = re.sub(r"Dados até \d{2}/\d{2}", f"Dados até {last_day}", html)
    today_str = date.today().strftime("%d/%m/%Y")
    html = re.sub(r"\d{2}/\d{2}/\d{4} · via planilha", f"{today_str} · via planilha", html)

    return html


# ── MAIN ──────────────────────────────────────────────
def main():
    print("=" * 50)
    print("CREDI Dashboard — Gerador automatico")
    print("=" * 50)

    df = load_sheet()

    print("Dados diarios...")
    daily, last_day, all_days = build_daily(df)
    print(f"   {len(daily['days'])} dias | ultimo: {last_day}")

    print("KPIs por periodo...")
    kpis = build_kpis(df, all_days)

    print("Dados mensais...")
    monthly = build_monthly(df)
    print(f"   {len(monthly['meses'])} meses")

    print("Campanhas por periodo...")
    camps = build_camps(df, all_days)

    print("Dias por mes...")
    mes_days = build_mes_days(df)

    print("Imagens dos criativos...")
    img_dir = Path("imgs")
    img_dir.mkdir(exist_ok=True)
    ads_data = build_ads(df, img_dir, all_days)

    print("Carregando breakdowns...")
    df_ga_raw = pd.read_csv(SHEET_URL_GA)
    df_ga_raw["date"] = pd.to_datetime(df_ga_raw["Date"], errors="coerce")
    df_ga_raw["spend"] = pd.to_numeric(df_ga_raw["Spend (Cost, Amount Spent)"].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
    df_ga_raw["leads"] = pd.to_numeric(df_ga_raw["Action Messaging Conversations Started (Onsite Conversion)"].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
    df_ga_raw["impressions"] = pd.to_numeric(df_ga_raw["Impressions"].astype(str).str.replace(",", "."), errors="coerce").fillna(0)
    df_ga_raw["age"] = df_ga_raw["Age (Breakdown)"]
    df_ga_raw["gender"] = df_ga_raw["Gender (Breakdown)"]
    df_ga_raw["ym"] = df_ga_raw["date"].dt.to_period("M")
    df_ga = df_ga_raw.dropna(subset=["date"])

    df_pt = load_breakdown(SHEET_URL_PT, "Platform Position (Breakdown)", "platform")

    print("Gerando breakdowns por periodo...")
    breakdown_data = build_breakdowns(df_ga, df_pt, all_days)

    print("Gerando HTML...")
    if not Path(TEMPLATE_FILE).exists():
        print(f"Template nao encontrado: {TEMPLATE_FILE}")
        return

    html = inject_data(TEMPLATE_FILE, daily, last_day, monthly, camps, mes_days, kpis, ads_data, breakdown_data)
    Path(OUTPUT_FILE).write_text(html, encoding="utf-8")
    print(f"Dashboard gerado: {OUTPUT_FILE} ({len(html)//1024}KB)")
    print("=" * 50)


if __name__ == "__main__":
    main()
