#!/usr/bin/env python3
"""
CREDI — Gerador automático do Dashboard Meta Ads
Lê a planilha do Google Sheets (Stract) e gera o HTML atualizado.
"""

import pandas as pd
import json
import re
from datetime import datetime, date
from pathlib import Path

# ── CONFIG ────────────────────────────────────────────
SHEET_ID = "1L3bUusX8nwynFcBxWW_9l-ouLVwOEy5IMuA8w5CI7xA"
SHEET_TAB = "meta-ads"
OUTPUT_FILE = "index.html"
TEMPLATE_FILE = "template_base.html"

# URL pública CSV do Google Sheets
SHEET_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_TAB}"

# ── LER PLANILHA ──────────────────────────────────────
def load_sheet():
    print(f"📥 Lendo planilha: {SHEET_URL}")
    df = pd.read_csv(SHEET_URL)

    # Normalizar nomes de colunas
    col_map = {
        'Date': 'date',
        'Campaign Name': 'campaign',
        'Adset Name': 'adset',
        'Ad Name': 'ad',
        'Thumbnail URL': 'thumb',
        'Spend (Cost, Amount Spent)': 'spend',
        'Impressions': 'impressions',
        'Clicks': 'clicks',
        'Action Messaging Conversations Started (Onsite Conversion)': 'leads',
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # Tipos
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    for c in ['spend', 'leads', 'impressions', 'clicks']:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

    # Produto
    df['product'] = df['campaign'].apply(lambda c: 'FGTS' if 'FGTS' in str(c).upper() else 'CLT')
    df['ym'] = df['date'].dt.to_period('M')

    # Remover linhas sem data
    df = df.dropna(subset=['date'])
    print(f"✅ {len(df)} linhas carregadas | {df['date'].min().date()} → {df['date'].max().date()}")
    return df


# ── DADOS DIÁRIOS ─────────────────────────────────────
def build_daily(df):
    daily = df.groupby('date').agg(
        spend=('spend', 'sum'), leads=('leads', 'sum'),
        impressions=('impressions', 'sum'), clicks=('clicks', 'sum')
    ).reset_index().sort_values('date')

    daily_clt = df[df['product'] == 'CLT'].groupby('date').agg(
        spend=('spend', 'sum'), leads=('leads', 'sum')).reset_index()
    daily_fgts = df[df['product'] == 'FGTS'].groupby('date').agg(
        spend=('spend', 'sum'), leads=('leads', 'sum')).reset_index()

    # Últimos 60 dias com dados
    all_days = sorted(daily['date'].unique())[-60:]

    out = {k: [] for k in ['days','spend','leads','cpl','ctr','cpm',
                             'cltL','fgtsL','cltS','fgtsS','cltCPL','fgtsCPL',
                             'cltCTR','fgtsCTR']}

    for d in all_days:
        r = daily[daily['date'] == d].iloc[0]
        cr = daily_clt[daily_clt['date'] == d]
        fr = daily_fgts[daily_fgts['date'] == d]

        cl = int(cr['leads'].sum()) if len(cr) else 0
        fl = int(fr['leads'].sum()) if len(fr) else 0
        cs = round(float(cr['spend'].sum()), 2) if len(cr) else 0
        fs = round(float(fr['spend'].sum()), 2) if len(fr) else 0

        tl = int(r['leads']); ts = float(r['spend'])
        imp = float(r['impressions']); clk = float(r['clicks'])

        out['days'].append(pd.Timestamp(d).strftime('%d/%m'))
        out['spend'].append(round(ts, 2))
        out['leads'].append(tl)
        out['cpl'].append(round(ts/tl, 2) if tl > 0 else None)
        out['ctr'].append(round(clk/imp*100, 2) if imp > 0 else None)
        out['cpm'].append(round(ts/imp*1000, 2) if imp > 0 else None)
        out['cltL'].append(cl)
        out['fgtsL'].append(fl)
        out['cltS'].append(cs)
        out['fgtsS'].append(fs)
        out['cltCPL'].append(round(cs/cl, 2) if cl > 0 else None)
        out['fgtsCPL'].append(round(fs/fl, 2) if fl > 0 else None)

        # CTR por produto (approx: proporcional aos leads)
        cltCTR = round(clk/imp*100*(cl/tl if tl else 1), 2) if imp > 0 and tl > 0 else None
        fgtsCTR = round(clk/imp*100*(fl/tl if tl else 0), 2) if imp > 0 and tl > 0 and fl > 0 else None
        out['cltCTR'].append(cltCTR)
        out['fgtsCTR'].append(fgtsCTR)

    last_day = out['days'][-1] if out['days'] else '—'
    return out, last_day


# ── DADOS MENSAIS ─────────────────────────────────────
def build_monthly(df):
    months = sorted(df['ym'].unique())
    data = {k: [] for k in ['meses','lbl','cltS','fgtsS','cplG','cltCPL','fgtsCPL','cltL','fgtsL']}

    for m in months:
        p = df[df['ym'] == m]
        clt = p[p['product'] == 'CLT']
        fgts = p[p['product'] == 'FGTS']
        cs = round(float(clt['spend'].sum()), 2)
        fs = round(float(fgts['spend'].sum()), 2)
        cl = int(clt['leads'].sum())
        fl = int(fgts['leads'].sum())
        ts = cs + fs; tl = cl + fl

        data['meses'].append(str(m))
        data['lbl'].append(pd.Period(m, 'M').strftime('%b/%y').capitalize())
        data['cltS'].append(cs)
        data['fgtsS'].append(fs)
        data['cplG'].append(round(ts/tl, 2) if tl > 0 else None)
        data['cltCPL'].append(round(cs/cl, 2) if cl > 0 else None)
        data['fgtsCPL'].append(round(fs/fl, 2) if fl > 0 else None)
        data['cltL'].append(cl)
        data['fgtsL'].append(fl)

    return data


# ── CAMPANHAS POR MÊS ─────────────────────────────────
def build_camps(df):
    all_months = sorted(df['ym'].unique())
    # Últimos 6 meses com dados
    target_months = all_months[-6:]
    result = {}

    for ym in target_months:
        p = df[df['ym'] == ym]
        camps = p.groupby(['campaign', 'product']).agg(
            spend=('spend','sum'), leads=('leads','sum'),
            impressions=('impressions','sum'), clicks=('clicks','sum')
        ).reset_index()
        camps['cpl'] = (camps['spend']/camps['leads']).where(camps['leads']>0).round(2)
        camps['cpm'] = (camps['spend']/camps['impressions']*1000).where(camps['impressions']>0).round(2)
        camps['ctr'] = (camps['clicks']/camps['impressions']*100).where(camps['impressions']>0).round(2)
        camps = camps.sort_values('leads', ascending=False).head(12)

        # Sparkline: CPL dos últimos 6 meses para cada campanha
        cur_idx = list(all_months).index(ym)
        spk_months = all_months[max(0, cur_idx-5):cur_idx+1]

        out = []
        for _, r in camps.iterrows():
            adsets = p[p['campaign'] == r['campaign']].groupby('adset').agg(
                spend=('spend','sum'), leads=('leads','sum'),
                impressions=('impressions','sum'), clicks=('clicks','sum')
            ).reset_index()
            adsets['cpl'] = (adsets['spend']/adsets['leads']).where(adsets['leads']>0).round(2)
            adsets['cpm'] = (adsets['spend']/adsets['impressions']*1000).where(adsets['impressions']>0).round(2)
            adsets['ctr'] = (adsets['clicks']/adsets['impressions']*100).where(adsets['impressions']>0).round(2)
            adsets = adsets.sort_values('leads', ascending=False)

            # Sparkline
            spk = []
            for sm in spk_months:
                cm = df[(df['ym']==sm) & (df['campaign']==r['campaign'])]
                ts2 = float(cm['spend'].sum()); tl2 = float(cm['leads'].sum())
                spk.append(round(ts2/tl2, 2) if tl2 > 0 else None)

            conjs = []
            for _, a in adsets.iterrows():
                conjs.append({
                    'n': str(a['adset']),
                    'spend': round(float(a['spend']), 2),
                    'leads': int(a['leads']),
                    'cpl': float(a['cpl']) if pd.notna(a['cpl']) else None,
                    'cpm': float(a['cpm']) if pd.notna(a['cpm']) else None,
                    'ctr': float(a['ctr']) if pd.notna(a['ctr']) else None,
                })

            out.append({
                'n': str(r['campaign']),
                'product': str(r['product']),
                'spend': round(float(r['spend']), 2),
                'leads': int(r['leads']),
                'cpl': float(r['cpl']) if pd.notna(r['cpl']) else None,
                'cpm': float(r['cpm']) if pd.notna(r['cpm']) else None,
                'ctr': float(r['ctr']) if pd.notna(r['ctr']) else None,
                'spk': spk,
                'conjs': conjs,
            })
        result[str(ym)] = out

    return result


# ── DADOS DOS DIAS POR MÊS (para filtro mensal) ───────
def build_mes_days(df):
    result = {}
    for ym in df['ym'].unique():
        days = sorted(df[df['ym']==ym]['date'].unique())
        result[str(ym)] = [pd.Timestamp(d).strftime('%d/%m') for d in days]
    return result


# ── INJETAR NO HTML ───────────────────────────────────
def inject_data(template_path, daily, last_day, monthly, camps, mes_days):
    html = Path(template_path).read_text(encoding='utf-8')

    # Substitui os blocos de dados JS no template
    def replace_js_const(html, const_name, value):
        pattern = rf'(const {const_name}\s*=\s*)({{[\s\S]*?}}|"[^"]*"|\[[^\]]*\]);'
        replacement = f'const {const_name} = {json.dumps(value, ensure_ascii=False)};'
        new_html, count = re.subn(pattern, replacement, html, count=1)
        if count == 0:
            print(f"  ⚠️  Não encontrou: {const_name}")
        return new_html

    html = replace_js_const(html, 'DAILY', daily)
    html = replace_js_const(html, 'MONTHLY', monthly)
    html = replace_js_const(html, 'CAMPS_MES', camps)
    html = replace_js_const(html, 'MES_DAYS', mes_days)

    # Atualiza "Dados até XX/XX"
    html = re.sub(
        r'Dados até \d{2}/\d{2}',
        f'Dados até {last_day}',
        html
    )

    # Atualiza rodapé de última atualização
    today_str = date.today().strftime('%d/%m/%Y')
    html = re.sub(
        r'\d{2}/\d{2}/\d{4} · via planilha',
        f'{today_str} · via planilha',
        html
    )

    return html


# ── MAIN ──────────────────────────────────────────────
def main():
    print("=" * 50)
    print("CREDI Dashboard — Gerador automático")
    print("=" * 50)

    df = load_sheet()

    print("🔧 Processando dados diários...")
    daily, last_day = build_daily(df)
    print(f"   {len(daily['days'])} dias | último: {last_day}")

    print("🔧 Processando dados mensais...")
    monthly = build_monthly(df)
    print(f"   {len(monthly['meses'])} meses")

    print("🔧 Processando campanhas...")
    camps = build_camps(df)
    print(f"   {len(camps)} meses de campanhas")

    print("🔧 Mapeando dias por mês...")
    mes_days = build_mes_days(df)

    print("📝 Gerando HTML...")
    if not Path(TEMPLATE_FILE).exists():
        print(f"❌ Template não encontrado: {TEMPLATE_FILE}")
        return

    html = inject_data(TEMPLATE_FILE, daily, last_day, monthly, camps, mes_days)
    Path(OUTPUT_FILE).write_text(html, encoding='utf-8')
    print(f"✅ Dashboard gerado: {OUTPUT_FILE} ({len(html)//1024}KB)")
    print("=" * 50)


if __name__ == '__main__':
    main()
