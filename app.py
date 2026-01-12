import io
import os
import re
import hashlib
from typing import Dict, List

import numpy as np
import pandas as pd
import streamlit as st
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter


# =========================
# Helpers: cleaning & parsing
# =========================

RE_NON_NUM = re.compile(r"[^0-9\-,.]+")

def parse_number(x):
    """Robust numeric parser for strings like 'Rp 1.234,56' or '12345.67'."""
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return np.nan
    s = RE_NON_NUM.sub("", s)

    # Indonesian format: 1.234,56 -> 1234.56
    if s.count(",") == 1 and s.count(".") >= 1:
        s = s.replace(".", "").replace(",", ".")
    elif s.count(",") == 1 and s.count(".") == 0:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return np.nan


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    # Common typos
    rename_map = {
        "Nama Barange": "Nama Barang",
        "Kampanye Partnerr": "Kampanye Partner",
        "Status Pemebelian": "Status Pembelian",
    }
    for k, v in rename_map.items():
        if k in df.columns and v not in df.columns:
            df = df.rename(columns={k: v})
    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Datetime columns (if exist)
    for col in ["Waktu Pemesanan", "Waktu Terselesaikan", "Waktu Klik"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "Waktu Pemesanan" in df.columns:
        df["Tanggal"] = df["Waktu Pemesanan"].dt.date

    # Numeric columns (if exist)
    num_cols = [
        "Harga(Rp)",
        "Jumlah",
        "Nilai Pembelian(Rp)",
        "Jumlah Pengembalian Dana(Rp)",
        "Komisi Bersih Affiliate (Rp)",
        "Total Komisi per Pesanan(Rp)",
        "Total Komisi per Produk(Rp)",
        "Komisi Shopee per Pesanan(Rp)",
        "Komisi XTRA per Pesanan(Rp)",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = df[c].apply(parse_number)

    if "Jumlah" in df.columns:
        df["Jumlah"] = df["Jumlah"].fillna(0).astype(float)

    # Status flags
    if "Status Pesanan" in df.columns:
        df["Status Pesanan"] = df["Status Pesanan"].astype(str).str.strip()
        df["is_pending"] = df["Status Pesanan"].str.lower().eq("tertunda")
        df["is_completed"] = df["Status Pesanan"].str.lower().isin(["selesai", "dibayarkan", "completed"])
    else:
        df["is_pending"] = False
        df["is_completed"] = False

    return df


def read_csv_bytes(raw: bytes) -> pd.DataFrame:
    """Try encodings: utf-8-sig, utf-8, latin1."""
    for enc in ["utf-8-sig", "utf-8", "latin1"]:
        try:
            return pd.read_csv(io.BytesIO(raw), encoding=enc)
        except Exception:
            continue
    # last resort
    return pd.read_csv(io.BytesIO(raw), encoding_errors="ignore")


def file_md5(raw: bytes) -> str:
    return hashlib.md5(raw).hexdigest()


@st.cache_data(show_spinner=False)
def parse_one_file(raw: bytes, filename: str) -> pd.DataFrame:
    df = read_csv_bytes(raw)
    df = normalize_columns(df)
    df = add_derived_columns(df)
    akun = os.path.splitext(os.path.basename(filename))[0]
    df["Akun"] = akun
    return df


@st.cache_data(show_spinner=False)
def parse_many(files_payload: List[dict]) -> pd.DataFrame:
    frames = []
    for item in files_payload:
        raw = item["raw"]
        name = item["name"]
        df = parse_one_file(raw, name)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# =========================
# Core analytics
# =========================

def overview_table(df: pd.DataFrame) -> pd.DataFrame:
    date_min = df["Tanggal"].min() if "Tanggal" in df.columns else None
    date_max = df["Tanggal"].max() if "Tanggal" in df.columns else None

    order_id_col = "ID Pemesanan" if "ID Pemesanan" in df.columns else None
    unique_orders = df[order_id_col].nunique(dropna=True) if order_id_col else len(df)

    total_gmv = df["Nilai Pembelian(Rp)"].sum() if "Nilai Pembelian(Rp)" in df.columns else np.nan
    total_net_comm = df["Komisi Bersih Affiliate (Rp)"].sum() if "Komisi Bersih Affiliate (Rp)" in df.columns else np.nan
    pending_cnt = int(df["is_pending"].sum()) if "is_pending" in df.columns else 0

    out = [
        ("Periode", f"{date_min} s/d {date_max}"),
        ("Jumlah baris", int(len(df))),
        ("Pesanan unik", int(unique_orders)),
        ("Total Nilai Pembelian (GMV)", float(total_gmv)),
        ("Total Komisi Bersih Affiliate", float(total_net_comm)),
        ("Jumlah baris status Tertunda", pending_cnt),
    ]
    return pd.DataFrame(out, columns=["Metric", "Value"])


def daily_totals(df: pd.DataFrame, metric_col: str) -> pd.DataFrame:
    if "Tanggal" not in df.columns:
        return pd.DataFrame()

    order_id_col = "ID Pemesanan" if "ID Pemesanan" in df.columns else None
    g = df.groupby("Tanggal", dropna=False)

    res = pd.DataFrame({
        "Orders (unik)": g[order_id_col].nunique() if order_id_col else g.size(),
        "Items (Jumlah)": g["Jumlah"].sum() if "Jumlah" in df.columns else g.size(),
        "GMV (Rp)": g["Nilai Pembelian(Rp)"].sum() if "Nilai Pembelian(Rp)" in df.columns else np.nan,
        "Komisi Bersih (Rp)": g["Komisi Bersih Affiliate (Rp)"].sum() if "Komisi Bersih Affiliate (Rp)" in df.columns else np.nan,
    }).reset_index()

    res["AOV (Rp)"] = np.where(res["Orders (unik)"] > 0, res["GMV (Rp)"] / res["Orders (unik)"], np.nan)
    res["Komisi / Order (Rp)"] = np.where(res["Orders (unik)"] > 0, res["Komisi Bersih (Rp)"] / res["Orders (unik)"], np.nan)

    # For chart convenience
    res["Metric (Rp)"] = res[metric_col]
    return res.sort_values("Tanggal")


def summary_by_category(df: pd.DataFrame, level_col: str, metric_col: str) -> pd.DataFrame:
    if level_col not in df.columns:
        return pd.DataFrame()

    order_id_col = "ID Pemesanan" if "ID Pemesanan" in df.columns else None
    g = df.groupby(level_col, dropna=False)

    res = pd.DataFrame({
        "Orders (unik)": g[order_id_col].nunique() if order_id_col else g.size(),
        "GMV (Rp)": g["Nilai Pembelian(Rp)"].sum() if "Nilai Pembelian(Rp)" in df.columns else np.nan,
        "Komisi Bersih (Rp)": g["Komisi Bersih Affiliate (Rp)"].sum() if "Komisi Bersih Affiliate (Rp)" in df.columns else np.nan,
        "Items (Jumlah)": g["Jumlah"].sum() if "Jumlah" in df.columns else g.size(),
    }).reset_index().rename(columns={level_col: "Kategori"})

    total_metric = res[metric_col].sum()
    res["Share"] = np.where(total_metric > 0, res[metric_col] / total_metric, np.nan)

    return res.sort_values(metric_col, ascending=False)


def top_products(df: pd.DataFrame, metric_col: str, top_n: int = 30) -> pd.DataFrame:
    if "ID Barang" not in df.columns:
        return pd.DataFrame()

    name_col = "Nama Barang" if "Nama Barang" in df.columns else None
    keys = ["ID Barang"] + ([name_col] if name_col else [])
    order_id_col = "ID Pemesanan" if "ID Pemesanan" in df.columns else None

    g = df.groupby(keys, dropna=False)

    res = pd.DataFrame({
        "Orders (unik)": g[order_id_col].nunique() if order_id_col else g.size(),
        "GMV (Rp)": g["Nilai Pembelian(Rp)"].sum() if "Nilai Pembelian(Rp)" in df.columns else np.nan,
        "Komisi Bersih (Rp)": g["Komisi Bersih Affiliate (Rp)"].sum() if "Komisi Bersih Affiliate (Rp)" in df.columns else np.nan,
        "Items (Jumlah)": g["Jumlah"].sum() if "Jumlah" in df.columns else g.size(),
    }).reset_index()

    return res.sort_values(metric_col, ascending=False).head(top_n)


def top_stores(df: pd.DataFrame, metric_col: str, top_n: int = 30) -> pd.DataFrame:
    if "Nama Toko" not in df.columns:
        return pd.DataFrame()

    order_id_col = "ID Pemesanan" if "ID Pemesanan" in df.columns else None
    g = df.groupby("Nama Toko", dropna=False)

    res = pd.DataFrame({
        "Orders (unik)": g[order_id_col].nunique() if order_id_col else g.size(),
        "GMV (Rp)": g["Nilai Pembelian(Rp)"].sum() if "Nilai Pembelian(Rp)" in df.columns else np.nan,
        "Komisi Bersih (Rp)": g["Komisi Bersih Affiliate (Rp)"].sum() if "Komisi Bersih Affiliate (Rp)" in df.columns else np.nan,
        "Items (Jumlah)": g["Jumlah"].sum() if "Jumlah" in df.columns else g.size(),
    }).reset_index()

    return res.sort_values(metric_col, ascending=False).head(top_n)


def daily_top_k(df: pd.DataFrame, group_col: str, metric_source_col: str, top_k: int) -> pd.DataFrame:
    """Per tanggal, ambil Top-K berdasarkan metric_source_col."""
    if "Tanggal" not in df.columns or group_col not in df.columns or metric_source_col not in df.columns:
        return pd.DataFrame()

    g = df.groupby(["Tanggal", group_col], dropna=False)[metric_source_col].sum().reset_index()
    g = g.rename(columns={metric_source_col: "Value"})
    g["Rank"] = g.groupby("Tanggal")["Value"].rank(method="first", ascending=False)

    out = g[g["Rank"] <= top_k].copy()
    out = out.sort_values(["Tanggal", "Rank"]).rename(columns={group_col: "Item"})
    return out


def winning_category_per_day(df: pd.DataFrame, l1_col: str, metric_source_col: str) -> pd.DataFrame:
    """Per tanggal: kategori L1 pemenang (metric terbesar)."""
    if "Tanggal" not in df.columns or l1_col not in df.columns or metric_source_col not in df.columns:
        return pd.DataFrame()

    g = df.groupby(["Tanggal", l1_col], dropna=False)[metric_source_col].sum().reset_index()
    g = g.sort_values(["Tanggal", metric_source_col], ascending=[True, False])

    winner = g.groupby("Tanggal").head(1).copy()
    winner = winner.rename(columns={l1_col: "Winning L1", metric_source_col: "Metric (Rp)"})

    total_day = df.groupby("Tanggal")[metric_source_col].sum().reset_index().rename(columns={metric_source_col: "Total Metric Hari Itu (Rp)"})
    winner = winner.merge(total_day, on="Tanggal", how="left")
    winner["Share"] = np.where(winner["Total Metric Hari Itu (Rp)"] > 0, winner["Metric (Rp)"] / winner["Total Metric Hari Itu (Rp)"], np.nan)
    return winner.sort_values("Tanggal")


# =========================
# Excel export formatting
# =========================

def autosize_columns(ws, max_width: int = 55):
    for col in ws.columns:
        col_letter = get_column_letter(col[0].column)
        max_len = 0
        for cell in col:
            try:
                v = "" if cell.value is None else str(cell.value)
                max_len = max(max_len, len(v))
            except Exception:
                pass
        ws.column_dimensions[col_letter].width = min(max_width, max(10, max_len + 2))


def style_header(ws, header_row: int = 1):
    fill = PatternFill("solid", fgColor="1F4E79")
    font = Font(color="FFFFFF", bold=True)
    align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    for cell in ws[header_row]:
        cell.fill = fill
        cell.font = font
        cell.alignment = align
    ws.freeze_panes = ws["A2"]


def apply_number_formats(ws, currency_cols=None, percent_cols=None, date_cols=None, int_cols=None):
    currency_cols = currency_cols or []
    percent_cols = percent_cols or []
    date_cols = date_cols or []
    int_cols = int_cols or []

    header = [c.value for c in ws[1]]
    col_index = {name: i + 1 for i, name in enumerate(header) if name is not None}

    for name in currency_cols:
        if name in col_index:
            idx = col_index[name]
            for r in range(2, ws.max_row + 1):
                ws.cell(r, idx).number_format = '"Rp" #,##0.00'

    for name in percent_cols:
        if name in col_index:
            idx = col_index[name]
            for r in range(2, ws.max_row + 1):
                ws.cell(r, idx).number_format = "0.0%"

    for name in date_cols:
        if name in col_index:
            idx = col_index[name]
            for r in range(2, ws.max_row + 1):
                ws.cell(r, idx).number_format = "yyyy-mm-dd"

    for name in int_cols:
        if name in col_index:
            idx = col_index[name]
            for r in range(2, ws.max_row + 1):
                ws.cell(r, idx).number_format = "#,##0"


def export_excel_bytes(tables: Dict[str, pd.DataFrame]) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        for sheet, table in tables.items():
            safe_sheet = sheet[:31]  # Excel limit
            table.to_excel(writer, sheet_name=safe_sheet, index=False)

    buf.seek(0)
    wb = load_workbook(buf)

    for sheet in wb.sheetnames:
        ws = wb[sheet]
        style_header(ws, 1)
        autosize_columns(ws)

        # Format by sheet
        if sheet in ["Daily Totals", "Winning L1 Daily", "Daily Top L1 Top3", "Daily Top Products Top5"]:
            apply_number_formats(
                ws,
                currency_cols=["GMV (Rp)", "Komisi Bersih (Rp)", "AOV (Rp)", "Komisi / Order (Rp)", "Metric (Rp)", "Value", "Total Metric Hari Itu (Rp)"],
                percent_cols=["Share"],
                date_cols=["Tanggal"],
                int_cols=["Orders (unik)", "Items (Jumlah)", "Rank"]
            )

        if sheet in ["Summary L1", "Summary L2", "Top Products", "Top Stores"]:
            apply_number_formats(
                ws,
                currency_cols=["GMV (Rp)", "Komisi Bersih (Rp)"],
                percent_cols=["Share"],
                date_cols=["Tanggal"],
                int_cols=["Orders (unik)", "Items (Jumlah)"]
            )

    out = io.BytesIO()
    wb.save(out)
    return out.getvalue()


# =========================
# Streamlit UI
# =========================

st.set_page_config(page_title="Affiliate Analyzer", layout="wide")

st.title("📊 Affiliate Winning Analyzer (CSV → Insight + Excel)")

with st.sidebar:
    st.header("Upload & Settings")
    uploaded = st.file_uploader("Upload CSV (bisa banyak akun)", type=["csv"], accept_multiple_files=True)

    metric_choice = st.selectbox(
        "Winning berdasarkan apa?",
        ["Komisi Bersih (Rp)", "GMV (Rp)"],
        index=0
    )
    metric_source_col = "Komisi Bersih Affiliate (Rp)" if metric_choice == "Komisi Bersih (Rp)" else "Nilai Pembelian(Rp)"

    top_products_n = st.slider("Top Products (overall)", 5, 100, 30, 5)
    top_stores_n = st.slider("Top Stores (overall)", 5, 100, 30, 5)

    st.divider()
    only_pending = st.checkbox("Filter hanya Status Pesanan = Tertunda", value=False)
    only_completed = st.checkbox("Filter hanya pesanan selesai/dibayarkan (flag)", value=False)

if not uploaded:
    st.info("Upload satu atau beberapa CSV dulu ya.")
    st.stop()

# Prepare payload for caching
files_payload = []
for f in uploaded:
    raw = f.getvalue()
    files_payload.append({"name": f.name, "raw": raw, "md5": file_md5(raw)})

df = parse_many(files_payload)

# Basic validation
if df.empty:
    st.error("Data kosong / gagal dibaca.")
    st.stop()

# Filters
work = df.copy()
if only_pending and "is_pending" in work.columns:
    work = work[work["is_pending"] == True]
if only_completed and "is_completed" in work.columns:
    work = work[work["is_completed"] == True]

# Date range filter
if "Tanggal" in work.columns and work["Tanggal"].notna().any():
    dmin = pd.to_datetime(work["Tanggal"].min())
    dmax = pd.to_datetime(work["Tanggal"].max())
    colA, colB, colC = st.columns([1, 1, 2])
    with colA:
        start = st.date_input("Tanggal mulai", value=dmin.date(), min_value=dmin.date(), max_value=dmax.date())
    with colB:
        end = st.date_input("Tanggal akhir", value=dmax.date(), min_value=dmin.date(), max_value=dmax.date())
    work = work[(pd.to_datetime(work["Tanggal"]) >= pd.to_datetime(start)) & (pd.to_datetime(work["Tanggal"]) <= pd.to_datetime(end))]

# Build tables
tables: Dict[str, pd.DataFrame] = {}

tables["Overview"] = overview_table(work)

daily = daily_totals(work, metric_col=metric_choice)
tables["Daily Totals"] = daily

tables["Summary L1"] = summary_by_category(work, "L1 Kategori Global", metric_col=metric_choice)
tables["Summary L2"] = summary_by_category(work, "L2 Kategori Global", metric_col=metric_choice)

tables["Top Products"] = top_products(work, metric_col=metric_choice, top_n=top_products_n)
tables["Top Stores"] = top_stores(work, metric_col=metric_choice, top_n=top_stores_n)

tables["Winning L1 Daily"] = winning_category_per_day(work, l1_col="L1 Kategori Global", metric_source_col=metric_source_col)
tables["Daily Top L1 Top3"] = daily_top_k(work, group_col="L1 Kategori Global", metric_source_col=metric_source_col, top_k=3)

prod_col = "Nama Barang" if "Nama Barang" in work.columns else ("ID Barang" if "ID Barang" in work.columns else None)
if prod_col:
    tables["Daily Top Products Top5"] = daily_top_k(work, group_col=prod_col, metric_source_col=metric_source_col, top_k=5)

# Layout
tab1, tab2, tab3 = st.tabs(["📌 Overview", "📈 Harian", "🏆 Winning & Detail"])

with tab1:
    st.subheader("Overview")
    st.dataframe(tables["Overview"], use_container_width=True)

    st.caption(f"Rows: {len(work):,} | Akun unik: {work['Akun'].nunique() if 'Akun' in work.columns else 1}")

with tab2:
    st.subheader("Daily Totals")
    st.dataframe(tables["Daily Totals"], use_container_width=True)

    if not daily.empty:
        chart_df = daily.set_index("Tanggal")[["GMV (Rp)", "Komisi Bersih (Rp)"]].copy()
        st.line_chart(chart_df)

with tab3:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Summary L1 (Overall)")
        st.dataframe(tables["Summary L1"].head(30), use_container_width=True)
    with c2:
        st.subheader("Winning L1 per Hari")
        st.dataframe(tables["Winning L1 Daily"], use_container_width=True)

    st.subheader("Top Products / Stores")
    c3, c4 = st.columns(2)
    with c3:
        st.dataframe(tables["Top Products"], use_container_width=True)
    with c4:
        st.dataframe(tables["Top Stores"], use_container_width=True)

    st.subheader("Daily Winners")
    c5, c6 = st.columns(2)
    with c5:
        st.dataframe(tables["Daily Top L1 Top3"], use_container_width=True)
    with c6:
        if "Daily Top Products Top5" in tables:
            st.dataframe(tables["Daily Top Products Top5"], use_container_width=True)

# Download excel
st.divider()
st.subheader("⬇️ Export Report")
excel_bytes = export_excel_bytes(tables)
st.download_button(
    "Download Excel Report",
    data=excel_bytes,
    file_name="affiliate_report.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
st.caption("Tip: kalau file besar, pakai filter tanggal / status dulu supaya lebih ringan.")
