# utils_ops.py — utilidades de datos y métricas (Plan vs Real)
from __future__ import annotations
import pandas as pd
import numpy as np
from io import BytesIO
from pathlib import Path
import re
from pandas.api.types import is_numeric_dtype

# ==========================
# 1) Mapeo y normalización (Plan y Real)
# ==========================
EXPECTED_PLAN = {
    "Fecha": ["fecha", "date", "dia", "día"],
    "Hora":  ["hora", "time"],
    "Base":  ["base", "sede", "plataforma"],
    "Moviles_Planificados":  ["moviles_planificados","moviles_plan","mov_plan","mov_planif","mov req","mov requeridos"],
    "Servicios_Planificados":["servicios_planificados","serv_plan","svc_plan","llamadas_plan","svc proy","servicios proy"],
}
EXPECTED_REAL = {
    "Fecha": ["fecha","date","dia","día"],
    "Hora":  ["hora","time"],
    "Base":  ["base","sede","plataforma"],
    "Moviles_Reales":  ["moviles_reales","mov_real","mov_obs","moviles_obs","mov x nomina","moviles x nomina"],
    "Servicios_Reales":["servicios_reales","svc_real","llamadas_real","llamadas_reales","svc reales","servicios reales"],
}

def _guess_map(df: pd.DataFrame, expected: dict[str, list[str]]) -> dict[str, str]:
    """Sugerir mapeo por coincidencia insensible a mayúsculas y acentos simples."""
    def norm(s: str) -> str:
        tr = str(s).strip().lower()
        tr = (tr.replace("á","a").replace("é","e").replace("í","i")
                .replace("ó","o").replace("ú","u").replace("ñ","n"))
        return tr
    cols = {norm(c): c for c in df.columns}
    m = {}
    for target, aliases in expected.items():
        hit = None
        for a in aliases+[target]:
            na = norm(a)
            if na in cols:
                hit = cols[na]; break
        m[target] = hit if hit else ""
    return m

def apply_map(df: pd.DataFrame, mapping: dict[str,str], kind: str) -> pd.DataFrame:
    """Renombra columnas usando mapping y devuelve sólo las esperadas."""
    if kind=="plan":
        want = ["Fecha","Hora","Base","Moviles_Planificados","Servicios_Planificados"]
    else:
        want = ["Fecha","Hora","Base","Moviles_Reales","Servicios_Reales"]
    miss = [k for k in want if not mapping.get(k)]
    if miss:
        raise ValueError(f"Faltan columnas en el mapeo: {', '.join(miss)}")
    return df.rename(columns=mapping)[want].copy()

def enrich_time(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte Fecha/Hora, crea Año, Mes, Semana ISO, Día, HoraStr."""
    out = df.copy()
    out["Fecha"] = pd.to_datetime(out["Fecha"], errors="coerce").dt.date
    if "Hora" in out.columns:
        if is_numeric_dtype(out["Hora"]):
            # serial Excel (fracción de día)
            frac = (pd.to_numeric(out["Hora"], errors="coerce") % 1)
            td = pd.to_timedelta(frac, unit="D")
            out["Hora"] = (pd.Timestamp("1900-01-01") + td).dt.time
        else:
            out["Hora"] = pd.to_datetime(out["Hora"].astype(str), errors="coerce").dt.time
    out["Fecha_dt"] = pd.to_datetime(out["Fecha"])
    iso = out["Fecha_dt"].dt.isocalendar()
    out["Año"]    = out["Fecha_dt"].dt.year
    out["Mes"]    = out["Fecha_dt"].dt.month
    out["Semana"] = iso.week
    out["Dia"]    = out["Fecha_dt"].dt.day
    out["HoraStr"]= pd.to_datetime(out["Hora"].astype(str), errors="coerce").dt.strftime("%H:%M")
    return out

# ==========================
# 2) Merge y métricas
# ==========================
def merge_plan_real(plan: pd.DataFrame, real: pd.DataFrame) -> pd.DataFrame:
    """Merge outer por Fecha+Hora+Base y clasifica match vs no-match."""
    keys = ["Fecha","Hora","Base"]
    merged = pd.merge(plan, real, on=keys, how="outer", indicator=True)
    merged["Status"] = np.select(
        [merged["_merge"].eq("left_only"),
         merged["_merge"].eq("right_only"),
         merged["_merge"].eq("both")],
        ["No ejecutado","No planificado","OK"], default="Desconocido"
    )
    merged.drop(columns=["_merge"], inplace=True)
    return merged

def _to_num(s: pd.Series) -> pd.Series:
    """Normaliza coma/punto y miles, convierte a float; errores -> NaN."""
    s = s.astype(str).str.strip()
    s = s.replace({"": np.nan, "nan": np.nan, "None": np.nan,
                   "#¿NOMBRE?": np.nan, "#¡NOMBRE?": np.nan, "#VALUE!": np.nan}, regex=False)
    def _fix_one(x: str):
        if x is np.nan or x is None:
            return np.nan
        txt = str(x)
        if "," in txt and "." in txt:
            if txt.rfind(",") > txt.rfind("."):
                txt = txt.replace(".", "").replace(",", ".")
            else:
                txt = txt.replace(",", "")
        elif "," in txt:
            txt = txt.replace(",", ".")
        try:
            return float(txt)
        except Exception:
            return np.nan
    return s.map(_fix_one)

def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Diferencias, % desvío, clasificación, efectividad y errores (sobre Servicios)."""
    out = df.copy()
    for c in ["Moviles_Planificados","Servicios_Planificados","Moviles_Reales","Servicios_Reales"]:
        if c not in out:
            out[c] = np.nan
        out[c] = _to_num(out[c])

    out[["Moviles_Planificados","Servicios_Planificados","Moviles_Reales","Servicios_Reales"]] = (
        out[["Moviles_Planificados","Servicios_Planificados","Moviles_Reales","Servicios_Reales"]].fillna(0.0)
    )

    out["Dif_Moviles"]   = out["Moviles_Reales"]   - out["Moviles_Planificados"]
    out["Dif_Servicios"] = out["Servicios_Reales"] - out["Servicios_Planificados"]

    out["Desvio_Moviles_%"] = np.where(
        out["Moviles_Planificados"] > 0,
        out["Dif_Moviles"] / out["Moviles_Planificados"] * 100, np.nan
    )
    out["Desvio_Servicios_%"] = np.where(
        out["Servicios_Planificados"] > 0,
        out["Dif_Servicios"] / out["Servicios_Planificados"] * 100, np.nan
    )

    out["Clasificacion"] = np.select(
        [
            out.get("Status","").astype(str).eq("No ejecutado"),
            out.get("Status","").astype(str).eq("No planificado"),
            out["Dif_Servicios"].fillna(0).eq(0),
            out["Dif_Servicios"].fillna(0) > 0,
            out["Dif_Servicios"].fillna(0) < 0
        ],
        ["No ejecutado","No planificado","Exacto","Sobre planificado","Bajo planificado"],
        default="NA"
    )

    out["Efectividad"] = np.where(
        out["Servicios_Planificados"] > 0,
        1 - (out["Dif_Servicios"].abs() / out["Servicios_Planificados"]),
        np.nan
    )

    out["AE"]  = (out["Servicios_Reales"] - out["Servicios_Planificados"]).abs()
    out["APE"] = np.where(
        out["Servicios_Planificados"] > 0,
        out["AE"] / out["Servicios_Planificados"],
        np.nan
    )
    out["Bias"] = (out["Servicios_Planificados"] - out["Servicios_Reales"])
    return out

def agg_error_metrics(df: pd.DataFrame) -> dict:
    """MAPE (%), MAE, Forecast Bias (%)."""
    d = df.copy()
    mape = d["APE"].mean()*100 if d["APE"].notna().any() else np.nan
    mae  = d["AE"].mean() if d["AE"].notna().any() else np.nan
    fbias = (d["Bias"].sum()/d["Servicios_Reales"].sum()*100) if d["Servicios_Reales"].sum()!=0 else np.nan
    return {"MAPE_%": mape, "MAE": mae, "ForecastBias_%": fbias}

# ==========================
# 3) Filtros y top‑N
# ==========================
def add_time_keys(df: pd.DataFrame) -> pd.DataFrame:
    if "Fecha_dt" not in df.columns:
        df["Fecha_dt"] = pd.to_datetime(df["Fecha"])
    iso = df["Fecha_dt"].dt.isocalendar()
    df["Año"] = df["Fecha_dt"].dt.year
    df["Mes"] = df["Fecha_dt"].dt.month
    df["Semana"] = iso.week
    df["Dia"] = df["Fecha_dt"].dt.day
    df["HoraStr"] = pd.to_datetime(df["Hora"].astype(str), errors="coerce").dt.strftime("%H:%M")
    return df

def filter_df(df: pd.DataFrame,
              bases: list[str]|None=None,
              fecha: pd.Timestamp|None=None,
              semana: int|None=None,
              anio: int|None=None,
              mes: int|None=None,
              hora_sel: list[str]|None=None) -> pd.DataFrame:
    d = df.copy()
    if bases: d = d[d["Base"].isin(bases)]
    if fecha is not None: d = d[d["Fecha"].eq(pd.to_datetime(fecha).date())]
    if semana: d = d[d["Semana"].eq(int(semana))]
    if anio: d = d[d["Año"].eq(int(anio))]
    if mes is not None:
        if isinstance(mes, int):
            d = d[d["Mes"].eq(int(mes))]
        elif isinstance(mes, str) and "-" in mes:
            try:
                aa, mm = mes.split("-"); aa=int(aa); mm=int(mm)
                d = d[(d["Año"].eq(aa)) & (d["Mes"].eq(mm))]
            except Exception:
                pass
    if hora_sel: d = d[d["HoraStr"].isin(hora_sel)]
    return d

def top5_hours(df: pd.DataFrame):
    g = df.groupby("HoraStr", as_index=False)["Dif_Servicios"].sum()
    sub = g.nsmallest(5, "Dif_Servicios")
    sobre = g.nlargest(5, "Dif_Servicios")
    return sub, sobre

def worst_base(df: pd.DataFrame):
    g_abs = df.groupby("Base", as_index=False)["Dif_Servicios"].agg(Desvio_absoluto=lambda s: s.abs().sum())
    return g_abs.sort_values("Desvio_absoluto", ascending=False).head(1)

# ==========================
# 4) Persistencia CSV + export simple
# ==========================
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
PLAN_CSV = DATA_DIR/"plan.csv"
REAL_CSV = DATA_DIR/"real.csv"
MERG_CSV = DATA_DIR/"merged.csv"

def save_csv(df: pd.DataFrame, path: Path):
    df.to_csv(path, index=False, encoding="utf-8")

def load_csv(path: Path) -> pd.DataFrame|None:
    return pd.read_csv(path, encoding="utf-8") if path.exists() else None

def to_excel_bytes(df: pd.DataFrame, sheet_name="datos", fname="reporte.xlsx") -> tuple[bytes,str]:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as wr:
        df.to_excel(wr, index=False, sheet_name=sheet_name)
    return buf.getvalue(), fname

# ==========================
# 5) Formato horario simple (1 hoja) + Libro multi-hojas
# ==========================
def _coerce_num(s):
    """Convierte a número con manejo de coma/punto."""
    return _to_num(pd.Series(s)).astype(float)

def _infer_base_from_text(texto: str) -> str:
    """Infiero Base desde el nombre de la hoja o un rótulo."""
    s = str(texto).upper()
    if "6001" in s:   return "PROY_6001"
    if "MECA" in s:   return "PROY_MECA"
    if "10541" in s:  return "PROY_10541"
    if "13305" in s:  return "PROY_13305"
    if "DEMOTOS" in s:return "DEMOTOS"
    s = re.sub(r"[^A-Z0-9_]+", "_", s)
    return s[:20] if s else "TOTAL"

def load_hourly_simple(df_raw: pd.DataFrame, fecha: str, base: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Lee columnas:
      HORA | SVC PROY | SVC REALES | MOV REQ | MOVILES X NOMINA | [COEFICIENTE HS] | [DIF MOVILES]
    y devuelve plan_df y real_df normalizados.
    """
    df = df_raw.copy()

    def _find(colnames: list[str], *aliases) -> str:
        low = {str(c).strip().lower(): c for c in colnames}
        al = [a.lower() for a in aliases]
        for a in al:
            if a in low: return low[a]
        return ""

    cols = list(df.columns)
    c_hora   = _find(cols, "hora")
    c_svc_p  = _find(cols, "svc proy", "servicios proy", "svc plan", "serv plan")
    c_svc_r  = _find(cols, "svc reales", "servicios reales", "svc real", "serv real")
    c_mov_p  = _find(cols, "mov req", "mov requeridos", "moviles plan", "mov plan")
    c_mov_r  = _find(cols, "moviles x nomina", "mov x nomina", "mov reales", "mov real")
    c_coef   = _find(cols, "coeficiente hs", "coef hs")
    c_difm   = _find(cols, "dif moviles", "dif mov", "delta moviles")

    missing = [("HORA", c_hora), ("SVC PROY", c_svc_p), ("SVC REALES", c_svc_r),
               ("MOV REQ", c_mov_p), ("MOVILES X NOMINA", c_mov_r)]
    faltan = [k for k, v in missing if v == ""]
    if faltan:
        raise ValueError(f"Faltan columnas en el archivo: {', '.join(faltan)}")

    out = pd.DataFrame()
    out["Hora"] = pd.to_datetime(df[c_hora].astype(str), errors="coerce").dt.time
    out["Servicios_Planificados"] = _coerce_num(df[c_svc_p])
    out["Servicios_Reales"]       = _coerce_num(df[c_svc_r])
    out["Moviles_Planificados"]   = _coerce_num(df[c_mov_p])
    out["Moviles_Reales"]         = _coerce_num(df[c_mov_r])

    if c_coef: out["Coeficiente_HS"] = _coerce_num(df[c_coef])
    if c_difm: out["Dif_Moviles_Archivo"] = _coerce_num(df[c_difm])

    out["Fecha"] = pd.to_datetime(str(fecha)).date()
    out["Base"]  = str(base).strip().upper() if base else "TOTAL"

    plan = out[["Fecha","Hora","Base","Moviles_Planificados","Servicios_Planificados"]].copy()
    real = out[["Fecha","Hora","Base","Moviles_Reales","Servicios_Reales"]].copy()

    plan = enrich_time(plan)
    real = enrich_time(real)
    return plan, real

def load_hourly_simple_book(xls_file, fecha: str):
    """
    Lee TODAS las hojas del libro como formato horario simple.
    Devuelve: (plan_concat, real_concat, reporte_list)
    """
    xl = pd.ExcelFile(xls_file)  # engine="openpyxl" si tu entorno lo requiere
    plan_all, real_all, reporte = [], [], []

    for sh in xl.sheet_names:
        base = _infer_base_from_text(sh)
        try:
            df_raw = pd.read_excel(xl, sheet_name=sh)
            try:
                top_left = str(df_raw.iloc[0,0])
                base2 = _infer_base_from_text(top_left)
                if base2 not in ("TOTAL", base): 
                    base = base2
            except Exception:
                pass

            plan_s, real_s = load_hourly_simple(df_raw, fecha=fecha, base=base)
            plan_all.append(plan_s); real_all.append(real_s)
            reporte.append({"sheet": sh, "base": base, "filas_plan": len(plan_s), "filas_real": len(real_s), "ok": True})
        except Exception as e:
            reporte.append({"sheet": sh, "base": base, "filas_plan": 0, "filas_real": 0, "ok": False, "error": str(e)})

    plan_concat = pd.concat(plan_all, ignore_index=True) if plan_all else pd.DataFrame()
    real_concat = pd.concat(real_all, ignore_index=True) if real_all else pd.DataFrame()
    return plan_concat, real_concat, reporte
