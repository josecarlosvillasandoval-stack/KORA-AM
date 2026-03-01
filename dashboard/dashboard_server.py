"""
dashboard_server.py — KORA AM Dashboard Server
Sirve el dashboard operativo en http://localhost:5002

Fuentes de datos:
  1. Supabase (kora_finance_mart, kora_ops_mart, kora_investor_mart)  ← modo producción
  2. Google Sheets directo (gspread)                                  ← fallback si dbt no ha corrido
  3. Datos estáticos de Premisas                                      ← fallback final

USO:
  cd "/Users/jcvs/Desktop/KORA AM/dashboards"
  /opt/homebrew/bin/python3.11 dashboard_server.py

  Opciones de entorno:
    DATA_SOURCE=supabase|sheets|static   (default: sheets)
    PORT=5002
    PRY002_SHEET_ID=<id del sheet PRY-002>
"""

import os
import sys
import json
import base64
import tempfile
from datetime import datetime, date

from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS

# ── SA JSON — soporta archivo local o variable de entorno (Render/cloud) ──────
def _resolve_sa_file():
    sa_b64 = os.getenv('SA_JSON_B64', '')
    if sa_b64:
        try:
            tmp = tempfile.NamedTemporaryFile(suffix='.json', delete=False, mode='w')
            tmp.write(base64.b64decode(sa_b64).decode('utf-8'))
            tmp.close()
            return tmp.name
        except Exception as e:
            print(f'  ⚠ Error decodificando SA_JSON_B64: {e}')
    # Ruta local como fallback
    return os.getenv('SA_FILE', '/Users/jcvs/Desktop/kora-am-platform/secrets/kora-service-account.json')

# ── Config ─────────────────────────────────────────────────────────────────────
PORT         = int(os.getenv('PORT', 5002))
DATA_SOURCE  = os.getenv('DATA_SOURCE', 'sheets')
SHEET_ID     = os.getenv('PRY002_SHEET_ID', '')
SA_FILE      = _resolve_sa_file()
SUPABASE_ENV = os.getenv('SUPABASE_ENV', '/Users/jcvs/Desktop/kora-am-platform/.env.local')
PROJECT_ID   = 'PRY-002'

DASHBOARD_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__, static_folder=DASHBOARD_DIR)
CORS(app)

# ── Helpers ────────────────────────────────────────────────────────────────────

def load_supabase_env():
    """Carga variables de .env.local sin dotenv."""
    env = {}
    try:
        with open(SUPABASE_ENV) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    k, v = line.split('=', 1)
                    env[k.strip()] = v.strip()
    except Exception:
        pass
    return env


def parse_num(val, default=0.0):
    """Convierte valor de gspread a float, manejando formatos europeos y fechas."""
    if val is None or val == '':
        return default
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        # Fecha auto-formateada por GSheets (e.g. '7074-08-22', '2026-10-05')
        if len(val) == 10 and val[4] == '-' and val[7] == '-':
            return default
        # Formato europeo con puntos como miles: '1.890.000' → 1890000
        # Formato con comas: '1,890,000' → 1890000
        clean = val.replace('$', '').replace('%', '').replace(' ', '')
        # Si tiene más de un punto, tratar los puntos como separadores de miles
        if clean.count('.') > 1:
            clean = clean.replace('.', '')
        # Coma como separador de miles (no decimal)
        elif clean.count(',') > 1 or (clean.count(',') == 1 and len(clean.split(',')[1]) == 3):
            clean = clean.replace(',', '')
        else:
            clean = clean.replace(',', '.')
        try:
            return float(clean)
        except Exception:
            return default
    return default


def fmt_mxn(val):
    """Formatea número como string MXN para el JSON."""
    try:
        return round(parse_num(val), 2)
    except Exception:
        return 0.0


# ── Data loaders ───────────────────────────────────────────────────────────────

def load_from_supabase():
    """Lee kora_finance_mart.fct_project_summary y marts relacionados."""
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        install('psycopg2-binary')
        import psycopg2
        import psycopg2.extras

    env = load_supabase_env()
    # La contraseña viene del env
    pw = os.getenv('SUPABASE_PASSWORD') or env.get('SUPABASE_PASSWORD', '')

    conn = psycopg2.connect(
        host=env.get('SUPABASE_HOST', ''),
        port=int(env.get('SUPABASE_PORT', 5432)),
        dbname=env.get('SUPABASE_DB', 'postgres'),
        user=env.get('SUPABASE_USER', 'postgres'),
        password=pw,
        sslmode='require',
        connect_timeout=8,
    )
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Resumen financiero
    cur.execute(f"""
        SELECT * FROM kora_finance_mart.fct_project_summary
        WHERE project_id = %s
    """, (PROJECT_ID,))
    summary = dict(cur.fetchone() or {})

    # Ventas por mes
    cur.execute(f"""
        SELECT mes, ventas_firmadas, revenue_firmado, cobrado_total,
               presupuesto_mes, costo_ejecutado_mes, flujo_neto_mes
        FROM kora_ops_mart.fct_sales_by_month
        WHERE project_id = %s
        ORDER BY mes
    """, (PROJECT_ID,))
    by_month = [dict(r) for r in cur.fetchall()]

    # LP Waterfall
    cur.execute(f"""
        SELECT tranche, capital_aportado, target_payout, yield_target,
               capital_retornado, yield_retornado, total_retornado,
               pendiente_pagar, pct_completado
        FROM kora_investor_mart.fct_lp_waterfall
        WHERE project_id = %s
        ORDER BY tranche
    """, (PROJECT_ID,))
    waterfall = [dict(r) for r in cur.fetchall()]

    cur.close()
    conn.close()

    return build_payload(summary, by_month, waterfall, source='supabase')


def load_from_sheets():
    """Lee el Google Sheet PRY-002 directamente."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        install('gspread'); install('google-auth')
        import gspread
        from google.oauth2.service_account import Credentials

    sheet_id = SHEET_ID or os.getenv('PRY002_SHEET_ID', '')
    if not sheet_id:
        print('  ⚠ PRY002_SHEET_ID no definido — usando datos estáticos')
        return load_static()

    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly',
              'https://www.googleapis.com/auth/drive.readonly']
    creds = Credentials.from_service_account_file(SA_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    ss = gc.open_by_key(sheet_id)

    def tab(name):
        try:
            # UNFORMATTED_VALUE devuelve números como números, evita bugs de formato %/europeo
            return ss.worksheet(name).get_all_records(
                value_render_option='UNFORMATTED_VALUE'
            )
        except Exception:
            return []

    config_rows   = tab('CONFIG')
    premisas_rows = tab('PREMISAS')
    ventas_rows   = tab('VENTAS')
    gastos_rows   = tab('GASTOS')
    cobranza_rows = tab('COBRANZA')
    capital_rows  = tab('CAPITAL_CALLS')
    hitos_rows    = tab('HITOS')
    avance_rows   = tab('AVANCE_OBRA')
    ppto_rows     = tab('PRESUPUESTO')
    deuda_rows    = tab('DEUDA')

    # ── Premisas dict ──────────────────────────────────────────────────────────
    prem = {r['Campo']: r['Valor'] for r in premisas_rows if r.get('Campo')}

    def p(key, default=0):
        try:
            return float(prem.get(key, default))
        except Exception:
            return default

    revenue_total   = p('revenue_total',   96_600_000)
    ppto_total      = p('ppto_total',      74_163_510)
    lp_equity       = p('lp_equity',       35_500_000)
    lp_multiple     = p('lp_multiple',     1.5)
    lp_payout       = p('lp_payout',       53_250_000)
    gp_promote      = p('gp_promote',       4_686_490)
    total_unidades  = int(p('total_unidades', 15))

    # ── Ventas ─────────────────────────────────────────────────────────────────
    ventas_firmadas = [v for v in ventas_rows if str(v.get('status','')).lower() == 'firmado']
    revenue_firmado = sum(parse_num(v.get('precio_lista', 0)) for v in ventas_firmadas)
    cobranza_total  = sum(parse_num(c.get('monto', 0)) for c in cobranza_rows)

    # ── Gastos ─────────────────────────────────────────────────────────────────
    costo_ejecutado = sum(parse_num(g.get('pagado', 0)) for g in gastos_rows)

    # ── Capital LP ─────────────────────────────────────────────────────────────
    lp_recibido = sum(parse_num(k.get('monto', 0)) for k in capital_rows
                      if str(k.get('status', '')).lower() == 'recibido')

    # ── Ventas por mes (M0-M36) ────────────────────────────────────────────────
    by_month_dict = {m: {'mes': m, 'ventas_firmadas': 0, 'revenue_firmado': 0.0,
                          'cobrado_total': 0.0, 'costo_ejecutado_mes': 0.0}
                     for m in range(37)}

    for v in ventas_firmadas:
        m = int(v.get('mes', 0))
        if 0 <= m <= 36:
            by_month_dict[m]['ventas_firmadas']  += 1
            by_month_dict[m]['revenue_firmado']  += parse_num(v.get('precio_lista', 0))

    for c in cobranza_rows:
        m = int(c.get('mes', 0))
        if 0 <= m <= 36:
            by_month_dict[m]['cobrado_total'] += parse_num(c.get('monto', 0))

    for g in gastos_rows:
        m = int(g.get('mes', 0))
        if 0 <= m <= 36:
            by_month_dict[m]['costo_ejecutado_mes'] += parse_num(g.get('pagado', 0))

    by_month = [by_month_dict[m] for m in range(37)]

    # ── LP Waterfall ──────────────────────────────────────────────────────────
    waterfall = []
    for k in capital_rows:
        aportado = parse_num(k.get('monto', 0))
        waterfall.append({
            'tranche':          k.get('tranche', ''),
            'capital_aportado': aportado,
            'target_payout':    aportado * lp_multiple,
            'yield_target':     aportado * (lp_multiple - 1),
            'capital_retornado': 0,
            'yield_retornado':   0,
            'total_retornado':   0,
            'pendiente_pagar':   aportado * lp_multiple,
            'pct_completado':    0.0,
        })

    # ── Avance obra ────────────────────────────────────────────────────────────
    avance = [{'mes': a.get('mes', 0), 'label': a.get('label', ''),
               'pct_avance': float(a.get('pct_avance', 0)),
               'pct_objetivo': float(a.get('pct_objetivo', 0)),
               'actividad': a.get('actividad', '')}
              for a in avance_rows]

    # ── Hitos ─────────────────────────────────────────────────────────────────
    hitos = [{'mes': h.get('mes', 0), 'hito': h.get('hito', ''),
              'status': h.get('status', ''), 'fecha_objetivo': h.get('fecha_objetivo', '')}
             for h in hitos_rows]

    summary = {
        'project_id':          PROJECT_ID,
        'project_name':        'Mártires 122',
        'status':              'construction',
        'total_units':         total_unidades,
        'revenue_proforma':    revenue_total,
        'ppto_proforma':       ppto_total,
        'lp_equity':           lp_equity,
        'lp_multiple':         lp_multiple,
        'lp_payout_target':    lp_payout,
        'gp_promote_est':      gp_promote,
        'margen_bruto_proforma': revenue_total - ppto_total,
        'unidades_firmadas':   len(ventas_firmadas),
        'revenue_firmado':     revenue_firmado,
        'pct_revenue_firmado': revenue_firmado / revenue_total if revenue_total else 0,
        'pct_absorcion':       len(ventas_firmadas) / total_unidades if total_unidades else 0,
        'costo_ejecutado':     costo_ejecutado,
        'pct_presupuesto_ejecutado': costo_ejecutado / ppto_total if ppto_total else 0,
        'cobranza_total':      cobranza_total,
        'lp_equity_recibido':  lp_recibido,
        'avance':              avance,
        'hitos':               hitos,
    }

    return build_payload(summary, by_month, waterfall, source='sheets',
                         detail_capital=capital_rows,
                         detail_ventas=ventas_rows,
                         detail_cobranza=cobranza_rows,
                         detail_gastos=gastos_rows,
                         detail_presupuesto=ppto_rows,
                         detail_hitos=hitos_rows,
                         detail_avance=avance_rows,
                         detail_deuda=deuda_rows)


def load_static():
    """Datos estáticos de la proforma — fallback sin conexión."""
    summary = {
        'project_id':          PROJECT_ID,
        'project_name':        'Mártires 122',
        'status':              'construction',
        'total_units':         15,
        'revenue_proforma':    96_600_000,
        'ppto_proforma':       74_163_510,
        'lp_equity':           35_500_000,
        'lp_multiple':         1.5,
        'lp_payout_target':    53_250_000,
        'gp_promote_est':       4_686_490,
        'margen_bruto_proforma': 22_436_490,
        'unidades_firmadas':   2,
        'revenue_firmado':     12_600_000,
        'pct_revenue_firmado': 0.130,
        'pct_absorcion':       0.133,
        'costo_ejecutado':     11_971_395,
        'pct_presupuesto_ejecutado': 0.161,
        'cobranza_total':       3_780_000,
        'lp_equity_recibido':  35_500_000,
        'avance': [
            {'mes': 0,  'label': 'Abr-26', 'pct_avance': 0,  'pct_objetivo': 0,  'actividad': 'Inicio'},
            {'mes': 1,  'label': 'May-26', 'pct_avance': 3,  'pct_objetivo': 3,  'actividad': 'Excavación'},
            {'mes': 2,  'label': 'Jun-26', 'pct_avance': 8,  'pct_objetivo': 8,  'actividad': 'Cimentación'},
            {'mes': 3,  'label': 'Jul-26', 'pct_avance': 13, 'pct_objetivo': 13, 'actividad': 'Estructura P1'},
            {'mes': 4,  'label': 'Ago-26', 'pct_avance': 18, 'pct_objetivo': 18, 'actividad': 'Estructura P2'},
            {'mes': 5,  'label': 'Sep-26', 'pct_avance': 23, 'pct_objetivo': 23, 'actividad': 'Estructura P3'},
            {'mes': 6,  'label': 'Oct-26', 'pct_avance': 28, 'pct_objetivo': 28, 'actividad': 'Ventas abiertas'},
            {'mes': 7,  'label': 'Nov-26', 'pct_avance': 33, 'pct_objetivo': 33, 'actividad': 'Estructura P5'},
            {'mes': 8,  'label': 'Dic-26', 'pct_avance': 38, 'pct_objetivo': 38, 'actividad': 'Muros fachada'},
            {'mes': 9,  'label': 'Ene-27', 'pct_avance': 44, 'pct_objetivo': 44, 'actividad': 'Instalaciones'},
            {'mes': 10, 'label': 'Feb-27', 'pct_avance': 50, 'pct_objetivo': 50, 'actividad': 'Instalaciones'},
            {'mes': 11, 'label': 'Mar-27', 'pct_avance': 55, 'pct_objetivo': 55, 'actividad': 'Muros interiores'},
            {'mes': 12, 'label': 'Abr-27', 'pct_avance': 60, 'pct_objetivo': 60, 'actividad': 'Obra gris'},
        ],
        'hitos': [
            {'mes': 0,  'hito': 'Inicio de obra',      'status': 'Completado', 'fecha_objetivo': '2026-04-01'},
            {'mes': 0,  'hito': 'Tranche A LP',         'status': 'Completado', 'fecha_objetivo': '2026-04-01'},
            {'mes': 2,  'hito': 'Cimentación OK',       'status': 'Completado', 'fecha_objetivo': '2026-06-01'},
            {'mes': 2,  'hito': 'Tranche B LP',         'status': 'Completado', 'fecha_objetivo': '2026-06-01'},
            {'mes': 5,  'hito': 'Tranche C LP',         'status': 'Completado', 'fecha_objetivo': '2026-09-01'},
            {'mes': 6,  'hito': 'Apertura de ventas',   'status': 'Completado', 'fecha_objetivo': '2026-10-01'},
            {'mes': 12, 'hito': 'Obra gris terminada',  'status': 'Pendiente',  'fecha_objetivo': '2027-04-01'},
            {'mes': 24, 'hito': 'Entrega de llaves',    'status': 'Pendiente',  'fecha_objetivo': '2028-04-01'},
            {'mes': 36, 'hito': 'Cierre del proyecto',  'status': 'Pendiente',  'fecha_objetivo': '2029-04-01'},
        ],
    }

    # Cashflow proforma simplificado (ventas y costos por mes)
    by_month = []
    for m in range(37):
        vf  = 12_600_000 if m == 6 else (9_450_000 if 7 <= m <= 8 else 0)
        cob = 3_780_000  if m == 6 else 0
        cos = {0: 9_040_000, 1: 1_598_062, 2: 1_473_062, 3: 3_986_254,
               4: 2_652_921, 5: 3_986_254, 6: 2_652_921, 7: 3_986_254,
               8: 2_652_921, 9: 3_986_254, 10: 2_652_921, 11: 3_986_254,
               12: 2_652_921}.get(m, 1_200_000 if 13 <= m <= 24 else 0)
        by_month.append({'mes': m, 'ventas_firmadas': 2 if m == 6 else 0,
                          'revenue_firmado': vf, 'cobrado_total': cob,
                          'costo_ejecutado_mes': cos,
                          'flujo_neto_mes': cob - cos})

    waterfall = [
        {'tranche': 'A', 'capital_aportado': 20_000_000, 'target_payout': 30_000_000,
         'yield_target': 10_000_000, 'capital_retornado': 0, 'yield_retornado': 0,
         'total_retornado': 0, 'pendiente_pagar': 30_000_000, 'pct_completado': 0.0},
        {'tranche': 'B', 'capital_aportado': 10_000_000, 'target_payout': 15_000_000,
         'yield_target': 5_000_000, 'capital_retornado': 0, 'yield_retornado': 0,
         'total_retornado': 0, 'pendiente_pagar': 15_000_000, 'pct_completado': 0.0},
        {'tranche': 'C', 'capital_aportado':  5_500_000, 'target_payout':  8_250_000,
         'yield_target': 2_750_000, 'capital_retornado': 0, 'yield_retornado': 0,
         'total_retornado': 0, 'pendiente_pagar':  8_250_000, 'pct_completado': 0.0},
    ]

    return build_payload(summary, by_month, waterfall, source='static')


_NUMERIC_KEYS = {
    'monto','precio_lista','enganche','diferido','residual','presupuestado','pagado',
    'target_payout','pct_avance','pct_objetivo','porcentaje_participacion',
    'revenue_total','ppto_total','lp_equity','lp_multiple','margen_bruto',
    'm2','total_unidades','saldo_insoluto','tasa','plazo',
}

def sanitize_rows(rows):
    """Convierte valores de gspread a tipos básicos JSON-serializables.
    Para campos numéricos conocidos, aplica parse_num para manejar formatos europeos."""
    clean = []
    for row in rows:
        r = {}
        for k, v in row.items():
            if k in _NUMERIC_KEYS:
                r[k] = parse_num(v)
            elif isinstance(v, (int, float, bool, type(None))):
                r[k] = v
            elif isinstance(v, str):
                r[k] = v
            else:
                r[k] = str(v)
        clean.append(r)
    return clean


def build_payload(summary, by_month, waterfall, source='static',
                  detail_capital=None, detail_ventas=None, detail_cobranza=None,
                  detail_gastos=None, detail_presupuesto=None, detail_hitos=None,
                  detail_avance=None, detail_deuda=None):
    """Construye el JSON final que consume el dashboard."""
    # Mes actual del proyecto (M0 = Abr-2026)
    proyecto_start = date(2026, 4, 1)
    today = date.today()
    delta = (today.year - proyecto_start.year) * 12 + (today.month - proyecto_start.month)
    mes_actual = max(0, min(36, delta))

    labels = []
    inicio = date(2026, 4, 1)
    for m in range(37):
        yr  = inicio.year + (inicio.month + m - 1) // 12
        mo  = (inicio.month + m - 1) % 12 + 1
        labels.append(f"{['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'][mo-1]}-{str(yr)[2:]}")

    # Cashflow acumulado proforma (desde datos estáticos del modelo)
    cashflow_pf = [
        -9_040_000, -1_598_062, -1_473_062, -2_652_921, -2_652_921, -3_986_254,
         1_237_079,  -815_842,  -815_842,  -1_749_175, -1_749_175, -1_083_042,
        -2_652_921,  -615_588,   -615_588,   -615_588,   -615_588,   -615_588,
         -615_588,   1_584_412,  1_584_412,  1_584_412,  1_584_412,  1_584_412,
         4_334_412,  1_584_412,  1_584_412,  1_584_412,  1_584_412,  1_584_412,
         4_334_412,  1_584_412,  1_584_412,  4_984_412,  1_584_412,  1_584_412,
        26_234_412,
    ]

    # Acumular
    acum = []
    s = 0
    for v in cashflow_pf:
        s += v
        acum.append(round(s / 1e6, 2))

    by_month_clean = []
    for r in by_month:
        m = int(r.get('mes', 0))
        by_month_clean.append({
            'mes':               m,
            'label':             labels[m] if m < len(labels) else f'M{m}',
            'ventas_firmadas':   int(r.get('ventas_firmadas', 0)),
            'revenue_firmado':   fmt_mxn(r.get('revenue_firmado', 0)),
            'cobrado_total':     fmt_mxn(r.get('cobrado_total', 0)),
            'costo_ejecutado':   fmt_mxn(r.get('costo_ejecutado_mes', 0)),
            'flujo_neto':        fmt_mxn(r.get('cobrado_total', 0)) - fmt_mxn(r.get('costo_ejecutado_mes', 0)),
        })

    waterfall_clean = []
    for w in waterfall:
        cap = fmt_mxn(w.get('capital_aportado', 0))
        tgt = fmt_mxn(w.get('target_payout', cap * 1.5))
        ret = fmt_mxn(w.get('total_retornado', 0))
        pct = round(ret / tgt * 100, 1) if tgt > 0 else 0.0
        waterfall_clean.append({
            'tranche':          w.get('tranche', ''),
            'capital_aportado': cap,
            'target_payout':    tgt,
            'total_retornado':  ret,
            'pendiente_pagar':  fmt_mxn(w.get('pendiente_pagar', tgt - ret)),
            'pct_completado':   pct,
        })

    return {
        'meta': {
            'source':       source,
            'project_id':   PROJECT_ID,
            'project_name': summary.get('project_name', 'Mártires 122'),
            'mes_actual':   mes_actual,
            'label_actual': labels[mes_actual] if mes_actual < len(labels) else f'M{mes_actual}',
            'updated_at':   datetime.now().isoformat(),
        },
        'kpis': {
            'revenue_proforma':         fmt_mxn(summary.get('revenue_proforma', 96_600_000)),
            'revenue_firmado':          fmt_mxn(summary.get('revenue_firmado', 0)),
            'pct_revenue_firmado':      round(float(summary.get('pct_revenue_firmado', 0)) * 100, 1),
            'ppto_proforma':            fmt_mxn(summary.get('ppto_proforma', 74_163_510)),
            'costo_ejecutado':          fmt_mxn(summary.get('costo_ejecutado', 0)),
            'pct_presupuesto_ejecutado': round(float(summary.get('pct_presupuesto_ejecutado', 0)) * 100, 1),
            'cobranza_total':           fmt_mxn(summary.get('cobranza_total', 0)),
            'margen_bruto_proforma':    fmt_mxn(summary.get('margen_bruto_proforma', 22_436_490)),
            'total_units':              int(summary.get('total_units', 15)),
            'unidades_firmadas':        int(summary.get('unidades_firmadas', 0)),
            'pct_absorcion':            round(float(summary.get('pct_absorcion', 0)) * 100, 1),
            'lp_equity':                fmt_mxn(summary.get('lp_equity', 35_500_000)),
            'lp_equity_recibido':       fmt_mxn(summary.get('lp_equity_recibido', 0)),
            'lp_payout_target':         fmt_mxn(summary.get('lp_payout_target', 53_250_000)),
            'lp_multiple':              float(summary.get('lp_multiple', 1.5)),
            'gp_promote_est':           fmt_mxn(summary.get('gp_promote_est', 4_686_490)),
        },
        'cashflow': {
            'labels':     labels,
            'proforma':   acum,
        },
        'by_month':  by_month_clean,
        'waterfall': waterfall_clean,
        'avance':    summary.get('avance', []),
        'hitos':     summary.get('hitos', []),
        'detail': {
            'capital_calls':  sanitize_rows(detail_capital or []),
            'ventas':         sanitize_rows(detail_ventas or []),
            'cobranza':       sanitize_rows(detail_cobranza or []),
            'gastos':         sanitize_rows(detail_gastos or []),
            'presupuesto':    sanitize_rows(detail_presupuesto or []),
            'hitos':          sanitize_rows(detail_hitos or []),
            'avance':         sanitize_rows(detail_avance or []),
            'deuda':          sanitize_rows(detail_deuda or []),
        },
    }


# ── Cache simple (60 segundos) ─────────────────────────────────────────────────
_cache = {'data': None, 'ts': 0}
CACHE_TTL = 60  # segundos

def get_data():
    import time
    now = time.time()
    if _cache['data'] and (now - _cache['ts']) < CACHE_TTL:
        return _cache['data']

    print(f'[{datetime.now().strftime("%H:%M:%S")}] Cargando datos ({DATA_SOURCE})...')
    try:
        if DATA_SOURCE == 'supabase':
            data = load_from_supabase()
        elif DATA_SOURCE == 'sheets':
            data = load_from_sheets()
        else:
            data = load_static()
        _cache['data'] = data
        _cache['ts']   = now
        print(f'  ✅ Datos cargados. Fuente: {data["meta"]["source"]}')
    except Exception as e:
        print(f'  ⚠ Error cargando {DATA_SOURCE}: {e} — usando estáticos')
        data = load_static()
        _cache['data'] = data
        _cache['ts']   = now

    return data


# ── Rutas ──────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return send_from_directory(DASHBOARD_DIR, 'dashboard.html')


@app.route('/api/data')
def api_data():
    data = get_data()
    return jsonify(data)


@app.route('/api/health')
def health():
    return jsonify({'status': 'ok', 'port': PORT, 'source': DATA_SOURCE,
                    'project': PROJECT_ID, 'ts': datetime.now().isoformat()})


@app.route('/api/refresh', methods=['POST'])
def refresh():
    """Fuerza re-carga de datos."""
    _cache['ts'] = 0
    data = get_data()
    return jsonify({'refreshed': True, 'source': data['meta']['source'],
                    'updated_at': data['meta']['updated_at']})


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print(f'\n══ KORA AM Dashboard Server ══')
    print(f'   Proyecto:   {PROJECT_ID} — Mártires 122')
    print(f'   Puerto:     http://localhost:{PORT}')
    print(f'   Data:       {DATA_SOURCE}')
    print(f'   SA File:    {SA_FILE}')
    if DATA_SOURCE == 'sheets' and not SHEET_ID:
        print(f'   ⚠ Tip: exporta PRY002_SHEET_ID=<id> para conectar a Sheets')
    print(f'   Dashboard:  http://localhost:{PORT}')
    print(f'   API:        http://localhost:{PORT}/api/data\n')
    app.run(host='0.0.0.0', port=PORT, debug=False)
