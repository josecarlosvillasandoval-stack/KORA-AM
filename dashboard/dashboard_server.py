"""
dashboard_server.py — KORA AM Dashboard Server (Multi-Proyecto)

Arquitectura:
  - REGISTRY tab en el Sheet maestro lista todos los proyectos del portfolio
  - Cada proyecto tiene su propio Google Sheet con tabs estándar
  - /api/portfolio  → consolidado de todos los proyectos
  - /api/proyecto/<id> → datos completos de un proyecto
  - /api/projects  → lista de proyectos del registry

Entorno:
  REGISTRY_SHEET_ID  — Sheet que contiene la tab REGISTRY
  SA_JSON_B64        — Service Account en base64 (para Render/cloud)
  SA_FILE            — Ruta local al SA JSON (para desarrollo)
  PORT               — Puerto (default 5002)
"""

import os, sys, json, base64, tempfile, time, concurrent.futures
from datetime import datetime, date
from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS

# ── SA JSON ────────────────────────────────────────────────────────────────────
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
    return os.getenv('SA_FILE', '/Users/jcvs/Desktop/kora-am-platform/secrets/kora-service-account.json')

# ── Config ─────────────────────────────────────────────────────────────────────
PORT             = int(os.getenv('PORT', 5002))
SA_FILE          = _resolve_sa_file()
# REGISTRY_SHEET_ID: si no se pasa, usa PRY002_SHEET_ID como fallback
REGISTRY_SHEET_ID = os.getenv('REGISTRY_SHEET_ID') or os.getenv('PRY002_SHEET_ID', '')

DASHBOARD_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, static_folder=DASHBOARD_DIR)
CORS(app)

print(f'''
══ KORA AM Dashboard Server (Multi-Proyecto) ══
   Puerto:    http://localhost:{PORT}
   SA File:   {SA_FILE}
   Registry:  {REGISTRY_SHEET_ID or "⚠ NO CONFIGURADO"}
''')

# ── Helpers numéricos ──────────────────────────────────────────────────────────
def parse_num(val, default=0.0):
    if val is None or val == '': return default
    if isinstance(val, (int, float)): return float(val)
    if isinstance(val, str):
        if len(val) == 10 and val[4] == '-' and val[7] == '-': return default
        clean = val.replace('$','').replace('%','').replace(' ','')
        if clean.count('.') > 1:    clean = clean.replace('.','')
        elif clean.count(',') > 1 or (clean.count(',') == 1 and len(clean.split(',')[1]) == 3):
            clean = clean.replace(',','')
        else: clean = clean.replace(',','.')
        try: return float(clean)
        except: return default
    return default

def fmt_mxn(val):
    try: return round(float(val), 2)
    except: return 0.0

_NUMERIC_KEYS = {
    'monto','precio_lista','enganche','diferido','residual','presupuestado','pagado',
    'target_payout','pct_avance','pct_objetivo','porcentaje_participacion',
    'revenue_total','ppto_total','lp_equity','lp_multiple','margen_bruto',
    'm2','total_unidades','saldo_insoluto','tasa','plazo',
}

def sanitize_rows(rows):
    clean = []
    for row in rows:
        r = {}
        for k, v in row.items():
            if k in _NUMERIC_KEYS: r[k] = parse_num(v)
            elif isinstance(v, (int, float, bool, type(None))): r[k] = v
            else: r[k] = str(v) if not isinstance(v, str) else v
        clean.append(r)
    return clean

# ── Google Sheets client ───────────────────────────────────────────────────────
_gc = None
def get_gc():
    global _gc
    if _gc is None:
        import gspread
        from google.oauth2.service_account import Credentials
        scopes = ['https://www.googleapis.com/auth/spreadsheets',
                  'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(SA_FILE, scopes=scopes)
        _gc = gspread.authorize(creds)
    return _gc

def open_sheet(sheet_id):
    return get_gc().open_by_key(sheet_id)

def tab(ws_or_sh, name):
    """Lee un tab de un spreadsheet como lista de dicts."""
    try:
        if hasattr(ws_or_sh, 'worksheet'):
            return ws_or_sh.worksheet(name).get_all_records(value_render_option='UNFORMATTED_VALUE')
        return ws_or_sh.get_all_records(value_render_option='UNFORMATTED_VALUE')
    except Exception:
        return []

# ── Registry ───────────────────────────────────────────────────────────────────
def load_registry():
    """Lee el tab REGISTRY y retorna lista de proyectos."""
    if not REGISTRY_SHEET_ID:
        return []
    try:
        sh = open_sheet(REGISTRY_SHEET_ID)
        rows = tab(sh, 'REGISTRY')
        return [r for r in rows if r.get('proyecto_id') and r.get('sheet_id')]
    except Exception as e:
        print(f'  ⚠ Error leyendo registry: {e}')
        return []

# ── BVA ────────────────────────────────────────────────────────────────────────
def compute_bva(ppto_rows, gastos_rows, ventas_rows, cobranza_rows, avance_rows, prem):
    modelo_cat, real_cat = {}, {}
    for r in ppto_rows:
        c = r.get('categoria','Sin categoría')
        modelo_cat[c] = modelo_cat.get(c,0) + parse_num(r.get('presupuestado',0))
    for r in gastos_rows:
        c = r.get('categoria','Sin categoría')
        real_cat[c] = real_cat.get(c,0) + parse_num(r.get('pagado',0))

    all_cats = sorted(set(list(modelo_cat)+list(real_cat)), key=lambda c:-modelo_cat.get(c,0))
    costos = []
    for c in all_cats:
        m, r = modelo_cat.get(c,0), real_cat.get(c,0)
        costos.append({'categoria':c,'modelo':m,'real':r,
                       'varianza':r-m,'pct_ejecucion':round(r/m*100,1) if m else 0})
    tm, tr = sum(modelo_cat.values()), sum(real_cat.values())

    rev_mod = parse_num(prem.get('revenue_total', 96_600_000))
    tot_uni = int(parse_num(prem.get('total_unidades', 15)))
    firmadas = [v for v in ventas_rows if str(v.get('status','')).lower()=='firmado']
    rev_real = sum(parse_num(v.get('precio_lista',0)) for v in firmadas)

    enganche_esp = sum(parse_num(v.get('enganche',0)) for v in firmadas)
    cobrado = sum(parse_num(c.get('monto',0)) for c in cobranza_rows
                  if str(c.get('status','')).lower()=='recibido')
    cobrado_x_u = {}
    for c in cobranza_rows:
        u = c.get('unidad_id','?')
        cobrado_x_u[u] = cobrado_x_u.get(u,0) + parse_num(c.get('monto',0))
    cobr_det = [{'unidad_id':v.get('unidad_id'),'comprador':v.get('comprador','—'),
                 'esperado':parse_num(v.get('enganche',0)),
                 'cobrado':cobrado_x_u.get(v.get('unidad_id','?'),0),
                 'pendiente':parse_num(v.get('enganche',0))-cobrado_x_u.get(v.get('unidad_id','?'),0),
                 'tasa':round(cobrado_x_u.get(v.get('unidad_id','?'),0)/parse_num(v.get('enganche',0))*100,1)
                       if parse_num(v.get('enganche',0)) else 0}
                for v in firmadas]

    av_bva = [{'mes':a.get('mes',0),'actividad':a.get('actividad',''),
               'objetivo':parse_num(a.get('pct_objetivo',0)),
               'real':parse_num(a.get('pct_avance',0)),
               'delta':round(parse_num(a.get('pct_avance',0))-parse_num(a.get('pct_objetivo',0)),1)}
              for a in avance_rows]

    margen_mod = parse_num(prem.get('margen_bruto', rev_mod-tm))
    margen_real = rev_real - tr
    return {
        'costos': costos,
        'costos_total': {'modelo':tm,'real':tr,'varianza':tr-tm,
                         'pct_ejecucion':round(tr/tm*100,1) if tm else 0},
        'ingresos': {'revenue_modelo':rev_mod,'revenue_real':rev_real,
                     'varianza':rev_real-rev_mod,
                     'pct_avance':round(rev_real/rev_mod*100,1) if rev_mod else 0,
                     'unidades_modelo':tot_uni,'unidades_real':len(firmadas),
                     'pct_unidades':round(len(firmadas)/tot_uni*100,1) if tot_uni else 0},
        'cobranza': {'esperado':enganche_esp,'cobrado':cobrado,'pendiente':enganche_esp-cobrado,
                     'tasa_cobro':round(cobrado/enganche_esp*100,1) if enganche_esp else 0,
                     'detalle':cobr_det},
        'avance': av_bva,
        'margen': {'modelo':margen_mod,'real':margen_real,
                   'pct_modelo':round(margen_mod/rev_mod*100,1) if rev_mod else 0,
                   'pct_real':round(margen_real/rev_real*100,1) if rev_real else 0},
    }

# ── Cargar un proyecto ─────────────────────────────────────────────────────────
def load_project(meta):
    """Carga todos los datos de un proyecto desde su Google Sheet."""
    pid      = meta['proyecto_id']
    nombre   = meta.get('nombre', pid)
    sheet_id = meta['sheet_id']
    fondo    = meta.get('fondo', '—')
    ciudad   = meta.get('ciudad', '—')
    tipo     = meta.get('tipo_activo', '—')
    moneda   = meta.get('moneda', 'MXN')

    try:
        sh = open_sheet(sheet_id)
        prem_rows   = tab(sh, 'PREMISAS')
        ventas_rows = tab(sh, 'VENTAS')
        gastos_rows = tab(sh, 'GASTOS')
        cobr_rows   = tab(sh, 'COBRANZA')
        cap_rows    = tab(sh, 'CAPITAL_CALLS')
        hitos_rows  = tab(sh, 'HITOS')
        avance_rows = tab(sh, 'AVANCE_OBRA')
        ppto_rows   = tab(sh, 'PRESUPUESTO')
        deuda_rows  = tab(sh, 'DEUDA')
        source = 'sheets'
    except Exception as e:
        print(f'  ⚠ Error cargando {pid}: {e}')
        prem_rows=ventas_rows=gastos_rows=cobr_rows=cap_rows=[]
        hitos_rows=avance_rows=ppto_rows=deuda_rows=[]
        source = 'error'

    prem = {r['Campo']: r['Valor'] for r in prem_rows if r.get('Campo')}
    def p(k, d=0):
        try: return float(prem.get(k, d))
        except: return d

    rev_total   = p('revenue_total',  96_600_000)
    ppto_total  = p('ppto_total',     74_163_510)
    lp_equity   = p('lp_equity',      35_500_000)
    lp_multiple = p('lp_multiple',    1.5)
    lp_payout   = p('lp_payout',      lp_equity * lp_multiple)
    gp_promote  = p('gp_promote',      4_686_490)
    tot_uni     = int(p('total_unidades', 15))

    firmadas     = [v for v in ventas_rows if str(v.get('status','')).lower()=='firmado']
    rev_firmado  = sum(parse_num(v.get('precio_lista',0)) for v in firmadas)
    cobr_total   = sum(parse_num(c.get('monto',0)) for c in cobr_rows)
    costo_ejec   = sum(parse_num(g.get('pagado',0)) for g in gastos_rows)
    lp_recibido  = sum(parse_num(k.get('monto',0)) for k in cap_rows
                       if str(k.get('status','')).lower()=='recibido')

    avance_list = [{'mes':a.get('mes',0),'label':a.get('label',''),
                    'pct_avance':float(a.get('pct_avance',0)),
                    'pct_objetivo':float(a.get('pct_objetivo',0)),
                    'actividad':a.get('actividad','')} for a in avance_rows]
    avance_actual = next((a['pct_avance'] for a in reversed(avance_list) if a['pct_avance']>0), 0)

    # Waterfall LP
    cap_by_tranche = {}
    for k in cap_rows:
        t = k.get('tranche','?')
        cap_by_tranche.setdefault(t,{'aportado':0,'target':0,'retornado':0})
        cap_by_tranche[t]['aportado']  += parse_num(k.get('monto',0))
        cap_by_tranche[t]['target']    += parse_num(k.get('target_payout',0)) or parse_num(k.get('monto',0))*lp_multiple
        cap_by_tranche[t]['retornado'] += 0
    waterfall = [{'tranche':t,'capital_aportado':v['aportado'],'target_payout':v['target'],
                  'total_retornado':v['retornado'],
                  'pendiente_pagar':v['target']-v['retornado'],
                  'pct_completado':round(v['retornado']/v['target']*100,1) if v['target'] else 0}
                 for t,v in sorted(cap_by_tranche.items())]

    # BVA
    bva = compute_bva(ppto_rows, gastos_rows, ventas_rows, cobr_rows, avance_rows, prem)

    # Labels M0-M36
    inicio = date(2026, 4, 1)
    labels = []
    for m in range(37):
        yr = inicio.year + (inicio.month+m-1)//12
        mo = (inicio.month+m-1)%12+1
        labels.append(f"{['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'][mo-1]}-{str(yr)[2:]}")

    proyecto_start = date(2026, 4, 1)
    today = date.today()
    delta = (today.year-proyecto_start.year)*12+(today.month-proyecto_start.month)
    mes_actual = max(0, min(36, delta))

    return {
        'meta': {
            'proyecto_id':   pid,
            'nombre':        nombre,
            'sheet_id':      sheet_id,
            'fondo':         fondo,
            'tipo_activo':   tipo,
            'ciudad':        ciudad,
            'moneda':        moneda,
            'status':        meta.get('status','—'),
            'source':        source,
            'mes_actual':    mes_actual,
            'label_actual':  labels[mes_actual],
            'updated_at':    datetime.now().isoformat(),
        },
        'kpis': {
            'revenue_proforma':           fmt_mxn(rev_total),
            'revenue_firmado':            fmt_mxn(rev_firmado),
            'pct_revenue_firmado':        round(rev_firmado/rev_total*100,1) if rev_total else 0,
            'ppto_proforma':              fmt_mxn(ppto_total),
            'costo_ejecutado':            fmt_mxn(costo_ejec),
            'pct_presupuesto_ejecutado':  round(costo_ejec/ppto_total*100,1) if ppto_total else 0,
            'cobranza_total':             fmt_mxn(cobr_total),
            'margen_bruto_proforma':      fmt_mxn(rev_total-ppto_total),
            'total_units':                tot_uni,
            'unidades_firmadas':          len(firmadas),
            'pct_absorcion':              round(len(firmadas)/tot_uni*100,1) if tot_uni else 0,
            'lp_equity':                  fmt_mxn(lp_equity),
            'lp_equity_recibido':         fmt_mxn(lp_recibido),
            'lp_payout_target':           fmt_mxn(lp_payout),
            'lp_multiple':                lp_multiple,
            'gp_promote_est':             fmt_mxn(gp_promote),
            'avance_obra_pct':            avance_actual,
        },
        'waterfall': waterfall,
        'avance':    avance_list,
        'hitos':     [{'mes':h.get('mes',0),'hito':h.get('hito',''),
                       'status':h.get('status',''),'fecha_objetivo':h.get('fecha_objetivo','')}
                      for h in hitos_rows],
        'detail': {
            'capital_calls': sanitize_rows(cap_rows),
            'ventas':        sanitize_rows(ventas_rows),
            'cobranza':      sanitize_rows(cobr_rows),
            'gastos':        sanitize_rows(gastos_rows),
            'presupuesto':   sanitize_rows(ppto_rows),
            'hitos':         sanitize_rows(hitos_rows),
            'avance':        sanitize_rows(avance_rows),
            'deuda':         sanitize_rows(deuda_rows),
        },
        'bva': bva,
    }

# ── Portfolio consolidado ──────────────────────────────────────────────────────
def build_portfolio(projects_data):
    """Agrega todos los proyectos en una vista consolidada."""
    if not projects_data:
        return {'proyectos': [], 'kpis': {}, 'por_fondo': {}}

    total_rev_pf   = sum(p['kpis'].get('revenue_proforma',0)   for p in projects_data)
    total_rev_real = sum(p['kpis'].get('revenue_firmado',0)     for p in projects_data)
    total_costo_pf = sum(p['kpis'].get('ppto_proforma',0)       for p in projects_data)
    total_costo_r  = sum(p['kpis'].get('costo_ejecutado',0)     for p in projects_data)
    total_lp_eq    = sum(p['kpis'].get('lp_equity',0)           for p in projects_data)
    total_lp_rec   = sum(p['kpis'].get('lp_equity_recibido',0)  for p in projects_data)
    total_cobr     = sum(p['kpis'].get('cobranza_total',0)       for p in projects_data)
    total_uni_pf   = sum(p['kpis'].get('total_units',0)          for p in projects_data)
    total_uni_firm = sum(p['kpis'].get('unidades_firmadas',0)    for p in projects_data)
    total_margen_m = sum(p['kpis'].get('margen_bruto_proforma',0) for p in projects_data)

    # Resumen por fondo
    por_fondo = {}
    for p in projects_data:
        f = p['meta'].get('fondo','Sin fondo')
        if f not in por_fondo:
            por_fondo[f] = {'proyectos':0,'revenue_pf':0,'revenue_real':0,
                            'lp_equity':0,'costo_ejecutado':0}
        por_fondo[f]['proyectos']      += 1
        por_fondo[f]['revenue_pf']     += p['kpis'].get('revenue_proforma',0)
        por_fondo[f]['revenue_real']   += p['kpis'].get('revenue_firmado',0)
        por_fondo[f]['lp_equity']      += p['kpis'].get('lp_equity',0)
        por_fondo[f]['costo_ejecutado']+= p['kpis'].get('costo_ejecutado',0)

    # Tabla de proyectos
    proyectos_tabla = [{
        'proyecto_id':    p['meta']['proyecto_id'],
        'nombre':         p['meta']['nombre'],
        'fondo':          p['meta']['fondo'],
        'tipo_activo':    p['meta']['tipo_activo'],
        'ciudad':         p['meta']['ciudad'],
        'status':         p['meta']['status'],
        'revenue_pf':     p['kpis'].get('revenue_proforma',0),
        'revenue_real':   p['kpis'].get('revenue_firmado',0),
        'pct_ventas':     p['kpis'].get('pct_revenue_firmado',0),
        'unidades':       f"{p['kpis'].get('unidades_firmadas',0)}/{p['kpis'].get('total_units',0)}",
        'pct_absorcion':  p['kpis'].get('pct_absorcion',0),
        'costo_ejec':     p['kpis'].get('costo_ejecutado',0),
        'pct_presup':     p['kpis'].get('pct_presupuesto_ejecutado',0),
        'avance_obra':    p['kpis'].get('avance_obra_pct',0),
        'lp_equity':      p['kpis'].get('lp_equity',0),
        'lp_recibido':    p['kpis'].get('lp_equity_recibido',0),
        'margen_pf':      p['kpis'].get('margen_bruto_proforma',0),
        'moneda':         p['meta']['moneda'],
    } for p in projects_data]

    return {
        'kpis': {
            'total_proyectos':      len(projects_data),
            'revenue_proforma':     total_rev_pf,
            'revenue_firmado':      total_rev_real,
            'pct_revenue':          round(total_rev_real/total_rev_pf*100,1) if total_rev_pf else 0,
            'ppto_total':           total_costo_pf,
            'costo_ejecutado':      total_costo_r,
            'pct_costos':           round(total_costo_r/total_costo_pf*100,1) if total_costo_pf else 0,
            'lp_equity_total':      total_lp_eq,
            'lp_equity_recibido':   total_lp_rec,
            'pct_lp_recibido':      round(total_lp_rec/total_lp_eq*100,1) if total_lp_eq else 0,
            'cobranza_total':       total_cobr,
            'total_unidades_pf':    total_uni_pf,
            'total_unidades_firm':  total_uni_firm,
            'pct_absorcion':        round(total_uni_firm/total_uni_pf*100,1) if total_uni_pf else 0,
            'margen_bruto_total':   total_margen_m,
            'pct_margen':           round(total_margen_m/total_rev_pf*100,1) if total_rev_pf else 0,
        },
        'por_fondo':       por_fondo,
        'proyectos':       proyectos_tabla,
    }

# ── Cache ──────────────────────────────────────────────────────────────────────
_cache = {}         # { 'portfolio': {...,ts}, 'PRY-002': {...,ts}, ... }
CACHE_TTL = 60

def _is_fresh(key):
    return key in _cache and (time.time() - _cache[key].get('_ts',0)) < CACHE_TTL

def _store(key, data):
    _cache[key] = {**data, '_ts': time.time()}

# ── Carga completa del portfolio ───────────────────────────────────────────────
def load_all():
    """Carga registry + todos los proyectos. Usa caché si está fresco."""
    if _is_fresh('_registry'):
        registry = _cache['_registry']['data']
    else:
        registry = load_registry()
        _cache['_registry'] = {'data': registry, '_ts': time.time()}

    # Cargar proyectos en paralelo
    def _load_one(meta):
        pid = meta['proyecto_id']
        if _is_fresh(pid):
            return _cache[pid]['data']
        data = load_project(meta)
        _store(pid, {'data': data})
        return data

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        projects_data = list(ex.map(_load_one, registry))

    portfolio = build_portfolio(projects_data)
    _store('portfolio', {'data': portfolio, 'projects': projects_data})
    return portfolio, projects_data, registry

# ── Rutas Flask ────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return send_from_directory(DASHBOARD_DIR, 'dashboard.html')

@app.route('/api/health')
def health():
    return jsonify({'status':'ok','port':PORT,'source':'sheets',
                    'ts':datetime.now().isoformat(),'version':'multi-proyecto'})

@app.route('/api/projects')
def projects():
    """Lista de proyectos del registry."""
    try:
        _, _, registry = load_all()
        return jsonify(registry)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/portfolio')
def portfolio():
    """Consolidado de todos los proyectos."""
    try:
        port, _, _ = load_all()
        return jsonify(port)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/proyecto/<proyecto_id>')
def proyecto(proyecto_id):
    """Datos completos de un proyecto específico."""
    try:
        _, projects_data, _ = load_all()
        for p in projects_data:
            if p['meta']['proyecto_id'] == proyecto_id:
                return jsonify(p)
        return jsonify({'error': f'Proyecto {proyecto_id} no encontrado'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/refresh', methods=['POST'])
def refresh():
    """Invalida el cache completo y recarga todo."""
    _cache.clear()
    try:
        load_all()
        return jsonify({'ok': True, 'ts': datetime.now().isoformat()})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

# Rutas legacy — compatibilidad con el frontend anterior
@app.route('/api/data')
def data_legacy():
    """Ruta legacy → redirige al primer proyecto para compatibilidad."""
    try:
        _, projects_data, _ = load_all()
        if projects_data:
            return jsonify(projects_data[0])
        return jsonify({'error': 'Sin proyectos en el registry'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT, debug=False)
