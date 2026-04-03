#!/usr/bin/env python3
import io, os, threading, webbrowser, json, re, hashlib
import fitz
from flask import Flask, request, send_file, jsonify, session, redirect, Response
from database import get_db, h, init_db, migrate_from_files, DATA_DIR

BASE         = os.path.dirname(os.path.abspath(__file__))
IPTU_PDF     = os.path.join(BASE, 'template_iptu.pdf')
LUZ_PDF      = os.path.join(BASE, 'template_luz.pdf')
HTML_APP     = os.path.join(BASE, 'gerador-contrato.html')
HTML_ADMIN   = os.path.join(BASE, 'admin.html')
LOGO         = os.path.join(BASE, 'logolitorano.png')
IMOVEIS_DIR  = os.path.join(BASE, 'imoveis')

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'ltr_x9k2#p7m4@q8n1!v3z5_wRt')
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

# ─── AUTH ─────────────────────────────────────────────────────────────────────

PUBLIC = {'login', 'logo', 'static'}

@app.before_request
def check_auth():
    if request.endpoint in PUBLIC:
        return
    if not session.get('user_id'):
        if request.path.startswith('/api/') or request.path.startswith('/admin/api/'):
            return jsonify({'error': 'unauthorized'}), 401
        return redirect('/login')
    if request.path.startswith('/admin') and session.get('role') != 'admin':
        if request.path.startswith('/admin/api/'):
            return jsonify({'error': 'forbidden'}), 403
        return redirect('/')

def log_action(acao, detalhes=None):
    try:
        conn = get_db()
        conn.execute('INSERT INTO logs (user_id,user_nome,acao,detalhes,ip) VALUES (?,?,?,?,?)', (
            session.get('user_id'), session.get('username'), acao,
            json.dumps(detalhes) if detalhes else None,
            request.remote_addr
        ))
        conn.commit()
        conn.close()
    except Exception:
        pass

# ─── LOGIN PAGE ───────────────────────────────────────────────────────────────

_LOGIN_HTML = '''<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>LITORANO 1.0</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
body{background:#03030d;min-height:100vh;display:flex;align-items:center;justify-content:center;font-family:'Share Tech Mono',monospace;
background-image:linear-gradient(rgba(0,245,255,.02) 1px,transparent 1px),linear-gradient(90deg,rgba(0,245,255,.02) 1px,transparent 1px);background-size:44px 44px;}
.card{background:#07071a;border:1px solid rgba(0,245,255,.15);border-radius:16px;padding:48px 40px 40px;width:100%;max-width:380px;text-align:center;box-shadow:0 0 60px rgba(0,245,255,.06)}
.logo{margin-bottom:28px}.logo img{max-width:320px;height:auto}
.label{color:rgba(0,245,255,.5);font-size:10px;letter-spacing:2px;text-transform:uppercase;text-align:left;margin-bottom:6px;margin-top:20px}
input{width:100%;background:rgba(255,255,255,.04);border:1px solid rgba(0,245,255,.15);border-radius:8px;color:#e0e0f0;font-family:'Share Tech Mono',monospace;font-size:14px;padding:11px 14px;outline:none;transition:.2s}
input:focus{border-color:rgba(0,245,255,.45);box-shadow:0 0 0 3px rgba(0,245,255,.07)}
.btn{margin-top:28px;width:100%;background:linear-gradient(135deg,rgba(0,245,255,.15),rgba(0,245,255,.08));border:1px solid rgba(0,245,255,.3);border-radius:8px;color:#00f5ff;font-family:'Share Tech Mono',monospace;font-size:13px;letter-spacing:2px;padding:13px;cursor:pointer;transition:.2s;text-transform:uppercase}
.btn:hover{background:linear-gradient(135deg,rgba(0,245,255,.25),rgba(0,245,255,.15));border-color:rgba(0,245,255,.6)}
.erro{margin-top:16px;color:#ff2d78;font-size:11px;letter-spacing:1px;min-height:16px}
.versao{margin-top:28px;color:rgba(255,255,255,.12);font-size:9px;letter-spacing:2px}
@media(max-width:480px){.card{padding:36px 20px 32px;border-radius:12px}.logo img{max-width:220px}input{font-size:16px}}
</style></head><body>
<div class="card">
  <div class="logo"><img src="/logo" alt="LITORANO"></div>
  <form method="POST" action="/login" autocomplete="off">
    <div class="label">Login</div>
    <input type="text" name="u" autofocus autocomplete="off" spellcheck="false">
    <div class="label">Senha</div>
    <input type="password" name="p" autocomplete="off">
    <button class="btn" type="submit">Entrar</button>
    <div class="erro">{{ERRO}}</div>
  </form>
  <div class="versao">LITORANO 1.0 &mdash; SISTEMA PRIVADO</div>
</div></body></html>'''

@app.route('/login', methods=['GET', 'POST'])
def login():
    erro = ''
    if request.method == 'POST':
        u = request.form.get('u', '').strip()
        p = request.form.get('p', '')
        conn = get_db()
        user = conn.execute('SELECT * FROM users WHERE username=? AND ativo=1', (u,)).fetchone()
        conn.close()
        if user and user['pwd_hash'] == h(p):
            session.clear()
            session['user_id'] = user['id']
            session['username'] = user['username']
            session['role'] = user['role']
            conn2 = get_db()
            conn2.execute("UPDATE users SET ultimo_login=datetime('now','localtime') WHERE id=?", (user['id'],))
            conn2.commit(); conn2.close()
            log_action('login')
            return redirect('/admin' if user['role'] == 'admin' else '/')
        erro = 'Login ou senha incorretos.'
    return _LOGIN_HTML.replace('{{ERRO}}', erro), 200, {'Content-Type': 'text/html; charset=utf-8'}

@app.route('/logout')
def logout():
    log_action('logout')
    session.clear()
    return redirect('/login')

@app.route('/logo')
def logo():
    return send_file(LOGO, mimetype='image/png') if os.path.exists(LOGO) else ('', 404)

# ─── APP PRINCIPAL ────────────────────────────────────────────────────────────

@app.route('/')
def index():
    html = open(HTML_APP, encoding='utf-8').read()
    role = session.get('role', 'user')
    html = html.replace('{{USER_ROLE}}', role).replace('{{USERNAME}}', session.get('username', ''))
    return html, 200, {'Content-Type': 'text/html; charset=utf-8'}

@app.route('/api/imoveis')
def api_imoveis():
    conn = get_db()
    rows = conn.execute('''
        SELECT i.*, (SELECT f.id FROM fotos f WHERE f.imovel_id=i.id ORDER BY f.ordem LIMIT 1) as foto_id
        FROM imoveis i WHERE i.ativo=1 ORDER BY i.nome
    ''').fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/imovel/<int:iid>/copy')
def api_copy(iid):
    conn = get_db()
    row = conn.execute('SELECT copy_txt FROM imoveis WHERE id=?', (iid,)).fetchone()
    conn.close()
    if not row or not row['copy_txt']:
        return '', 404
    return row['copy_txt'], 200, {'Content-Type': 'text/plain; charset=utf-8'}

@app.route('/api/foto/<int:fid>')
def api_foto(fid):
    conn = get_db()
    row = conn.execute('SELECT dados, mime FROM fotos WHERE id=?', (fid,)).fetchone()
    conn.close()
    if not row:
        return '', 404
    return Response(bytes(row['dados']), mimetype=row['mime'])

@app.route('/api/funil')
def api_funil():
    conn = get_db()
    rows = conn.execute('SELECT id,ordem,label,mensagem FROM funil WHERE ativo=1 ORDER BY ordem').fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/config/ads')
def api_config_ads():
    conn = get_db()
    rows = conn.execute("SELECT chave,valor FROM config WHERE chave LIKE 'ads_%'").fetchall()
    conn.close()
    return jsonify({r['chave']: r['valor'] for r in rows})

@app.route('/api/log', methods=['POST'])
def api_log():
    d = request.json or {}
    log_action(d.get('acao', 'frontend'), d.get('detalhes'))
    return jsonify({'ok': True})

@app.route('/api/me')
def api_me():
    return jsonify({'username': session.get('username'), 'role': session.get('role')})

# ─── UTILITÁRIOS PDF ─────────────────────────────────────────────────────────

def fmt_brl(v):
    try:
        v = float(v or 0)
        s = f"{abs(v):,.2f}".replace(",","X").replace(".",",").replace("X",".")
        return f"R$ {s}"
    except: return "R$ 0,00"

def fmt_data(d):
    try:
        p = str(d).split('-'); return f"{p[2]}/{p[1]}/{p[0]}"
    except: return str(d)

def limpar(page, rect):
    for w in page.get_text("words", clip=rect):
        page.add_redact_annot(fitz.Rect(w[:4]), fill=None)

def buscar(page, texto, clip):
    rects = page.search_for(texto, clip=clip)
    for r in rects: page.add_redact_annot(r, fill=None)
    return rects

def aplicar(page):
    page.apply_redactions(images=fitz.PDF_REDACT_IMAGE_NONE, graphics=fitz.PDF_REDACT_LINE_ART_NONE)

def ins(page, x, y, texto, sz=7, bold=False, cor=(0,0,0)):
    if texto:
        page.insert_text((x,y), str(texto), fontsize=sz, fontname="hebo" if bold else "helv", color=cor)

# ─── GERADOR IPTU ─────────────────────────────────────────────────────────────

def editar_iptu(d):
    nome=( d.get('nome') or '').upper(); nome_tc=(d.get('nome') or '').title()
    cpf=d.get('cpf',''); exerc=d.get('exercicio','2025'); endereco=(d.get('endereco') or '').upper()
    cod_im=d.get('codImovel',''); guia=d.get('guia',''); parc=d.get('parcela','1')
    orig=float(d.get('valorOrig',0) or 0); multa=float(d.get('multa',0) or 0)
    juros=float(d.get('juros',0) or 0); total=orig+multa+juros
    venc=fmt_data(d.get('vencimento','')); pix=d.get('pix',''); barras=d.get('barras','')
    doc=fitz.open(IPTU_PDF); page=doc[0]; pendentes=[]
    for r in buscar(page,'2025',fitz.Rect(200,22,256,52)):
        pendentes.append((r.x0,r.y1-3,exerc,13,True,(0,0,0)))
    limpar(page,fitz.Rect(18,55,205,85))
    end_full=f"Imovel: {endereco}"
    if len(end_full)<=48: pendentes.append((22,67,end_full,7,False,(0,0,0)))
    else:
        corte=end_full[:48].rfind(' ');
        if corte<10: corte=48
        pendentes.append((22,67,end_full[:corte],7,False,(0,0,0)))
        pendentes.append((22,77,end_full[corte:].strip(),7,False,(0,0,0)))
    limpar(page,fitz.Rect(18,77,205,93)); pendentes.append((22,87,f"Emitido por: {cpf} - {nome_tc}",7,False,(0,0,0)))
    limpar(page,fitz.Rect(18,87,205,103)); pendentes.append((22,97,f"Proprietario:{nome}",7,False,(0,0,0)))
    limpar(page,fitz.Rect(200,77,252,92)); pendentes.append((206,86,venc,8,False,(0,0,0)))
    limpar(page,fitz.Rect(183,122,242,182))
    pendentes+= [(191,134,fmt_brl(orig),7,False,(0,0,0)),(191,144,"R$ 0,00",7,False,(0,0,0)),
                 (191,154,fmt_brl(multa),7,False,(0,0,0)),(191,164,fmt_brl(juros),7,False,(0,0,0)),
                 (191,174,fmt_brl(total),7,True,(0,0,0))]
    if pix:
        limpar(page,fitz.Rect(18,185,263,248)); y_p=198
        for chunk in [pix[i:i+55] for i in range(0,len(pix),55)][:4]:
            pendentes.append((21,y_p,chunk,8,False,(0,0,0))); y_p+=12
    for r in buscar(page,'2025',fitz.Rect(508,22,560,52)):
        pendentes.append((r.x0,r.y1-3,exerc,13,True,(0,0,0)))
    limpar(page,fitz.Rect(272,55,515,72))
    pendentes.append((278,66,f"Imovel:{cod_im}Guia:{guia} Emitidopor:{nome_tc}",7,False,(0,0,0)))
    limpar(page,fitz.Rect(315,68,562,84)); pendentes.append((317,78,nome,7,False,(0,0,0)))
    limpar(page,fitz.Rect(272,82,388,99)); pendentes.append((279,94,f"Parcela(s):{parc}",7,False,(0,0,0)))
    limpar(page,fitz.Rect(505,57,562,72)); pendentes.append((513,66,venc,8,False,(0,0,0)))
    def sub_val(txt_orig,novo,clip_r,bold=False):
        rs=buscar(page,txt_orig,fitz.Rect(*clip_r))
        if rs:
            for r in rs: pendentes.append((r.x0,r.y1-2,novo,7,bold,(0,0,0)))
        else:
            limpar(page,fitz.Rect(*clip_r)); pendentes.append((clip_r[0]+9,clip_r[3]-2,novo,7,bold,(0,0,0)))
    sub_val('R$ 1.416,87',fmt_brl(orig),[492,78,557,97])
    sub_val('R$ 0,00','R$ 0,00',[492,87,542,107])
    sub_val('R$ 24,30',fmt_brl(multa),[492,100,545,119])
    sub_val('R$ 21,25',fmt_brl(juros),[492,110,545,129])
    sub_val('R$ 1.462,42',fmt_brl(total),[492,119,557,139],bold=True)
    if barras:
        limpar(page,fitz.Rect(305,205,562,223)); pendentes.append((312,218,barras,8,False,(0,0,0)))
    aplicar(page)
    for (x,y,t,sz,bold,cor) in pendentes: ins(page,x,y,t,sz,bold,cor)
    buf=io.BytesIO(); doc.save(buf); doc.close(); buf.seek(0); return buf

# ─── GERADOR CONTA DE LUZ ─────────────────────────────────────────────────────

def editar_luz(d):
    nome=(d.get('nome') or ''); endereco=(d.get('endereco') or '')
    cep=d.get('cep',''); cpf=d.get('cpf',''); codigo=d.get('codigo','')
    fatura=d.get('fatura',''); mes_ref=d.get('mesRef','')
    consumo=int(d.get('consumo',0) or 0); venc=fmt_data(d.get('vencimento',''))
    pix=d.get('pix',''); barras=d.get('barras','')
    energia=float(d.get('energia',0) or 0); distrib=float(d.get('distrib',0) or 0)
    transm=float(d.get('transm',0) or 0); encargos=float(d.get('encargos',0) or 0)
    tributos=float(d.get('tributos',0) or 0); perdas=float(d.get('perdas',0) or 0)
    total=energia+distrib+transm+encargos+tributos+perdas
    doc=fitz.open(LUZ_PDF); page=doc[0]; pendentes=[]
    def sub(txt_orig,novo,clip_r,sz=8,bold=False):
        rs=buscar(page,txt_orig,fitz.Rect(*clip_r))
        if rs:
            for r in rs: pendentes.append((r.x0,r.y1-2,novo,sz,bold,(0,0,0)))
        else:
            limpar(page,fitz.Rect(*clip_r)); pendentes.append((clip_r[0]+1,clip_r[3]-2,novo,sz,bold,(0,0,0)))
    rs_cod=buscar(page,'421774',fitz.Rect(248,28,340,68))
    if rs_cod:
        for r in rs_cod: pendentes.append((r.x0,r.y1-5,codigo,20,True,(0,0,0)))
    else:
        limpar(page,fitz.Rect(248,28,340,68)); pendentes.append((259,60,codigo,20,True,(0,0,0)))
    limpar(page,fitz.Rect(48,74,365,96)); pendentes.append((52,90,nome,8,False,(0,0,0)))
    limpar(page,fitz.Rect(48,88,365,116))
    end_cep=endereco+(f" - CEP {cep}" if cep else ""); pendentes.append((52,100,end_cep,8,False,(0,0,0)))
    limpar(page,fitz.Rect(88,105,165,123)); pendentes.append((96,118,cpf,8,False,(0,0,0)))
    if fatura:
        limpar(page,fitz.Rect(462,105,563,123)); pendentes.append((470,117,fatura,8,False,(0,0,0)))
    sub('Dezembro/2025',mes_ref,[288,133,388,157],sz=10,bold=True)
    sub('28/01/2026',venc,[385,133,460,157],sz=10,bold=True)
    sub('R$ 258,83',fmt_brl(total),[497,130,565,157],sz=10,bold=True)
    for (orig_v,novo_v,clip_r) in [
        ('R$ 50,70',energia,[123,424,175,440]),('R$ 41,89',distrib,[123,439,175,455]),
        ('R$ 25,31',transm,[123,454,175,470]),('R$ 60,83',encargos,[247,424,298,440]),
        ('R$ 53,45',tributos,[247,439,298,455]),('R$ 11,96',perdas,[247,454,298,470])]:
        sub(orig_v,fmt_brl(novo_v),clip_r,sz=7)
    rs_s=buscar(page,'421774',fitz.Rect(58,730,100,752))
    if rs_s:
        for r in rs_s: pendentes.append((r.x0,r.y1-2,codigo,7,True,(0,0,0)))
    else:
        limpar(page,fitz.Rect(58,730,100,752)); pendentes.append((65,746,codigo,7,True,(0,0,0)))
    if fatura:
        limpar(page,fitz.Rect(135,730,232,752)); pendentes.append((144,746,fatura,7,True,(0,0,0)))
    sub('28/01/2026',venc,[407,730,462,752],sz=7,bold=True)
    sub('R$ 258,83',fmt_brl(total),[510,730,565,752],sz=7,bold=True)
    if barras:
        limpar(page,fitz.Rect(48,762,296,782)); pendentes.append((55,777,barras,8,True,(0,0,0)))
    if pix:
        limpar(page,fitz.Rect(470,762,590,840)); y_p=775
        for chunk in [pix[i:i+18] for i in range(0,len(pix),18)][:8]:
            pendentes.append((474,y_p,chunk,5,False,(0,0,0))); y_p+=7
    aplicar(page)
    for (x,y,t,sz,bold,cor) in pendentes: ins(page,x,y,t,sz,bold,cor)
    if doc.page_count>1:
        p2=doc[1]; p2_pend=[]
        def l2(rect):
            for w in p2.get_text("words",clip=rect): p2.add_redact_annot(fitz.Rect(w[:4]),fill=None)
        def b2(texto,clip):
            rects=p2.search_for(texto,clip=clip)
            for r in rects: p2.add_redact_annot(r,fill=None)
            return rects
        rs=b2('421774',fitz.Rect(435,368,472,386))
        if rs:
            for r in rs: p2_pend.append((r.x0,r.y1-1,codigo,6,False,(0,0,0)))
        else:
            l2(fitz.Rect(435,368,472,386)); p2_pend.append((443,381,codigo,6,False,(0,0,0)))
        l2(fitz.Rect(425,376,472,394)); p2_pend.append((433,388,venc,6,False,(0,0,0)))
        l2(fitz.Rect(125,420,380,436)); p2_pend.append((133,433,nome,8,False,(0,0,0)))
        l2(fitz.Rect(125,430,380,447)); p2_pend.append((133,443,endereco,8,False,(0,0,0)))
        l2(fitz.Rect(125,441,380,457))
        end2=f"Ubatuba - SP - {cep}" if cep else 'Ubatuba - SP'
        p2_pend.append((133,452,end2,8,False,(0,0,0)))
        p2.apply_redactions(images=fitz.PDF_REDACT_IMAGE_NONE,graphics=fitz.PDF_REDACT_LINE_ART_NONE)
        for (x,y,t,sz,bold,cor) in p2_pend: ins(p2,x,y,t,sz,bold,cor)
    buf=io.BytesIO(); doc.save(buf); doc.close(); buf.seek(0); return buf

# ─── ROTAS PDF ────────────────────────────────────────────────────────────────

@app.route('/api/gerar-iptu', methods=['POST'])
def api_iptu():
    try:
        d = request.json or {}
        buf = editar_iptu(d)
        log_action('gerar_iptu', {'nome': d.get('nome'), 'imovel_id': d.get('imovel_id')})
        return send_file(buf, mimetype='application/pdf', download_name='iptu.pdf', as_attachment=False)
    except Exception as e:
        import traceback; return traceback.format_exc(), 500

@app.route('/api/gerar-luz', methods=['POST'])
def api_luz():
    try:
        d = request.json or {}
        buf = editar_luz(d)
        log_action('gerar_luz', {'nome': d.get('nome'), 'imovel_id': d.get('imovel_id')})
        return send_file(buf, mimetype='application/pdf', download_name='luz.pdf', as_attachment=False)
    except Exception as e:
        import traceback; return traceback.format_exc(), 500

# ─── ADMIN HTML ───────────────────────────────────────────────────────────────

@app.route('/admin')
@app.route('/admin/')
def admin():
    return open(HTML_ADMIN, encoding='utf-8').read(), 200, {'Content-Type': 'text/html; charset=utf-8'}

# ─── ADMIN API — STATS ────────────────────────────────────────────────────────

@app.route('/admin/api/stats')
def admin_stats():
    conn = get_db()
    stats = {
        'total_imoveis':  conn.execute('SELECT COUNT(*) FROM imoveis').fetchone()[0],
        'imoveis_ativos': conn.execute('SELECT COUNT(*) FROM imoveis WHERE ativo=1').fetchone()[0],
        'total_users':    conn.execute('SELECT COUNT(*) FROM users').fetchone()[0],
        'users_ativos':   conn.execute('SELECT COUNT(*) FROM users WHERE ativo=1').fetchone()[0],
        'logs_hoje':      conn.execute("SELECT COUNT(*) FROM logs WHERE criado_em >= date('now','localtime')").fetchone()[0],
        'logs_total':     conn.execute('SELECT COUNT(*) FROM logs').fetchone()[0],
    }
    logs = conn.execute('SELECT * FROM logs ORDER BY id DESC LIMIT 15').fetchall()
    conn.close()
    return jsonify({'stats': stats, 'logs': [dict(r) for r in logs]})

# ─── ADMIN API — USUÁRIOS ─────────────────────────────────────────────────────

@app.route('/admin/api/users', methods=['GET'])
def admin_users_list():
    conn = get_db()
    rows = conn.execute('SELECT id,username,role,ativo,criado_em,ultimo_login FROM users ORDER BY id').fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/admin/api/users', methods=['POST'])
def admin_users_create():
    d = request.json or {}
    username = d.get('username','').strip()
    senha = d.get('senha','')
    role = d.get('role','user')
    if not username or not senha:
        return jsonify({'error': 'Login e senha obrigatórios'}), 400
    if len(senha) < 6:
        return jsonify({'error': 'Senha mínimo 6 caracteres'}), 400
    if role not in ('admin','user'):
        return jsonify({'error': 'Role inválido'}), 400
    conn = get_db()
    try:
        conn.execute('INSERT INTO users (username,pwd_hash,role) VALUES (?,?,?)', (username, h(senha), role))
        conn.commit()
        log_action('admin_criar_user', {'username': username, 'role': role})
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': 'Login já existe'}), 409
    finally:
        conn.close()

@app.route('/admin/api/users/<int:uid>', methods=['PUT'])
def admin_users_edit(uid):
    d = request.json or {}
    conn = get_db()
    conn.execute('UPDATE users SET username=?, role=? WHERE id=?', (d.get('username'), d.get('role'), uid))
    conn.commit(); conn.close()
    log_action('admin_editar_user', {'user_id': uid})
    return jsonify({'ok': True})

@app.route('/admin/api/users/<int:uid>/senha', methods=['PUT'])
def admin_users_senha(uid):
    senha = (request.json or {}).get('senha','')
    if len(senha) < 6:
        return jsonify({'error': 'Mínimo 6 caracteres'}), 400
    conn = get_db()
    conn.execute('UPDATE users SET pwd_hash=? WHERE id=?', (h(senha), uid))
    conn.commit(); conn.close()
    log_action('admin_trocar_senha', {'user_id': uid})
    return jsonify({'ok': True})

@app.route('/admin/api/users/<int:uid>/toggle', methods=['PUT'])
def admin_users_toggle(uid):
    if uid == session.get('user_id'):
        return jsonify({'error': 'Não é possível desativar o próprio usuário'}), 400
    conn = get_db()
    admins_ativos = conn.execute("SELECT COUNT(*) FROM users WHERE role='admin' AND ativo=1").fetchone()[0]
    user = conn.execute('SELECT role,ativo FROM users WHERE id=?', (uid,)).fetchone()
    if user and user['role'] == 'admin' and user['ativo'] == 1 and admins_ativos <= 1:
        conn.close()
        return jsonify({'error': 'Deve existir ao menos 1 admin ativo'}), 400
    conn.execute('UPDATE users SET ativo = CASE WHEN ativo=1 THEN 0 ELSE 1 END WHERE id=?', (uid,))
    conn.commit(); conn.close()
    log_action('admin_toggle_user', {'user_id': uid})
    return jsonify({'ok': True})

@app.route('/admin/api/users/<int:uid>', methods=['DELETE'])
def admin_users_delete(uid):
    if uid == session.get('user_id'):
        return jsonify({'error': 'Não é possível excluir o próprio usuário'}), 400
    conn = get_db()
    admins_ativos = conn.execute("SELECT COUNT(*) FROM users WHERE role='admin' AND ativo=1").fetchone()[0]
    user = conn.execute('SELECT role FROM users WHERE id=?', (uid,)).fetchone()
    if user and user['role'] == 'admin' and admins_ativos <= 1:
        conn.close()
        return jsonify({'error': 'Deve existir ao menos 1 admin'}), 400
    conn.execute('DELETE FROM users WHERE id=?', (uid,))
    conn.commit(); conn.close()
    log_action('admin_excluir_user', {'user_id': uid})
    return jsonify({'ok': True})

# ─── ADMIN API — IMÓVEIS ──────────────────────────────────────────────────────

def _slug(nome):
    import unicodedata
    s = unicodedata.normalize('NFKD', nome).encode('ascii','ignore').decode()
    return re.sub(r'[^a-z0-9]+', '-', s.lower()).strip('-')

@app.route('/admin/api/imoveis', methods=['GET'])
def admin_imoveis_list():
    conn = get_db()
    rows = conn.execute('''
        SELECT i.*, (SELECT f.id FROM fotos f WHERE f.imovel_id=i.id ORDER BY f.ordem LIMIT 1) as foto_id
        FROM imoveis i ORDER BY i.nome
    ''').fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/admin/api/imoveis', methods=['POST'])
def admin_imoveis_create():
    d = request.json or {}
    nome = d.get('nome','').strip()
    if not nome:
        return jsonify({'error': 'Nome obrigatório'}), 400
    slug = _slug(nome)
    conn = get_db()
    if conn.execute('SELECT id FROM imoveis WHERE slug=?', (slug,)).fetchone():
        slug = slug + '-' + str(conn.execute('SELECT COUNT(*) FROM imoveis').fetchone()[0])
    conn.execute('''INSERT INTO imoveis
        (slug,nome,endereco,cep,cidade,estado,cod_imovel,quartos,banheiros,area,mobiliado,
         destaque1,destaque2,destaque3,descricao,copy_txt)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (
        slug, nome, d.get('endereco',''), d.get('cep',''), d.get('cidade','Ubatuba'),
        d.get('estado','SP'), d.get('cod_imovel',''), d.get('quartos',''), d.get('banheiros',''),
        d.get('area',''), d.get('mobiliado','Sim'), d.get('destaque1',''), d.get('destaque2',''),
        d.get('destaque3',''), d.get('descricao',''), d.get('copy_txt',''),
    ))
    new_id = conn.lastrowid
    conn.commit(); conn.close()
    log_action('admin_criar_imovel', {'nome': nome, 'id': new_id})
    return jsonify({'ok': True, 'id': new_id})

@app.route('/admin/api/imoveis/<int:iid>', methods=['GET'])
def admin_imovel_get(iid):
    conn = get_db()
    row = conn.execute('SELECT * FROM imoveis WHERE id=?', (iid,)).fetchone()
    conn.close()
    return jsonify(dict(row)) if row else ('', 404)

@app.route('/admin/api/imoveis/<int:iid>', methods=['PUT'])
def admin_imoveis_edit(iid):
    d = request.json or {}
    conn = get_db()
    conn.execute('''UPDATE imoveis SET nome=?,endereco=?,cep=?,cidade=?,estado=?,cod_imovel=?,
        quartos=?,banheiros=?,area=?,mobiliado=?,destaque1=?,destaque2=?,destaque3=?,
        descricao=?,copy_txt=?,atualizado_em=datetime('now','localtime') WHERE id=?''', (
        d.get('nome'), d.get('endereco'), d.get('cep'), d.get('cidade'), d.get('estado'),
        d.get('cod_imovel'), d.get('quartos'), d.get('banheiros'), d.get('area'), d.get('mobiliado'),
        d.get('destaque1'), d.get('destaque2'), d.get('destaque3'), d.get('descricao'),
        d.get('copy_txt'), iid
    ))
    conn.commit(); conn.close()
    log_action('admin_editar_imovel', {'id': iid})
    return jsonify({'ok': True})

@app.route('/admin/api/imoveis/<int:iid>/toggle', methods=['PUT'])
def admin_imoveis_toggle(iid):
    conn = get_db()
    conn.execute('UPDATE imoveis SET ativo = CASE WHEN ativo=1 THEN 0 ELSE 1 END WHERE id=?', (iid,))
    conn.commit(); conn.close()
    log_action('admin_toggle_imovel', {'id': iid})
    return jsonify({'ok': True})

@app.route('/admin/api/imoveis/<int:iid>', methods=['DELETE'])
def admin_imoveis_delete(iid):
    conn = get_db()
    conn.execute('DELETE FROM imoveis WHERE id=?', (iid,))
    conn.commit(); conn.close()
    log_action('admin_excluir_imovel', {'id': iid})
    return jsonify({'ok': True})

@app.route('/admin/api/imoveis/<int:iid>/fotos', methods=['GET'])
def admin_fotos_list(iid):
    conn = get_db()
    rows = conn.execute('SELECT id,nome_orig,mime,ordem,criado_em FROM fotos WHERE imovel_id=? ORDER BY ordem', (iid,)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/admin/api/imoveis/<int:iid>/fotos', methods=['POST'])
def admin_fotos_upload(iid):
    files = request.files.getlist('fotos')
    if not files:
        return jsonify({'error': 'Nenhum arquivo'}), 400
    conn = get_db()
    max_ordem = conn.execute('SELECT MAX(ordem) FROM fotos WHERE imovel_id=?', (iid,)).fetchone()[0] or -1
    count = 0
    for f in files:
        if f and f.filename:
            ext = f.filename.rsplit('.', 1)[-1].lower()
            mime = 'image/png' if ext == 'png' else 'image/jpeg'
            dados = f.read()
            max_ordem += 1
            conn.execute('INSERT INTO fotos (imovel_id,nome_orig,mime,dados,ordem) VALUES (?,?,?,?,?)',
                         (iid, f.filename, mime, dados, max_ordem))
            count += 1
    conn.commit(); conn.close()
    log_action('admin_upload_fotos', {'imovel_id': iid, 'count': count})
    return jsonify({'ok': True, 'count': count})

@app.route('/admin/api/fotos/<int:fid>', methods=['DELETE'])
def admin_fotos_delete(fid):
    conn = get_db()
    conn.execute('DELETE FROM fotos WHERE id=?', (fid,))
    conn.commit(); conn.close()
    log_action('admin_excluir_foto', {'foto_id': fid})
    return jsonify({'ok': True})

@app.route('/admin/api/fotos/<int:fid>/ordem', methods=['PUT'])
def admin_fotos_ordem(fid):
    ordem = (request.json or {}).get('ordem', 0)
    conn = get_db()
    conn.execute('UPDATE fotos SET ordem=? WHERE id=?', (ordem, fid))
    conn.commit(); conn.close()
    return jsonify({'ok': True})

# ─── ADMIN API — CONFIG ───────────────────────────────────────────────────────

@app.route('/admin/api/config', methods=['GET'])
def admin_config_get():
    conn = get_db()
    rows = conn.execute('SELECT chave,valor FROM config').fetchall()
    conn.close()
    return jsonify({r['chave']: r['valor'] for r in rows})

@app.route('/admin/api/config', methods=['PUT'])
def admin_config_set():
    d = request.json or {}
    conn = get_db()
    for k, v in d.items():
        conn.execute("INSERT OR REPLACE INTO config (chave,valor,atualizado_em) VALUES (?,?,datetime('now','localtime'))", (k, v))
    conn.commit(); conn.close()
    log_action('admin_atualizar_config')
    return jsonify({'ok': True})

# ─── ADMIN API — FUNIL ────────────────────────────────────────────────────────

@app.route('/admin/api/funil', methods=['GET'])
def admin_funil_get():
    conn = get_db()
    rows = conn.execute('SELECT * FROM funil ORDER BY ordem').fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/admin/api/funil', methods=['PUT'])
def admin_funil_set():
    items = request.json or []
    conn = get_db()
    for item in items:
        conn.execute('UPDATE funil SET label=?,mensagem=?,ativo=? WHERE id=?',
                     (item.get('label'), item.get('mensagem'), item.get('ativo', 1), item.get('id')))
    conn.commit(); conn.close()
    log_action('admin_atualizar_funil')
    return jsonify({'ok': True})

# ─── ADMIN API — LOGS ─────────────────────────────────────────────────────────

@app.route('/admin/api/logs')
def admin_logs():
    page = int(request.args.get('page', 1))
    per = int(request.args.get('per_page', 50))
    acao = request.args.get('acao', '')
    user_id = request.args.get('user_id', '')
    offset = (page - 1) * per
    where = '1=1'
    params = []
    if acao: where += ' AND acao=?'; params.append(acao)
    if user_id: where += ' AND user_id=?'; params.append(user_id)
    conn = get_db()
    total = conn.execute(f'SELECT COUNT(*) FROM logs WHERE {where}', params).fetchone()[0]
    rows = conn.execute(f'SELECT * FROM logs WHERE {where} ORDER BY id DESC LIMIT ? OFFSET ?',
                        params + [per, offset]).fetchall()
    conn.close()
    return jsonify({'items': [dict(r) for r in rows], 'total': total, 'page': page, 'pages': max(1,(total+per-1)//per)})

# ─── MAIN ─────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    init_db()
    n = migrate_from_files(IMOVEIS_DIR)
    if n: print(f'  {n} imóveis migrados para o banco de dados.')
    def _abrir():
        import time; time.sleep(1.5)
        webbrowser.open('http://localhost:5000')
    threading.Thread(target=_abrir, daemon=True).start()
    print("=" * 50)
    print("  LITORANO 1.0 — http://localhost:5000")
    print("  Admin       — http://localhost:5000/admin")
    print("  Ctrl+C para parar.")
    print("=" * 50)
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)
else:
    # Produção (gunicorn)
    init_db()
    migrate_from_files(IMOVEIS_DIR)
