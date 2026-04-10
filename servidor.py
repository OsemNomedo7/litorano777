#!/usr/bin/env python3
import io, os, threading, webbrowser, json, re, hashlib, datetime
import fitz
import urllib.request as _ureq, urllib.error as _uerr
from flask import Flask, request, send_file, jsonify, session, redirect, Response
from database import get_db, h, init_db, migrate_from_files, DATA_DIR

BASE         = os.path.dirname(os.path.abspath(__file__))
IPTU_PDF     = os.path.join(BASE, 'template_iptu.pdf')
LUZ_PDF      = os.path.join(BASE, 'template_luz.pdf')
HTML_APP     = os.path.join(BASE, 'gerador-contrato.html')
HTML_ADMIN   = os.path.join(BASE, 'admin.html')
HTML_PLANOS  = os.path.join(BASE, 'planos.html')
LOGO         = os.path.join(BASE, 'logolitorano.png')
IMOVEIS_DIR  = os.path.join(BASE, 'imoveis')

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'ltr_x9k2#p7m4@q8n1!v3z5_wRt')
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

# ─── META ADS CONFIG ──────────────────────────────────────────────────────────
# Valores padrão do env; podem ser sobrescritos pelo banco (admin → Configurações → Meta App)
_META_APP_ID_ENV     = os.environ.get('META_APP_ID', '')
_META_APP_SECRET_ENV = os.environ.get('META_APP_SECRET', '')
META_API_VER         = 'v19.0'

def _get_meta_app_creds():
    """Retorna (app_id, app_secret) lendo do banco, com fallback nas env vars."""
    try:
        conn = get_db()
        rows = conn.execute("SELECT chave,valor FROM config WHERE chave IN ('meta_app_id','meta_app_secret')").fetchall()
        conn.close()
        cfg = {r['chave']: r['valor'] for r in rows}
        app_id     = cfg.get('meta_app_id','').strip()     or _META_APP_ID_ENV
        app_secret = cfg.get('meta_app_secret','').strip() or _META_APP_SECRET_ENV
        return app_id, app_secret
    except Exception:
        return _META_APP_ID_ENV, _META_APP_SECRET_ENV

# ─── SIGILOPAY CONFIG ─────────────────────────────────────────────────────────
# Autenticação via headers: x-public-key e x-secret-key em todas as requisições
SIGILOPAY_PUBLIC_KEY     = os.environ.get('SIGILOPAY_PUBLIC_KEY', '')
SIGILOPAY_SECRET_KEY     = os.environ.get('SIGILOPAY_SECRET_KEY', '')
SIGILOPAY_API_URL        = os.environ.get('SIGILOPAY_API_URL', 'https://app.sigilopay.com.br/api/v1')
SIGILOPAY_WEBHOOK_SECRET = os.environ.get('SIGILOPAY_WEBHOOK_SECRET', '')
APP_BASE_URL             = os.environ.get('APP_BASE_URL', 'http://localhost:5000')
# MODO_TESTE=1 aprova pagamentos automaticamente sem chamar a API real
MODO_TESTE               = os.environ.get('MODO_TESTE', '1') == '1'

def _get_sigilopay_creds():
    """Lê chave pública e privada do banco (admin config), com fallback em env vars."""
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT chave, valor FROM config WHERE chave IN ('sigilopay_public_key','sigilopay_secret_key','sigilopay_api_url')"
        ).fetchall()
        conn.close()
        cfg     = {r['chave']: r['valor'] for r in rows}
        pub_key = cfg.get('sigilopay_public_key') or SIGILOPAY_PUBLIC_KEY
        sec_key = cfg.get('sigilopay_secret_key') or SIGILOPAY_SECRET_KEY
        api_url = cfg.get('sigilopay_api_url')    or SIGILOPAY_API_URL
        return pub_key, sec_key, api_url
    except Exception:
        return SIGILOPAY_PUBLIC_KEY, SIGILOPAY_SECRET_KEY, SIGILOPAY_API_URL

def sigilopay_criar_cobranca(valor_reais, descricao, nome, email, ref_id, phone=None, document=None):
    """Cria cobrança PIX via SigiloPay (auth por headers x-public-key / x-secret-key)."""
    import datetime as _dt
    pub_key, sec_key, api_url = _get_sigilopay_creds()
    if MODO_TESTE or not pub_key or not sec_key:
        return {
            'id': f'teste_{ref_id}',
            'pix_code': '00020126580014BR.GOV.BCB.PIX0136TESTE-SIGILOPAY-PIX-CODE-AQUI5204000053039865802BR5913LITORANO SAS6009SAO PAULO62070503***6304ABCD',
            'qr_code_base64': None,
            'qr_code_url': None,
            '_teste': True,
        }
    base_url = _get_app_base_url()
    due_date = (_dt.date.today() + _dt.timedelta(days=1)).strftime('%Y-%m-%d')
    payload = json.dumps({
        'identifier': str(ref_id),
        'amount': round(float(valor_reais), 2),   # reais (não centavos)
        'client': {
            'name': nome or 'Cliente',
            'email': email or '',
            'phone': phone or '(11) 99999-9999',
            'document': document or '000.000.000-00',
        },
        'products': [
            {'id': 'plano_litorano', 'name': descricao, 'quantity': 1, 'price': round(float(valor_reais), 2)}
        ],
        'dueDate': due_date,
        'callbackUrl': f'{base_url}/webhook/sigilopay',
    }).encode()
    req = _ureq.Request(
        f'{api_url}/gateway/pix/receive',
        data=payload,
        headers={
            'x-public-key': pub_key,
            'x-secret-key': sec_key,
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Origin': api_url.split('/api/')[0],
        },
    )
    print(f"[SIGILOPAY] chamando {api_url}/gateway/pix/receive com pub_key={pub_key[:8]}...")
    try:
        with _ureq.urlopen(req, timeout=30) as r:
            resp = json.loads(r.read())
    except _ureq.HTTPError as e:
        body = ''
        try: body = e.read().decode('utf-8', errors='replace')
        except Exception: pass
        print(f"[SIGILOPAY ERROR] HTTP {e.code}: {body[:600]}")
        raise Exception(f"SigiloPay {e.code}: {body[:300]}")
    print(f"[SIGILOPAY] resposta: {json.dumps(resp)[:400]}")
    pix = resp.get('pix') or {}
    return {
        'id': resp.get('transactionId') or resp.get('id'),
        'pix_code': pix.get('code'),
        'qr_code_base64': pix.get('base64'),
        'qr_code_url': pix.get('image'),
        '_raw': resp,
    }

# ─── AUTH ─────────────────────────────────────────────────────────────────────

PUBLIC = {
    'login', 'logo', 'static', 'api_debug_fotos', 'api_foto_fs', 'imovel_link',
    'planos_page', 'api_cadastro', 'webhook_sigilopay', 'api_planos_publicos',
    'politica_privacidade', 'exclusao_dados', 'robots_txt',
}
# Rotas que exigem login mas NÃO exigem assinatura ativa
SEM_ASSINATURA_OK = {'logout', 'api_assinar', 'api_minha_assinatura'}

def _tem_assinatura_ativa(user_id):
    conn = get_db()
    try:
        row = conn.execute('''
            SELECT id FROM assinaturas
            WHERE user_id=? AND status='ativa'
            AND (expira_em IS NULL OR expira_em > datetime('now','localtime'))
            ORDER BY id DESC LIMIT 1
        ''', (user_id,)).fetchone()
        return row is not None
    finally:
        conn.close()

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
    # Usuários comuns sem assinatura ativa → página de planos
    if (session.get('role') == 'user'
            and request.endpoint not in SEM_ASSINATURA_OK
            and not request.path.startswith('/admin')):
        if not _tem_assinatura_ativa(session.get('user_id')):
            if request.path.startswith('/api/'):
                return jsonify({'error': 'subscription_required', 'redirect': '/planos'}), 402
            return redirect('/planos')

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
.card{background:#07071a;border:1px solid rgba(0,245,255,.15);border-radius:16px;padding:40px 36px 36px;width:100%;max-width:400px;text-align:center;box-shadow:0 0 60px rgba(0,245,255,.06)}
.logo{margin-bottom:24px}.logo img{max-width:280px;height:auto}
.tabs{display:flex;gap:0;border:1px solid rgba(0,245,255,.15);border-radius:8px;margin-bottom:24px;overflow:hidden}
.tab{flex:1;padding:9px;cursor:pointer;font-family:'Share Tech Mono',monospace;font-size:11px;letter-spacing:1.5px;text-transform:uppercase;border:none;background:transparent;color:rgba(0,245,255,.4);transition:.2s}
.tab.active{background:rgba(0,245,255,.1);color:#00f5ff}
.f-label{color:rgba(0,245,255,.5);font-size:10px;letter-spacing:2px;text-transform:uppercase;text-align:left;margin-bottom:5px;margin-top:16px;display:block}
input{width:100%;background:rgba(255,255,255,.04);border:1px solid rgba(0,245,255,.15);border-radius:8px;color:#e0e0f0;font-family:'Share Tech Mono',monospace;font-size:14px;padding:10px 13px;outline:none;transition:.2s}
input:focus{border-color:rgba(0,245,255,.45);box-shadow:0 0 0 3px rgba(0,245,255,.07)}
.btn{margin-top:22px;width:100%;background:linear-gradient(135deg,rgba(0,245,255,.15),rgba(0,245,255,.08));border:1px solid rgba(0,245,255,.3);border-radius:8px;color:#00f5ff;font-family:'Share Tech Mono',monospace;font-size:13px;letter-spacing:2px;padding:13px;cursor:pointer;transition:.2s;text-transform:uppercase}
.btn:hover{background:linear-gradient(135deg,rgba(0,245,255,.25),rgba(0,245,255,.15));border-color:rgba(0,245,255,.6)}
.msg{margin-top:14px;font-size:11px;letter-spacing:1px;min-height:16px}
.msg.erro{color:#ff2d78}.msg.ok{color:#39ff14}
.versao{margin-top:24px;color:rgba(255,255,255,.12);font-size:9px;letter-spacing:2px}
@media(max-width:480px){.card{padding:32px 18px;border-radius:12px}.logo img{max-width:200px}input{font-size:16px}}
</style></head><body>
<div class="card">
  <div class="logo"><img src="/logo" alt="LITORANO"></div>
  <div class="tabs">
    <button class="tab active" onclick="setTab('login')">Entrar</button>
    <button class="tab" onclick="setTab('cadastro')">Criar Conta</button>
  </div>

  <!-- FORMULÁRIO LOGIN -->
  <div id="frm-login">
    <form method="POST" action="/login" autocomplete="off">
      <label class="f-label">Login</label>
      <input type="text" name="u" autofocus autocomplete="off" spellcheck="false">
      <label class="f-label">Senha</label>
      <input type="password" name="p" autocomplete="off">
      <button class="btn" type="submit">Entrar</button>
      <div class="msg erro">{{ERRO}}</div>
    </form>
  </div>

  <!-- FORMULÁRIO CADASTRO -->
  <div id="frm-cadastro" style="display:none">
    <label class="f-label">Nome</label>
    <input type="text" id="c-nome" placeholder="Seu nome completo" autocomplete="off">
    <label class="f-label">Login</label>
    <input type="text" id="c-user" placeholder="nome de usuário" autocomplete="off" spellcheck="false">
    <label class="f-label">E-mail</label>
    <input type="email" id="c-email" placeholder="seu@email.com" autocomplete="off">
    <label class="f-label">Senha</label>
    <input type="password" id="c-senha" placeholder="mínimo 6 caracteres">
    <label class="f-label">Confirmar Senha</label>
    <input type="password" id="c-confirm" placeholder="repita a senha">
    <label class="f-label">CPF</label>
    <input type="text" id="c-cpf" placeholder="000.000.000-00" autocomplete="off" maxlength="14" oninput="mascaraCPF(this)">
    <label class="f-label">Telefone</label>
    <input type="text" id="c-phone" placeholder="(11) 99999-9999" autocomplete="off" maxlength="15" oninput="mascaraTel(this)">
    <button class="btn" type="button" onclick="cadastrar()">Criar Conta</button>
    <div class="msg" id="c-msg"></div>
  </div>

  <div class="versao">LITORANO 1.0 &mdash; SISTEMA PRIVADO</div>
</div>
<script>
function setTab(t) {
  document.querySelectorAll('.tab').forEach((el,i)=>el.classList.toggle('active',i===(t==='login'?0:1)));
  document.getElementById('frm-login').style.display = t==='login'?'':'none';
  document.getElementById('frm-cadastro').style.display = t==='cadastro'?'':'none';
}
function mascaraCPF(el){
  let v=el.value.replace(/\D/g,'').slice(0,11);
  if(v.length>9) v=v.replace(/(\d{3})(\d{3})(\d{3})(\d+)/,'$1.$2.$3-$4');
  else if(v.length>6) v=v.replace(/(\d{3})(\d{3})(\d+)/,'$1.$2.$3');
  else if(v.length>3) v=v.replace(/(\d{3})(\d+)/,'$1.$2');
  el.value=v;
}
function mascaraTel(el){
  let v=el.value.replace(/\D/g,'').slice(0,11);
  if(v.length>10) v=v.replace(/(\d{2})(\d{5})(\d+)/,'($1) $2-$3');
  else if(v.length>6) v=v.replace(/(\d{2})(\d{4})(\d+)/,'($1) $2-$3');
  else if(v.length>2) v=v.replace(/(\d{2})(\d+)/,'($1) $2');
  el.value=v;
}
async function cadastrar() {
  const nome=document.getElementById('c-nome').value.trim();
  const user=document.getElementById('c-user').value.trim();
  const email=document.getElementById('c-email').value.trim();
  const senha=document.getElementById('c-senha').value;
  const confirm=document.getElementById('c-confirm').value;
  const cpf=document.getElementById('c-cpf').value.trim();
  const phone=document.getElementById('c-phone').value.trim();
  const msg=document.getElementById('c-msg');
  if(!nome||!user||!senha){msg.className='msg erro';msg.textContent='Preencha todos os campos.';return;}
  if(senha!==confirm){msg.className='msg erro';msg.textContent='As senhas não coincidem.';return;}
  if(senha.length<6){msg.className='msg erro';msg.textContent='Senha mínima: 6 caracteres.';return;}
  msg.className='msg';msg.textContent='Criando conta...';
  const r=await fetch('/api/cadastro',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({nome,username:user,email,senha,cpf,phone})});
  const d=await r.json();
  if(d.error){msg.className='msg erro';msg.textContent=d.error;return;}
  msg.className='msg ok';msg.textContent='Conta criada! Redirecionando...';
  setTimeout(()=>window.location='/planos',1200);
}
</script>
</body></html>'''

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

# ─── CADASTRO ─────────────────────────────────────────────────────────────────

@app.route('/api/cadastro', methods=['POST'])
def api_cadastro():
    d = request.json or {}
    username = (d.get('username') or '').strip()
    email    = (d.get('email') or '').strip()
    nome     = (d.get('nome') or '').strip()
    senha    = d.get('senha', '')
    cpf      = (d.get('cpf') or '').strip()
    phone    = (d.get('phone') or '').strip()
    if not username or not senha:
        return jsonify({'error': 'Login e senha obrigatórios'}), 400
    if len(senha) < 6:
        return jsonify({'error': 'Senha mínima: 6 caracteres'}), 400
    conn = get_db()
    try:
        conn.execute('INSERT INTO users (username,pwd_hash,role,email,cpf,phone) VALUES (?,?,?,?,?,?)',
                     (username, h(senha), 'user', email, cpf, phone))
        conn.commit()
        uid = conn.execute('SELECT id FROM users WHERE username=?', (username,)).fetchone()[0]
        # Auto-login após cadastro
        session.clear()
        session['user_id'] = uid
        session['username'] = username
        session['role'] = 'user'
        return jsonify({'ok': True})
    except Exception:
        return jsonify({'error': 'Este login já está em uso'}), 409
    finally:
        conn.close()

# ─── PÁGINA DE PLANOS ─────────────────────────────────────────────────────────

@app.route('/planos')
def planos_page():
    if os.path.exists(HTML_PLANOS):
        html = open(HTML_PLANOS, encoding='utf-8').read()
        html = html.replace('{{USERNAME}}', session.get('username', ''))
        return html, 200, {'Content-Type': 'text/html; charset=utf-8'}
    return 'Página de planos não encontrada', 404

@app.route('/api/planos-publicos')
def api_planos_publicos():
    """Lista planos ativos com preço — acessível sem assinatura."""
    conn = get_db()
    rows = conn.execute('SELECT id,nome,descricao,max_pdfs_mes,preco,tipo,checkout_url FROM planos WHERE ativo=1 ORDER BY preco').fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/api/minha-assinatura')
def api_minha_assinatura():
    """Retorna a assinatura ativa do usuário logado."""
    uid = session.get('user_id')
    conn = get_db()
    row = conn.execute('''
        SELECT a.id, a.status, a.pago_em, a.expira_em, a.external_id,
               p.nome as plano_nome, p.max_pdfs_mes, p.preco
        FROM assinaturas a
        JOIN planos p ON p.id = a.plano_id
        WHERE a.user_id=?
        ORDER BY a.id DESC LIMIT 1
    ''', (uid,)).fetchone()
    conn.close()
    if not row:
        return jsonify({'status': 'none'})
    d = dict(row)
    d['ativa'] = (d['status'] == 'ativa' and
                  (not d['expira_em'] or d['expira_em'] > datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    return jsonify(d)

@app.route('/api/assinar', methods=['POST'])
def api_assinar():
    """Cria cobrança no gateway para o plano escolhido."""
    d = request.json or {}
    plano_id = d.get('plano_id')
    if not plano_id:
        return jsonify({'error': 'Plano não informado'}), 400
    uid  = session.get('user_id')
    conn = get_db()
    plano = conn.execute('SELECT * FROM planos WHERE id=? AND ativo=1', (plano_id,)).fetchone()
    if not plano:
        conn.close()
        return jsonify({'error': 'Plano inválido'}), 400
    user = conn.execute('SELECT username, email, phone, cpf FROM users WHERE id=?', (uid,)).fetchone()
    # Cria registro pendente
    cur_assn = conn.execute('INSERT INTO assinaturas (user_id,plano_id,status,valor) VALUES (?,?,?,?)',
                            (uid, plano_id, 'pendente', plano['preco']))
    assn_id = cur_assn.lastrowid
    conn.commit()
    def _expira_em(tipo):
        now = datetime.datetime.now()
        if tipo == 'semanal':    return (now + datetime.timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        if tipo == 'vitalicio':  return None
        return (now + datetime.timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')

    # Se o plano tem link de checkout, redireciona com o assn_id como referência
    checkout_url = (plano['checkout_url'] or '').strip()
    if checkout_url:
        sep = '&' if '?' in checkout_url else '?'
        url_final = f"{checkout_url}{sep}ref={assn_id}&client_ref={assn_id}"
        conn.commit(); conn.close()
        return jsonify({'ok': True, 'redirect': url_final})

    # Modo teste: aprova imediatamente (padrão) — só chama SigiloPay se MODO_TESTE=0 E chaves configuradas
    pub_key, sec_key, _ = _get_sigilopay_creds()
    usar_gateway = (not MODO_TESTE) and bool(pub_key.strip()) and bool(sec_key.strip())
    if not usar_gateway:
        expira = _expira_em(plano['tipo'])
        conn.execute('''UPDATE assinaturas SET status='ativa', external_id=?,
            pago_em=datetime('now','localtime'), expira_em=? WHERE id=?''',
            (f'teste_{assn_id}', expira, assn_id))
        conn.execute('UPDATE users SET plano_id=? WHERE id=?', (plano_id, uid))
        conn.commit(); conn.close()
        return jsonify({'ok': True, 'modo_teste': True, 'redirect': '/'})
    # Produção: cria cobrança real
    try:
        cobranca = sigilopay_criar_cobranca(
            valor_reais=plano['preco'],
            descricao=f'LITORANO — Plano {plano["nome"]}',
            nome=user['username'],
            email=user['email'] or '',
            ref_id=assn_id,
            phone=user['phone'] if user['phone'] else None,
            document=user['cpf'] if user['cpf'] else None,
        )
        conn.execute('UPDATE assinaturas SET external_id=? WHERE id=?', (cobranca['id'], assn_id))
        conn.commit(); conn.close()
        return jsonify({
            'ok': True,
            'assn_id': assn_id,
            'pix_code': cobranca.get('pix_code'),
            'qr_code_base64': cobranca.get('qr_code_base64'),
            'qr_code_url': cobranca.get('qr_code_url'),
        })
    except Exception as e:
        conn.execute('DELETE FROM assinaturas WHERE id=?', (assn_id,))
        conn.commit(); conn.close()
        return jsonify({'error': str(e)}), 500

# ─── WEBHOOK SIGILOPAY ────────────────────────────────────────────────────────

@app.route('/webhook/sigilopay', methods=['POST'])
def webhook_sigilopay():
    """Recebe notificações da SigiloPay (formato documentado: event + transaction)."""
    data = request.json or {}
    print(f"[WEBHOOK] payload recebido: {json.dumps(data)[:500]}")

    event       = data.get('event', '')
    transaction = data.get('transaction') or {}

    # Valida token do webhook se configurado
    wh_secret = ''
    try:
        conn_cfg = get_db()
        row = conn_cfg.execute("SELECT valor FROM config WHERE chave='webhook_secret'").fetchone()
        conn_cfg.close()
        if row: wh_secret = row['valor'] or ''
    except Exception:
        pass
    if wh_secret and data.get('token') != wh_secret:
        print(f"[WEBHOOK] token inválido recebido: {data.get('token')}")
        return jsonify({'ok': False, 'error': 'invalid token'}), 401

    # Só processa evento de pagamento confirmado
    pago = (event == 'TRANSACTION_PAID' or
            (transaction.get('status') or '').upper() == 'COMPLETED')
    if not pago:
        return jsonify({'ok': True, 'ignored': True, 'event': event})

    # transaction.id  = ID interno da SigiloPay
    # transaction.identifier = nosso ID passado na criação
    ext_id = transaction.get('id')
    ref_id = transaction.get('identifier')
    # email do cliente para fallback por checkout externo
    client_email = (data.get('client') or {}).get('email') or ''

    conn = get_db()
    assn = None
    if ext_id:
        assn = conn.execute('SELECT * FROM assinaturas WHERE external_id=?', (ext_id,)).fetchone()
    if not assn and ref_id:
        # ref_id pode ser o assn_id direto
        try:
            assn = conn.execute('SELECT * FROM assinaturas WHERE id=?', (int(ref_id),)).fetchone()
        except Exception:
            pass
    if not assn and client_email:
        # Fallback: assinatura pendente mais recente do usuário com esse email
        assn = conn.execute('''
            SELECT a.* FROM assinaturas a
            JOIN users u ON u.id = a.user_id
            WHERE u.email=? AND a.status='pendente'
            ORDER BY a.id DESC LIMIT 1
        ''', (client_email,)).fetchone()

    if assn and assn['status'] != 'ativa':
        plano_tipo = conn.execute('SELECT tipo FROM planos WHERE id=?', (assn['plano_id'],)).fetchone()
        tipo = plano_tipo['tipo'] if plano_tipo else 'mensal'
        now = datetime.datetime.now()
        if tipo == 'semanal':     expira = (now + datetime.timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        elif tipo == 'vitalicio': expira = None
        else:                     expira = (now + datetime.timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
        conn.execute('''UPDATE assinaturas SET status='ativa', external_id=?,
            pago_em=datetime('now','localtime'), expira_em=? WHERE id=?''',
            (ext_id or assn['external_id'], expira, assn['id']))
        conn.execute('UPDATE users SET plano_id=? WHERE id=?', (assn['plano_id'], assn['user_id']))
        conn.execute('INSERT INTO logs (user_id,user_nome,acao,detalhes,ip) VALUES (?,?,?,?,?)',
                     (assn['user_id'], 'webhook', 'pagamento_confirmado',
                      json.dumps({'assn_id': assn['id'], 'plano_id': assn['plano_id'],
                                  'gateway': 'sigilopay', 'ext_id': ext_id}),
                      request.remote_addr))
        conn.commit()
        print(f"[WEBHOOK] assinatura {assn['id']} ativada para user {assn['user_id']}")
    else:
        print(f"[WEBHOOK] assinatura não encontrada ou já ativa. ext_id={ext_id} ref_id={ref_id}")

    conn.close()
    return jsonify({'ok': True})

# ─── META ADS OAUTH ───────────────────────────────────────────────────────────

@app.route('/auth/meta')
def auth_meta():
    """Redireciona o usuário para o OAuth do Meta/Facebook."""
    meta_app_id, _ = _get_meta_app_creds()
    if not meta_app_id:
        return '''<!DOCTYPE html><html><head><meta charset="UTF-8">
        <style>body{background:#03030d;color:#ccd0f0;font-family:sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}
        .box{background:#07071a;border:1px solid rgba(0,245,255,.2);border-radius:14px;padding:32px;max-width:420px;text-align:center}
        h2{color:#00f5ff;font-size:16px;margin-bottom:12px}p{font-size:13px;color:#4a4a7a;line-height:1.6}
        a{display:inline-block;margin-top:20px;padding:10px 24px;background:rgba(0,245,255,.1);border:1px solid rgba(0,245,255,.3);border-radius:8px;color:#00f5ff;text-decoration:none;font-size:12px}</style>
        </head><body><div class="box">
        <h2>⚠️ Meta App não configurado</h2>
        <p>O administrador precisa configurar o <strong>App ID</strong> e o <strong>App Secret</strong> do Facebook.<br><br>
        Acesse o painel admin → <strong>Configurações → Meta App (OAuth)</strong> e insira as credenciais.</p>
        <a href="/">← Voltar</a></div></body></html>''', 500
    base_url = _get_app_base_url()
    callback = f'{base_url}/auth/meta/callback'
    scope = 'ads_management,ads_read,pages_show_list,pages_read_engagement'
    url = (f'https://www.facebook.com/dialog/oauth'
           f'?client_id={meta_app_id}'
           f'&redirect_uri={callback}'
           f'&scope={scope}'
           f'&state={session.get("user_id")}')
    return redirect(url)

@app.route('/auth/meta/callback')
def auth_meta_callback():
    """Recebe o código do Meta, troca por access_token e salva."""
    code = request.args.get('code')
    error = request.args.get('error')
    if error or not code:
        return redirect('/?meta_error=1')
    meta_app_id, meta_app_secret = _get_meta_app_creds()
    base_url = _get_app_base_url()
    callback = f'{base_url}/auth/meta/callback'
    token_url = (f'https://graph.facebook.com/{META_API_VER}/oauth/access_token'
                 f'?client_id={meta_app_id}&redirect_uri={callback}'
                 f'&client_secret={meta_app_secret}&code={code}')
    try:
        with _ureq.urlopen(token_url, timeout=15) as r:
            data = json.loads(r.read())
        access_token = data.get('access_token')
        expires_in   = data.get('expires_in', 0)
        expires_at   = None
        if expires_in:
            expires_at = (datetime.datetime.now() + datetime.timedelta(seconds=int(expires_in))).strftime('%Y-%m-%d %H:%M:%S')
        # Troca por token de longa duração
        lt_url = (f'https://graph.facebook.com/{META_API_VER}/oauth/access_token'
                  f'?grant_type=fb_exchange_token&client_id={meta_app_id}'
                  f'&client_secret={meta_app_secret}&fb_exchange_token={access_token}')
        try:
            with _ureq.urlopen(lt_url, timeout=15) as r2:
                lt = json.loads(r2.read())
            access_token = lt.get('access_token', access_token)
            lt_exp = lt.get('expires_in', 0)
            if lt_exp:
                expires_at = (datetime.datetime.now() + datetime.timedelta(seconds=int(lt_exp))).strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            pass
        conn = get_db()
        conn.execute('UPDATE users SET meta_access_token=?, meta_token_expires=? WHERE id=?',
                     (access_token, expires_at, session.get('user_id')))
        conn.commit(); conn.close()
        log_action('meta_conectado')
    except Exception as e:
        return redirect(f'/?meta_error={e}')
    return redirect('/?meta_ok=1')

@app.route('/api/meta/status')
def api_meta_status():
    """Retorna se o usuário tem Meta Ads conectado."""
    conn = get_db()
    row = conn.execute('SELECT meta_access_token, meta_token_expires, meta_ad_account_id FROM users WHERE id=?',
                       (session.get('user_id'),)).fetchone()
    conn.close()
    if not row or not row['meta_access_token']:
        return jsonify({'conectado': False})
    expirado = False
    if row['meta_token_expires']:
        expirado = row['meta_token_expires'] < datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return jsonify({
        'conectado': not expirado,
        'expira_em': row['meta_token_expires'],
        'ad_account_id': row['meta_ad_account_id'] or '',
    })

@app.route('/api/meta/desconectar', methods=['POST'])
def api_meta_desconectar():
    conn = get_db()
    conn.execute('UPDATE users SET meta_access_token=NULL, meta_token_expires=NULL, meta_ad_account_id=? WHERE id=?',
                 ('', session.get('user_id')))
    conn.commit(); conn.close()
    log_action('meta_desconectado')
    return jsonify({'ok': True})

@app.route('/api/meta/salvar-conta', methods=['POST'])
def api_meta_salvar_conta():
    """Salva o ID da conta de anúncios escolhida pelo usuário."""
    ad_account_id = (request.json or {}).get('ad_account_id', '').strip()
    conn = get_db()
    conn.execute('UPDATE users SET meta_ad_account_id=? WHERE id=?',
                 (ad_account_id, session.get('user_id')))
    conn.commit(); conn.close()
    return jsonify({'ok': True})

def _get_app_base_url():
    """Lê APP_BASE_URL do banco (admin → webhook config), com fallback na env var."""
    try:
        conn = get_db()
        row = conn.execute("SELECT valor FROM config WHERE chave='app_base_url'").fetchone()
        conn.close()
        return (row['valor'] or '').strip().rstrip('/') or APP_BASE_URL
    except Exception:
        return APP_BASE_URL

# ─── META MARKETING API — PROXY ───────────────────────────────────────────────

def _meta_token():
    conn = get_db()
    row = conn.execute('SELECT meta_access_token FROM users WHERE id=?', (session.get('user_id'),)).fetchone()
    conn.close()
    return row['meta_access_token'] if row else None

def _meta_account_id():
    conn = get_db()
    row = conn.execute('SELECT meta_ad_account_id FROM users WHERE id=?', (session.get('user_id'),)).fetchone()
    conn.close()
    aid = row['meta_ad_account_id'] if row else ''
    if aid and not aid.startswith('act_'):
        aid = 'act_' + aid
    return aid

def _meta_get(path, params=None):
    import urllib.parse
    token = _meta_token()
    if not token:
        raise Exception('Meta Ads não conectado')
    p = dict(params or {})
    p['access_token'] = token
    qs = urllib.parse.urlencode(p)
    url = f'https://graph.facebook.com/{META_API_VER}/{path}?{qs}'
    req = _ureq.Request(url)
    with _ureq.urlopen(req, timeout=20) as r:
        return json.loads(r.read())

def _meta_post(path, data):
    import urllib.parse
    token = _meta_token()
    if not token:
        raise Exception('Meta Ads não conectado')

    def _encode_val(v):
        """Converte valores para form-urlencoded compatível com Meta API."""
        if isinstance(v, bool):
            return 'true' if v else 'false'
        if isinstance(v, (dict, list)):
            return json.dumps(v)
        return v

    flat = {k: _encode_val(v) for k, v in data.items()}
    flat['access_token'] = token
    payload = urllib.parse.urlencode(flat).encode('utf-8')
    url = f'https://graph.facebook.com/{META_API_VER}/{path}'
    req = _ureq.Request(url, data=payload,
                        headers={'Content-Type': 'application/x-www-form-urlencoded'})
    print(f'[META POST] {path} keys={list(flat.keys())} payload={payload.decode()[:600]}')
    try:
        with _ureq.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except _uerr.HTTPError as e:
        body = ''
        try: body = e.read().decode('utf-8', errors='replace')
        except Exception: pass
        print(f'[META POST ERROR] {path} → HTTP {e.code}: {body[:1200]}')
        try:
            err_json = json.loads(body)
            err = err_json.get('error', {})
            msg = err.get('error_user_msg') or err.get('message', body[:400])
            details = f" | type={err.get('type')} code={err.get('code')} subcode={err.get('error_subcode')}"
        except Exception:
            msg = body[:400] or str(e)
            details = ''
        raise Exception(f'Meta API {e.code}: {msg}{details}')

@app.route('/api/meta/contas')
def api_meta_contas():
    """Lista as contas de anúncios do usuário."""
    try:
        data = _meta_get('me/adaccounts', {'fields': 'id,name,account_status,currency,amount_spent'})
        return jsonify(data.get('data', []))
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/meta/paginas')
def api_meta_paginas():
    """Lista as páginas do Facebook do usuário."""
    try:
        data = _meta_get('me/accounts', {'fields': 'id,name'})
        paginas = data.get('data', [])
        print(f'[META PAGINAS] encontradas: {len(paginas)} → {[p.get("name") for p in paginas]}')
        return jsonify(paginas)
    except Exception as e:
        print(f'[META PAGINAS ERROR] {e}')
        return jsonify({'error': str(e)}), 400

@app.route('/api/meta/campanhas')
def api_meta_campanhas():
    """Lista campanhas da conta de anúncios do usuário."""
    try:
        account = _meta_account_id()
        if not account:
            return jsonify({'error': 'Configure o ID da conta de anúncios primeiro'}), 400
        fields = 'id,name,status,objective,daily_budget,lifetime_budget,start_time,stop_time,created_time,insights{spend,impressions,clicks,actions}'
        data = _meta_get(f'{account}/campaigns', {'fields': fields, 'limit': 50})
        return jsonify(data.get('data', []))
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/meta/campanhas', methods=['POST'])
def api_meta_criar_campanha():
    """Cria Campanha + Ad Set + Ad Creative + Ad."""
    try:
        d = request.json or {}
        account = _meta_account_id()
        if not account:
            return jsonify({'error': 'Configure o ID da conta de anúncios primeiro'}), 400

        objetivo   = d.get('objetivo', 'MESSAGES')
        page_id    = d.get('page_id', '')
        formato    = d.get('formato', 'single')   # single | carousel
        print(f'[CAMP] objetivo={objetivo} page_id={repr(page_id)} formato={formato} fotos={d.get("fotos")} copy={repr(d.get("copy",""))[:60]}')

        # ── 1. CAMPANHA (com orçamento — CBO) ───────────────────────────────
        orcamento_centavos = str(int(float(d.get('orcamento', 30)) * 100))
        tipo_orcamento     = d.get('tipo_orcamento', 'daily')

        camp_data = {
            'name':                  d.get('nome', 'Campanha Litorano'),
            'objective':             objetivo,
            'status':                'PAUSED',
            'special_ad_categories': [],
            'bid_strategy':          'LOWEST_COST_WITHOUT_CAP',
        }
        if tipo_orcamento == 'daily':
            camp_data['daily_budget'] = orcamento_centavos
        else:
            camp_data['lifetime_budget'] = orcamento_centavos
            if d.get('data_fim'):
                camp_data['end_time'] = d['data_fim']
        if d.get('data_inicio'):
            camp_data['start_time'] = d['data_inicio']

        camp = _meta_post(f'{account}/campaigns', camp_data)
        camp_id = camp.get('id')
        if not camp_id:
            return jsonify({'error': f'Erro ao criar campanha: {camp}'}), 400

        # ── 2. AD SET ────────────────────────────────────────────────────────

        # Targeting
        genero_raw   = d.get('genero', '0')
        genders      = [int(genero_raw)] if genero_raw != '0' else []
        # Coordenadas lat/lng das capitais brasileiras
        _COORD = {
            'AC':(-9.9754,-67.8249),'AL':(-9.6658,-35.7353),'AP':(0.0349,-51.0694),
            'AM':(-3.1190,-60.0217),'BA':(-12.9714,-38.5014),'CE':(-3.7172,-38.5433),
            'DF':(-15.7801,-47.9292),'ES':(-20.3155,-40.3128),'GO':(-16.6864,-49.2643),
            'MA':(-2.5297,-44.3028),'MT':(-15.6014,-56.0979),'MS':(-20.4697,-54.6201),
            'MG':(-19.9167,-43.9345),'PA':(-1.4558,-48.5044),'PB':(-7.1195,-34.8450),
            'PR':(-25.4284,-49.2733),'PE':(-8.0476,-34.8770),'PI':(-5.0892,-42.8019),
            'RJ':(-22.9068,-43.1729),'RN':(-5.7793,-35.2009),'RS':(-30.0346,-51.2177),
            'RO':(-8.7612,-63.9004),'RR':(2.8235,-60.6758),'SC':(-27.5954,-48.5480),
            'SP':(-23.5505,-46.6333),'SE':(-10.9472,-37.0731),'TO':(-10.1837,-48.3336),
        }

        def _geocode(cidade, estado):
            """Geocodifica cidade via Nominatim (OSM). Retorna (lat, lng) ou None."""
            import urllib.parse as _up
            try:
                q   = _up.quote(f"{cidade}, {estado}, Brasil")
                url = f"https://nominatim.openstreetmap.org/search?q={q}&format=json&limit=1&countrycodes=br"
                req = _ureq.Request(url, headers={'User-Agent': 'LitoranoApp/1.0'})
                with _ureq.urlopen(req, timeout=8) as r:
                    res = json.loads(r.read())
                if res:
                    return float(res[0]['lat']), float(res[0]['lon'])
            except Exception as ge:
                print(f'[GEOCODE] {cidade}/{estado}: {ge}')
            return None, None

        localizacoes = d.get('localizacoes') or []
        if localizacoes:
            custom_locs = []
            for loc in localizacoes:
                cidade = (loc.get('cidade') or '').strip()
                estado = (loc.get('estado') or 'SP').strip().upper()
                raio   = int(loc.get('raio') or 30)
                MAX_RAIO = 80  # Meta aceita no máximo ~50 milhas (~80 km)
                if cidade:
                    lat, lng = _geocode(cidade, estado)
                    if not lat:
                        lat, lng = _COORD.get(estado, (-15.7801, -47.9292))
                else:
                    lat, lng = _COORD.get(estado, (-15.7801, -47.9292))
                    raio = MAX_RAIO  # estado: raio máximo ao redor da capital
                raio = min(raio, MAX_RAIO)
                custom_locs.append({'latitude': lat, 'longitude': lng,
                                    'radius': raio, 'distance_unit': 'kilometer'})
            geo = {'custom_locations': custom_locs}
        else:
            geo = {'countries': ['BR']}

        targeting_obj = {
            'geo_locations': geo,
            'age_min': int(d.get('idade_min', 18)),
            'age_max': int(d.get('idade_max', 65)),
            'targeting_automation': {'advantage_audience': 0},
        }
        if genders:
            targeting_obj['genders'] = genders

        # Posicionamentos manuais
        if not d.get('posicionamento_auto', True):
            pubs    = d.get('publisher_platforms') or ['facebook']
            fb_pos  = d.get('facebook_positions') or ['feed']
            ig_pos  = d.get('instagram_positions') or []
            an_pos  = d.get('audience_network_positions') or []
            ms_pos  = d.get('messenger_positions') or []
            targeting_obj['publisher_platforms'] = list(set(pubs))
            if fb_pos:  targeting_obj['facebook_positions']          = list(set(fb_pos))
            if ig_pos:  targeting_obj['instagram_positions']         = list(set(ig_pos))
            if an_pos:  targeting_obj['audience_network_positions']  = list(set(an_pos))
            if ms_pos:  targeting_obj['messenger_positions']         = list(set(ms_pos))

        adset_data = {
            'name':              d.get('adset_nome') or d.get('nome', 'Conjunto') + ' — Público',
            'campaign_id':       camp_id,
            'billing_event':     'IMPRESSIONS',
            'optimization_goal': 'REACH',
            'bid_strategy':      'LOWEST_COST_WITHOUT_CAP',
            'targeting':         targeting_obj,
            'status':            'PAUSED',
        }
        # Com orçamento vitalício na campanha, o adset também precisa de end_time
        if tipo_orcamento == 'lifetime':
            data_fim = d.get('data_fim')
            data_ini = d.get('data_inicio')
            if not data_fim:
                # Garante mínimo 48h a partir de agora se não foi informado
                import datetime
                data_fim = (datetime.datetime.utcnow() + datetime.timedelta(hours=48)).strftime('%Y-%m-%dT%H:%M:%S+0000')
            adset_data['end_time'] = data_fim
            if data_ini:
                adset_data['start_time'] = data_ini

        adset = _meta_post(f'{account}/adsets', adset_data)
        adset_id = adset.get('id')
        if not adset_id:
            return jsonify({'error': f'Erro ao criar conjunto: {adset}'}), 400

        # ── 3. AD CREATIVE (se tiver page_id e fotos) ────────────────────────
        ad_id = None
        print(f'[CREATIVE] page_id={repr(page_id)} fotos_count={len(d.get("fotos") or [])}')
        if page_id:
            fotos       = d.get('fotos') or []
            copy        = d.get('copy', '')
            headline    = d.get('headline', '')
            desc_ad     = d.get('descricao_ad', '')
            cta_type    = d.get('cta', 'LEARN_MORE')
            url_destino = d.get('url_destino', '')
            link_fallback = url_destino or 'https://litorano777.onrender.com'

            def _upload_imagem(foto_url):
                """Lê imagem do banco e faz upload para Meta via base64."""
                import base64, urllib.parse as _up
                print(f'[UPLOAD IMG] tentando: {foto_url}')
                try:
                    # Extrai slug/nome_orig da URL: /api/foto/fs/<slug>/<nome_orig>
                    parsed  = _up.urlparse(foto_url)
                    path_part = _up.unquote(parsed.path)
                    prefix  = '/api/foto/fs/'
                    if prefix not in path_part:
                        raise Exception(f'URL não reconhecida: {foto_url}')
                    rel       = path_part.split(prefix, 1)[1]   # slug/nome_orig
                    parts     = rel.split('/', 1)
                    slug      = parts[0]
                    nome_orig = parts[1] if len(parts) > 1 else ''
                    print(f'[UPLOAD IMG] buscando banco: slug={slug} nome={nome_orig}')
                    # Busca blob no banco pela junção imoveis.slug + fotos.nome_orig
                    _conn = get_db()
                    row = _conn.execute(
                        '''SELECT f.dados, f.mime FROM fotos f
                           JOIN imoveis i ON i.id = f.imovel_id
                           WHERE i.slug=? AND f.nome_orig=?
                           LIMIT 1''',
                        (slug, nome_orig)
                    ).fetchone()
                    _conn.close()
                    if not row or not row['dados']:
                        raise Exception(f'Foto não encontrada no banco: {slug}/{nome_orig}')
                    img_bytes = bytes(row['dados']) if isinstance(row['dados'], memoryview) else row['dados']
                    img_b64   = base64.b64encode(img_bytes).decode('utf-8')
                    print(f'[UPLOAD IMG] b64 size={len(img_b64)}, enviando para Meta...')
                    result = _meta_post(f'{account}/adimages', {'bytes': img_b64})
                    print(f'[UPLOAD IMG] resposta: {str(result)[:300]}')
                    images = result.get('images', {})
                    for key, val in images.items():
                        h = val.get('hash') or key
                        print(f'[UPLOAD IMG] hash={h}')
                        return h
                except Exception as ue:
                    print(f'[UPLOAD IMG ERROR] {foto_url}: {ue}')
                return None

            if formato == 'carousel' and len(fotos) >= 2:
                # Carrossel — faz upload de cada imagem e usa image_hash
                titulo_tpl = d.get('carousel_titulo', '{nome}')
                nome_imovel = d.get('imovel_nome', headline)
                cards = []
                for i, foto_url in enumerate(fotos[:10]):
                    titulo_card = titulo_tpl.replace('{n}', str(i+1)).replace('{nome}', nome_imovel) or headline
                    card = {'link': link_fallback, 'name': titulo_card}
                    if foto_url:
                        img_hash = _upload_imagem(foto_url)
                        if img_hash:
                            card['image_hash'] = img_hash
                        else:
                            card['picture'] = foto_url  # fallback para URL
                    cards.append(card)
                story_spec = {
                    'page_id': page_id,
                    'link_data': {
                        'link':               link_fallback,
                        'message':            copy,
                        'child_attachments':  cards,
                        'call_to_action':     {'type': cta_type},
                        'multi_share_optimized': True,
                    }
                }
            else:
                # Imagem única — faz upload e usa image_hash
                cta_value = {'link': link_fallback}
                if objetivo == 'OUTCOME_ENGAGEMENT' and d.get('whatsapp_phone'):
                    cta_value['whatsapp_number'] = d['whatsapp_phone']
                link_data = {
                    'link':        link_fallback,
                    'message':     copy,
                    'name':        headline,
                    'description': desc_ad,
                    'call_to_action': {'type': cta_type, 'value': cta_value},
                }
                if fotos:
                    img_hash = _upload_imagem(fotos[0])
                    if img_hash:
                        link_data['image_hash'] = img_hash
                    else:
                        link_data['picture'] = fotos[0]  # fallback para URL
                story_spec = {'page_id': page_id, 'link_data': link_data}

            print(f'[CREATIVE] story_spec page_id={page_id} keys={list(story_spec.get("link_data",story_spec).keys())}')
            creative = _meta_post(f'{account}/adcreatives', {
                'name':              f'Creative — {d.get("nome","Litorano")}',
                'object_story_spec': story_spec,
            })
            creative_id = creative.get('id')
            print(f'[CREATIVE] creative_id={creative_id}')

            if creative_id:
                ad = _meta_post(f'{account}/ads', {
                    'name':      f'Anúncio — {d.get("nome","Litorano")}',
                    'adset_id':  adset_id,
                    'creative':  {'creative_id': creative_id},
                    'status':    'PAUSED',
                })
                ad_id = ad.get('id')
                print(f'[AD] ad_id={ad_id}')

        log_action('meta_criar_campanha', {'camp_id': camp_id, 'nome': d.get('nome'), 'formato': formato})
        resp = {'ok': True, 'campaign_id': camp_id, 'adset_id': adset_id, 'ad_id': ad_id}
        if page_id and not ad_id:
            resp['aviso'] = 'Campanha e conjunto criados, mas o anúncio (criativo) não foi criado. Verifique os logs do servidor.'
        return jsonify(resp)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error': str(e)}), 400

@app.route('/api/meta/campanhas/<cid>/status', methods=['PUT'])
def api_meta_toggle_campanha(cid):
    """Pausa ou ativa uma campanha."""
    try:
        novo_status = (request.json or {}).get('status', 'PAUSED')
        if novo_status not in ('ACTIVE', 'PAUSED'):
            return jsonify({'error': 'Status inválido'}), 400
        result = _meta_post(cid, {'status': novo_status})
        log_action('meta_toggle_campanha', {'camp_id': cid, 'status': novo_status})
        return jsonify({'ok': True, 'result': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/meta/campanhas/<cid>/orcamento', methods=['PUT'])
def api_meta_orcamento_campanha(cid):
    """Atualiza orçamento de um ad set."""
    try:
        d = request.json or {}
        valor = str(int(float(d.get('valor', 30)) * 100))
        campo = 'daily_budget' if d.get('tipo') == 'daily' else 'lifetime_budget'
        result = _meta_post(cid, {campo: valor})
        return jsonify({'ok': True, 'result': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/meta/insights')
def api_meta_insights():
    """Retorna insights da conta nos últimos N dias."""
    try:
        account = _meta_account_id()
        if not account:
            return jsonify({'error': 'Configure o ID da conta de anúncios'}), 400
        period = request.args.get('period', 'last_7d')
        fields = 'spend,impressions,clicks,actions,cpc,cpm,reach'
        data = _meta_get(f'{account}/insights', {
            'fields': fields,
            'date_preset': period,
            'level': 'account',
        })
        return jsonify(data.get('data', []))
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# ─── ADMIN API — MINHA CONTA ─────────────────────────────────────────────────

@app.route('/admin/api/minha-conta', methods=['PUT'])
def admin_minha_conta():
    d = request.json or {}
    uid = session.get('user_id')
    updates = []
    params  = []
    if d.get('username', '').strip():
        updates.append('username=?'); params.append(d['username'].strip())
    if d.get('email', '').strip():
        updates.append('email=?'); params.append(d['email'].strip())
    if d.get('senha', '').strip():
        if len(d['senha']) < 6:
            return jsonify({'error': 'Senha mínimo 6 caracteres'}), 400
        updates.append('pwd_hash=?'); params.append(h(d['senha']))
    if not updates:
        return jsonify({'error': 'Nada para atualizar'}), 400
    params.append(uid)
    conn = get_db()
    conn.execute(f"UPDATE users SET {','.join(updates)} WHERE id=?", params)
    conn.commit(); conn.close()
    log_action('admin_update_minha_conta')
    # Se mudou username ou senha, força novo login
    logout_needed = bool(d.get('username') or d.get('senha'))
    if logout_needed:
        session.clear()
    return jsonify({'ok': True, 'logout': logout_needed})

# ─── ADMIN API — META APP CONFIG ─────────────────────────────────────────────

@app.route('/admin/api/meta-app-config', methods=['GET'])
def admin_meta_app_config_get():
    meta_app_id, meta_app_secret = _get_meta_app_creds()
    base_url = _get_app_base_url()
    return jsonify({
        'meta_app_id':     meta_app_id,
        'meta_app_secret': meta_app_secret,
        'app_base_url':    base_url,
        'callback_url':    f'{base_url}/auth/meta/callback',
    })

@app.route('/admin/api/meta-app-config', methods=['PUT'])
def admin_meta_app_config_set():
    d = request.json or {}
    allowed = {'meta_app_id', 'meta_app_secret', 'app_base_url'}
    conn = get_db()
    for k, v in d.items():
        if k in allowed:
            conn.execute("INSERT OR REPLACE INTO config (chave,valor,atualizado_em) VALUES (?,?,datetime('now','localtime'))", (k, str(v)))
    conn.commit(); conn.close()
    log_action('admin_meta_app_config')
    return jsonify({'ok': True})

# ─── ADMIN API — WEBHOOK CONFIG ───────────────────────────────────────────────

@app.route('/admin/api/webhook-config', methods=['GET'])
def admin_webhook_config_get():
    conn = get_db()
    rows = conn.execute("SELECT chave, valor FROM config WHERE chave IN ('webhook_secret','sigilopay_public_key','sigilopay_secret_key','sigilopay_api_url','app_base_url')").fetchall()
    conn.close()
    cfg = {r['chave']: r['valor'] for r in rows}
    base = cfg.get('app_base_url') or APP_BASE_URL
    cfg['webhook_url'] = f'{base}/webhook/sigilopay'
    return jsonify(cfg)

@app.route('/admin/api/webhook-config', methods=['PUT'])
def admin_webhook_config_set():
    d = request.json or {}
    allowed = {'webhook_secret', 'sigilopay_api_url', 'sigilopay_public_key', 'sigilopay_secret_key', 'app_base_url'}
    conn = get_db()
    for k, v in d.items():
        if k in allowed:
            conn.execute("INSERT OR REPLACE INTO config (chave,valor,atualizado_em) VALUES (?,?,datetime('now','localtime'))", (k, v))
    conn.commit(); conn.close()
    log_action('admin_webhook_config')
    return jsonify({'ok': True})

@app.route('/robots.txt')
def robots_txt():
    return 'User-agent: *\nAllow: /\n', 200, {'Content-Type': 'text/plain'}

@app.route('/logo')
def logo():
    return send_file(LOGO, mimetype='image/png') if os.path.exists(LOGO) else ('', 404)

@app.route('/politica-privacidade')
def politica_privacidade():
    return '''<!DOCTYPE html>
<html lang="pt-BR"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Política de Privacidade — LITORANO</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',sans-serif;background:#03030d;color:#ccd0f0;min-height:100vh;
  background-image:linear-gradient(rgba(0,245,255,.018) 1px,transparent 1px),linear-gradient(90deg,rgba(0,245,255,.018) 1px,transparent 1px);
  background-size:44px 44px}
.wrap{max-width:760px;margin:0 auto;padding:60px 24px}
.logo{text-align:center;margin-bottom:40px}
.logo img{height:40px}
h1{font-family:'Share Tech Mono',monospace;font-size:20px;color:#00f5ff;letter-spacing:2px;text-transform:uppercase;margin-bottom:8px}
.tag{font-family:'Share Tech Mono',monospace;font-size:10px;color:#4a4a7a;letter-spacing:2px;margin-bottom:40px}
h2{font-family:'Share Tech Mono',monospace;font-size:13px;color:#00f5ff;letter-spacing:1px;text-transform:uppercase;margin:32px 0 12px;border-left:3px solid #00f5ff;padding-left:12px}
p,li{font-size:14px;color:#9090b0;line-height:1.8;margin-bottom:8px}
ul{padding-left:20px;margin-bottom:8px}
a{color:#00f5ff;text-decoration:none}
.box{background:#07071a;border:1px solid rgba(0,245,255,.1);border-radius:12px;padding:24px;margin-top:40px;font-family:'Share Tech Mono',monospace;font-size:11px;color:#4a4a7a;text-align:center}
</style></head><body>
<div class="wrap">
  <div class="logo"><img src="/logo" alt="LITORANO"></div>
  <h1>Política de Privacidade</h1>
  <div class="tag">Última atualização: Abril de 2026</div>

  <h2>1. Quem somos</h2>
  <p>A plataforma <strong style="color:#ccd0f0">LITORANO</strong> é um sistema SaaS voltado para corretores e imobiliárias, oferecendo geração de documentos de locação e criação de campanhas no Meta Ads.</p>

  <h2>2. Dados que coletamos</h2>
  <ul>
    <li>Nome, e-mail e senha dos usuários cadastrados</li>
    <li>Informações de imóveis inseridas pelo próprio usuário</li>
    <li>Token de acesso ao Meta Ads (Facebook) fornecido voluntariamente pelo usuário</li>
    <li>Dados de campanhas e anúncios criados via a plataforma</li>
    <li>Logs de acesso e ações realizadas no sistema</li>
  </ul>

  <h2>3. Como usamos os dados</h2>
  <ul>
    <li>Autenticação e controle de acesso à plataforma</li>
    <li>Criação e gerenciamento de campanhas publicitárias no Meta Ads em nome do usuário</li>
    <li>Geração de documentos (contratos, boletos, laudos)</li>
    <li>Comunicação sobre a conta e suporte técnico</li>
  </ul>

  <h2>4. Uso da API do Meta (Facebook)</h2>
  <p>A LITORANO utiliza a API do Meta Ads para criar e gerenciar campanhas publicitárias. Ao conectar sua conta do Facebook, você autoriza o acesso às seguintes permissões:</p>
  <ul>
    <li><strong style="color:#ccd0f0">ads_management</strong> — criação e edição de campanhas, conjuntos e anúncios</li>
    <li><strong style="color:#ccd0f0">ads_read</strong> — leitura de campanhas e relatórios de desempenho</li>
    <li><strong style="color:#ccd0f0">pages_show_list</strong> — listagem das suas Páginas do Facebook</li>
    <li><strong style="color:#ccd0f0">pages_read_engagement</strong> — leitura básica das suas Páginas</li>
  </ul>
  <p>Essas permissões são usadas exclusivamente para criar anúncios em seu nome. Não compartilhamos seu token ou dados do Meta com terceiros.</p>

  <h2>5. Compartilhamento de dados</h2>
  <p>Não vendemos, alugamos ou compartilhamos seus dados pessoais com terceiros, exceto quando exigido por lei ou necessário para operar os serviços integrados (Meta Ads API, processador de pagamentos).</p>

  <h2>6. Segurança</h2>
  <p>Os dados são armazenados em servidores seguros. Os tokens de acesso ao Meta são armazenados de forma criptografada e associados exclusivamente à sua conta.</p>

  <h2>7. Seus direitos</h2>
  <ul>
    <li>Solicitar acesso, correção ou exclusão dos seus dados a qualquer momento</li>
    <li>Revogar o acesso ao Meta Ads desconectando sua conta na plataforma</li>
    <li>Encerrar sua conta e solicitar a remoção completa dos dados</li>
  </ul>

  <h2>8. Retenção de dados</h2>
  <p>Os dados são mantidos enquanto a conta estiver ativa. Após o cancelamento, os dados são excluídos em até 30 dias, exceto quando houver obrigação legal de retenção.</p>

  <h2>9. Contato</h2>
  <p>Para dúvidas ou solicitações relacionadas à privacidade, entre em contato pelo suporte da plataforma.</p>

  <div class="box">LITORANO SaaS · Todos os direitos reservados · <a href="/">litorano777.onrender.com</a></div>
</div>
</body></html>'''

@app.route('/exclusao-dados')
def exclusao_dados():
    return '''<!DOCTYPE html>
<html lang="pt-BR"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Exclusão de Dados — LITORANO</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',sans-serif;background:#03030d;color:#ccd0f0;min-height:100vh;
  background-image:linear-gradient(rgba(0,245,255,.018) 1px,transparent 1px),linear-gradient(90deg,rgba(0,245,255,.018) 1px,transparent 1px);
  background-size:44px 44px}
.wrap{max-width:760px;margin:0 auto;padding:60px 24px}
.logo{text-align:center;margin-bottom:40px}
.logo img{height:40px}
h1{font-family:'Share Tech Mono',monospace;font-size:20px;color:#00f5ff;letter-spacing:2px;text-transform:uppercase;margin-bottom:8px}
.tag{font-family:'Share Tech Mono',monospace;font-size:10px;color:#4a4a7a;letter-spacing:2px;margin-bottom:40px}
h2{font-family:'Share Tech Mono',monospace;font-size:13px;color:#00f5ff;letter-spacing:1px;text-transform:uppercase;margin:32px 0 12px;border-left:3px solid #00f5ff;padding-left:12px}
p,li{font-size:14px;color:#9090b0;line-height:1.8;margin-bottom:8px}
ul{padding-left:20px;margin-bottom:8px}
.step{background:#07071a;border:1px solid rgba(0,245,255,.1);border-radius:12px;padding:20px 24px;margin-bottom:12px;display:flex;gap:16px;align-items:flex-start}
.step-num{font-family:'Share Tech Mono',monospace;font-size:20px;color:#00f5ff;font-weight:700;flex-shrink:0;line-height:1}
.step-text{font-size:13px;color:#9090b0;line-height:1.7}
.step-text strong{color:#ccd0f0}
.box{background:#07071a;border:1px solid rgba(0,245,255,.1);border-radius:12px;padding:24px;margin-top:40px;font-family:'Share Tech Mono',monospace;font-size:11px;color:#4a4a7a;text-align:center}
a{color:#00f5ff;text-decoration:none}
</style></head><body>
<div class="wrap">
  <div class="logo"><img src="/logo" alt="LITORANO"></div>
  <h1>Exclusão de Dados</h1>
  <div class="tag">Instruções para remoção de dados do Facebook/Meta</div>

  <p>De acordo com a política do Meta (Facebook), você tem o direito de solicitar a exclusão de todos os dados que a plataforma <strong style="color:#ccd0f0">LITORANO</strong> coletou através da sua conta do Facebook.</p>

  <h2>Como solicitar a exclusão</h2>

  <div class="step">
    <div class="step-num">01</div>
    <div class="step-text"><strong>Acesse a plataforma LITORANO</strong><br>
    Faça login em <a href="/">litorano777.onrender.com</a> com sua conta.</div>
  </div>

  <div class="step">
    <div class="step-num">02</div>
    <div class="step-text"><strong>Desconecte o Meta Ads</strong><br>
    No painel principal, clique em <strong>Meta Ads → Desconectar conta</strong>. Isso revoga o token de acesso imediatamente.</div>
  </div>

  <div class="step">
    <div class="step-num">03</div>
    <div class="step-text"><strong>Solicite a exclusão da conta</strong><br>
    Entre em contato com o suporte da plataforma informando seu e-mail cadastrado e solicite a exclusão completa. Seus dados serão removidos em até <strong>30 dias</strong>.</div>
  </div>

  <div class="step">
    <div class="step-num">04</div>
    <div class="step-text"><strong>Revogue o acesso pelo Facebook</strong><br>
    Acesse <a href="https://www.facebook.com/settings?tab=applications" target="_blank">facebook.com/settings → Aplicativos e sites</a>, localize <strong>LITORANO</strong> e clique em <strong>Remover</strong>.</div>
  </div>

  <h2>O que será excluído</h2>
  <ul>
    <li>Token de acesso ao Meta Ads</li>
    <li>Dados de conta e perfil cadastrados</li>
    <li>Histórico de campanhas criadas via plataforma</li>
    <li>Logs de atividade associados à sua conta</li>
  </ul>

  <h2>O que não podemos excluir</h2>
  <p>Campanhas, anúncios e criativos que já foram publicados no Meta Ads permanecem na plataforma do Facebook e devem ser excluídos diretamente pelo <a href="https://business.facebook.com/" target="_blank">Meta Business Manager</a>.</p>

  <div class="box">LITORANO SaaS · Todos os direitos reservados · <a href="/">litorano777.onrender.com</a></div>
</div>
</body></html>'''

# ─── APP PRINCIPAL ────────────────────────────────────────────────────────────

@app.route('/')
def index():
    if not session.get('user_id'):
        return redirect('/login')
    html = open(HTML_APP, encoding='utf-8').read()
    role = session.get('role', 'user')
    html = html.replace('{{USER_ROLE}}', role).replace('{{USERNAME}}', session.get('username', ''))
    return html, 200, {'Content-Type': 'text/html; charset=utf-8'}

@app.route('/api/imoveis')
def api_imoveis():
    try:
        conn = get_db()
        rows = conn.execute('SELECT * FROM imoveis WHERE ativo=1 ORDER BY nome').fetchall()
        fotos = conn.execute('SELECT imovel_id, nome_orig FROM fotos ORDER BY imovel_id, ordem').fetchall()
        conn.close()
        fotos_map = {}
        for f in fotos:
            fotos_map.setdefault(f['imovel_id'], []).append(f['nome_orig'])
        result = []
        for r in rows:
            d = dict(r)
            d['fotos'] = fotos_map.get(r['id'], [])
            result.append(d)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/imovel/<int:iid>/copy')
def api_copy(iid):
    conn = get_db()
    row = conn.execute('SELECT copy_txt FROM imoveis WHERE id=?', (iid,)).fetchone()
    conn.close()
    if not row or not row['copy_txt']:
        return '', 404
    return row['copy_txt'], 200, {'Content-Type': 'text/plain; charset=utf-8'}

@app.route('/api/foto/fs/<path:filepath>')
def api_foto_fs(filepath):
    """Serve fotos do filesystem — sem query no banco"""
    fullpath = os.path.join(IMOVEIS_DIR, filepath)
    if not os.path.abspath(fullpath).startswith(os.path.abspath(IMOVEIS_DIR)):
        return '', 403
    if not os.path.exists(fullpath):
        return '', 404
    mime = 'image/png' if fullpath.lower().endswith('.png') else 'image/jpeg'
    return send_file(fullpath, mimetype=mime)

@app.route('/api/foto/<int:fid>')
def api_foto(fid):
    """Serve fotos de admin (blob no banco)"""
    try:
        conn = get_db()
        row = conn.execute('SELECT dados, mime FROM fotos WHERE id=?', (fid,)).fetchone()
        conn.close()
        if not row or not row['dados']:
            return '', 404
        dados = row['dados']
        if isinstance(dados, memoryview):
            dados = bytes(dados)
        elif not isinstance(dados, bytes):
            import base64 as _b64
            dados = _b64.b64decode(dados)
        return Response(dados, mimetype=row['mime'])
    except Exception:
        return '', 500

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

@app.route('/api/debug/fotos')
def api_debug_fotos():
    conn = get_db()
    rows = conn.execute('SELECT id, imovel_id, nome_orig, mime, LENGTH(dados) as tamanho, ordem FROM fotos ORDER BY imovel_id, ordem').fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

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

# ─── HELPERS PLANO ────────────────────────────────────────────────────────────

def _pdfs_mes(conn, user_id):
    """Conta PDFs gerados pelo usuário no mês atual."""
    row = conn.execute(
        "SELECT COUNT(*) FROM logs WHERE user_id=? AND acao IN ('gerar_iptu','gerar_luz') "
        "AND strftime('%Y-%m', criado_em)=strftime('%Y-%m','now','localtime')",
        (user_id,)
    ).fetchone()
    return row[0] if row else 0

def _check_plano(user_id):
    """Retorna mensagem de erro se limite atingido, None se ok."""
    conn = get_db()
    try:
        user = conn.execute('SELECT plano_id FROM users WHERE id=?', (user_id,)).fetchone()
        if not user or not user['plano_id']:
            return None
        plano = conn.execute('SELECT max_pdfs_mes FROM planos WHERE id=? AND ativo=1', (user['plano_id'],)).fetchone()
        if not plano or plano['max_pdfs_mes'] == 0:
            return None
        uso = _pdfs_mes(conn, user_id)
        if uso >= plano['max_pdfs_mes']:
            return f'Limite de {plano["max_pdfs_mes"]} PDFs por mês atingido. Atualize seu plano.'
        return None
    finally:
        conn.close()

# ─── ROTAS PDF ────────────────────────────────────────────────────────────────

@app.route('/api/gerar-iptu', methods=['POST'])
def api_iptu():
    try:
        erro = _check_plano(session.get('user_id'))
        if erro:
            return jsonify({'error': erro}), 403
        d = request.json or {}
        buf = editar_iptu(d)
        log_action('gerar_iptu', {'nome': d.get('nome'), 'imovel_id': d.get('imovel_id')})
        return send_file(buf, mimetype='application/pdf', download_name='iptu.pdf', as_attachment=False)
    except Exception as e:
        import traceback; return traceback.format_exc(), 500

@app.route('/api/gerar-luz', methods=['POST'])
def api_luz():
    try:
        erro = _check_plano(session.get('user_id'))
        if erro:
            return jsonify({'error': erro}), 403
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
        'total_imoveis':     conn.execute('SELECT COUNT(*) FROM imoveis').fetchone()[0],
        'imoveis_ativos':    conn.execute('SELECT COUNT(*) FROM imoveis WHERE ativo=1').fetchone()[0],
        'total_users':       conn.execute('SELECT COUNT(*) FROM users').fetchone()[0],
        'users_ativos':      conn.execute('SELECT COUNT(*) FROM users WHERE ativo=1').fetchone()[0],
        'logs_hoje':         conn.execute("SELECT COUNT(*) FROM logs WHERE criado_em >= date('now','localtime')").fetchone()[0],
        'logs_total':        conn.execute('SELECT COUNT(*) FROM logs').fetchone()[0],
        'assn_ativas':       conn.execute("SELECT COUNT(*) FROM assinaturas WHERE status='ativa' AND (expira_em IS NULL OR expira_em > datetime('now','localtime'))").fetchone()[0],
        'assn_pendentes':    conn.execute("SELECT COUNT(*) FROM assinaturas WHERE status='pendente'").fetchone()[0],
        'receita_mes':       conn.execute("SELECT COALESCE(SUM(valor),0) FROM assinaturas WHERE status='ativa' AND strftime('%Y-%m',pago_em)=strftime('%Y-%m','now','localtime')").fetchone()[0],
    }
    logs = conn.execute('SELECT * FROM logs ORDER BY id DESC LIMIT 15').fetchall()
    # Planos com contagem de assinantes ativos para o painel rápido
    planos = conn.execute('''
        SELECT p.id, p.nome, p.descricao, p.max_pdfs_mes, p.preco, p.ativo,
               COUNT(a.id) as total_assinantes
        FROM planos p
        LEFT JOIN assinaturas a ON a.plano_id=p.id AND a.status='ativa'
            AND (a.expira_em IS NULL OR a.expira_em > datetime('now','localtime'))
        GROUP BY p.id ORDER BY p.id
    ''').fetchall()
    conn.close()
    return jsonify({'stats': stats, 'logs': [dict(r) for r in logs], 'planos': [dict(p) for p in planos]})

# ─── ADMIN API — USUÁRIOS ─────────────────────────────────────────────────────

@app.route('/admin/api/users', methods=['GET'])
def admin_users_list():
    conn = get_db()
    rows = conn.execute('''
        SELECT u.id, u.username, u.email, u.role, u.ativo, u.criado_em, u.ultimo_login,
               u.plano_id, p.nome as plano_nome, p.max_pdfs_mes
        FROM users u
        LEFT JOIN planos p ON p.id = u.plano_id
        ORDER BY u.id
    ''').fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d['pdfs_mes'] = _pdfs_mes(conn, r['id'])
        result.append(d)
    conn.close()
    return jsonify(result)

@app.route('/admin/api/users', methods=['POST'])
def admin_users_create():
    d = request.json or {}
    username = d.get('username','').strip()
    senha = d.get('senha','')
    role = d.get('role','user')
    plano_id = d.get('plano_id') or None
    if not username or not senha:
        return jsonify({'error': 'Login e senha obrigatórios'}), 400
    if len(senha) < 6:
        return jsonify({'error': 'Senha mínimo 6 caracteres'}), 400
    if role not in ('admin','user'):
        return jsonify({'error': 'Role inválido'}), 400
    conn = get_db()
    try:
        conn.execute('INSERT INTO users (username,pwd_hash,role,plano_id) VALUES (?,?,?,?)',
                     (username, h(senha), role, plano_id))
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
    plano_id = d.get('plano_id') or None
    conn = get_db()
    # Campos básicos
    conn.execute('UPDATE users SET username=?, role=?, plano_id=?, email=?, ativo=? WHERE id=?',
                 (d.get('username'), d.get('role'), plano_id,
                  d.get('email', ''), int(d.get('ativo', 1)), uid))
    # Senha: só atualiza se foi enviada
    senha = (d.get('senha') or '').strip()
    if senha:
        if len(senha) < 6:
            conn.close()
            return jsonify({'error': 'Senha mínimo 6 caracteres'}), 400
        conn.execute('UPDATE users SET pwd_hash=? WHERE id=?', (h(senha), uid))
    # Plano definido manualmente pelo admin → ativa assinatura na tabela
    if plano_id:
        plano = conn.execute('SELECT tipo FROM planos WHERE id=?', (plano_id,)).fetchone()
        expira = None
        if plano:
            tipo = plano['tipo']
            if tipo == 'semanal':
                expira = conn.execute("SELECT datetime('now','+7 days','localtime')").fetchone()[0]
            elif tipo == 'mensal':
                expira = conn.execute("SELECT datetime('now','+30 days','localtime')").fetchone()[0]
            # vitalicio → expira_em = NULL
        # Cancela assinaturas ativas anteriores
        conn.execute("UPDATE assinaturas SET status='cancelada' WHERE user_id=? AND status='ativa'", (uid,))
        # Cria nova assinatura ativa
        conn.execute('''INSERT INTO assinaturas (user_id, plano_id, status, valor, pago_em, expira_em)
            VALUES (?, ?, 'ativa', 0, datetime('now','localtime'), ?)''',
            (uid, plano_id, expira))
    elif plano_id is None and 'plano_id' in d:
        # Admin removeu o plano → cancela assinatura ativa
        conn.execute("UPDATE assinaturas SET status='cancelada' WHERE user_id=? AND status='ativa'", (uid,))
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

# ─── ADMIN API — PLANOS ───────────────────────────────────────────────────────

@app.route('/admin/api/planos', methods=['GET'])
def admin_planos_list():
    conn = get_db()
    rows = conn.execute('''
        SELECT p.*, COUNT(a.id) as total_assinantes
        FROM planos p
        LEFT JOIN assinaturas a ON a.plano_id=p.id AND a.status='ativa'
            AND (a.expira_em IS NULL OR a.expira_em > datetime('now','localtime'))
        GROUP BY p.id ORDER BY p.id
    ''').fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route('/admin/api/planos', methods=['POST'])
def admin_planos_create():
    try:
        d = request.json or {}
        nome = d.get('nome','').strip()
        if not nome:
            return jsonify({'error': 'Nome obrigatório'}), 400
        conn = get_db()
        tipo = d.get('tipo','mensal')
        if tipo not in ('semanal','mensal','vitalicio'): tipo = 'mensal'
        cur = conn.execute('INSERT INTO planos (nome,descricao,max_pdfs_mes,preco,tipo,checkout_url) VALUES (?,?,?,?,?,?)',
                           (nome, d.get('descricao',''), int(d.get('max_pdfs_mes') or 0),
                            float(d.get('preco') or 0), tipo, d.get('checkout_url','').strip()))
        new_id = cur.lastrowid
        conn.commit(); conn.close()
        log_action('admin_criar_plano', {'nome': nome})
        return jsonify({'ok': True, 'id': new_id})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/admin/api/planos/<int:pid>', methods=['PUT'])
def admin_planos_edit(pid):
    try:
        d = request.json or {}
        conn = get_db()
        tipo = d.get('tipo','mensal')
        if tipo not in ('semanal','mensal','vitalicio'): tipo = 'mensal'
        conn.execute('UPDATE planos SET nome=?,descricao=?,max_pdfs_mes=?,preco=?,tipo=?,checkout_url=?,ativo=? WHERE id=?',
                     (d.get('nome'), d.get('descricao',''), int(d.get('max_pdfs_mes') or 0),
                      float(d.get('preco') or 0), tipo, d.get('checkout_url','').strip(), int(d.get('ativo', 1)), pid))
        conn.commit(); conn.close()
        log_action('admin_editar_plano', {'id': pid})
        return jsonify({'ok': True})
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/admin/api/planos/<int:pid>', methods=['DELETE'])
def admin_planos_delete(pid):
    conn = get_db()
    conn.execute('UPDATE users SET plano_id=NULL WHERE plano_id=?', (pid,))
    conn.execute('DELETE FROM planos WHERE id=?', (pid,))
    conn.commit(); conn.close()
    log_action('admin_excluir_plano', {'id': pid})
    return jsonify({'ok': True})

# ─── ADMIN API — IMÓVEIS ──────────────────────────────────────────────────────

def _slug(nome):
    import unicodedata
    s = unicodedata.normalize('NFKD', nome).encode('ascii','ignore').decode()
    return re.sub(r'[^a-z0-9]+', '-', s.lower()).strip('-')

@app.route('/admin/api/imoveis', methods=['GET'])
def admin_imoveis_list():
    conn = get_db()
    rows = conn.execute('SELECT * FROM imoveis ORDER BY nome').fetchall()
    fotos = conn.execute('SELECT imovel_id, nome_orig FROM fotos ORDER BY imovel_id, ordem').fetchall()
    conn.close()
    fotos_map = {}
    for f in fotos:
        if f['imovel_id'] not in fotos_map:
            fotos_map[f['imovel_id']] = f['nome_orig']
    result = []
    for r in rows:
        d = dict(r)
        d['foto_nome'] = fotos_map.get(r['id'])
        result.append(d)
    return jsonify(result)

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
    cur_im = conn.execute('''INSERT INTO imoveis
        (slug,nome,endereco,cep,cidade,estado,cod_imovel,quartos,banheiros,area,mobiliado,
         destaque1,destaque2,destaque3,descricao,copy_txt,preco_baixa,preco_alta)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (
        slug, nome, d.get('endereco',''), d.get('cep',''), d.get('cidade','Ubatuba'),
        d.get('estado','SP'), d.get('cod_imovel',''), d.get('quartos',''), d.get('banheiros',''),
        d.get('area',''), d.get('mobiliado','Sim'), d.get('destaque1',''), d.get('destaque2',''),
        d.get('destaque3',''), d.get('descricao',''), d.get('copy_txt',''),
        d.get('preco_baixa',''), d.get('preco_alta',''),
    ))
    new_id = cur_im.lastrowid
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
        descricao=?,copy_txt=?,preco_baixa=?,preco_alta=?,atualizado_em=datetime('now','localtime') WHERE id=?''', (
        d.get('nome'), d.get('endereco'), d.get('cep'), d.get('cidade'), d.get('estado'),
        d.get('cod_imovel'), d.get('quartos'), d.get('banheiros'), d.get('area'), d.get('mobiliado'),
        d.get('destaque1'), d.get('destaque2'), d.get('destaque3'), d.get('descricao'),
        d.get('copy_txt'), d.get('preco_baixa',''), d.get('preco_alta',''), iid
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

# ─── ROTA LINK DIRETO DO IMÓVEL ──────────────────────────────────────────────

@app.route('/imovel/<slug>')
def imovel_link(slug):
    html = open(HTML_APP, encoding='utf-8').read()
    role = session.get('role', 'user') if session.get('user_id') else 'guest'
    username = session.get('username', '')
    html = html.replace('{{USER_ROLE}}', role).replace('{{USERNAME}}', username)
    # Inject auto-select script
    inject = f'<script>window._IMOVEL_SLUG = {json.dumps(slug)};</script>'
    html = html.replace('</head>', inject + '</head>', 1)
    return html, 200, {'Content-Type': 'text/html; charset=utf-8'}

# ─── API HISTÓRICO ────────────────────────────────────────────────────────────

@app.route('/api/historico', methods=['GET'])
def api_historico_list():
    try:
        conn = get_db()
        rows = conn.execute('SELECT * FROM historico ORDER BY id DESC LIMIT 50').fetchall()
        conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/historico', methods=['POST'])
def api_historico_create():
    try:
        d = request.json or {}
        conn = get_db()
        conn.execute('''INSERT INTO historico
            (user_id,user_nome,imovel_id,imovel_nome,cliente_nome,cliente_cpf,checkin,checkout,valor)
            VALUES (?,?,?,?,?,?,?,?,?)''', (
            session.get('user_id'), session.get('username'),
            d.get('imovel_id'), d.get('imovel_nome'),
            d.get('cliente_nome'), d.get('cliente_cpf'),
            d.get('checkin'), d.get('checkout'),
            d.get('valor')
        ))
        conn.commit(); conn.close()
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/historico/stats')
def api_historico_stats():
    try:
        conn = get_db()
        rows = conn.execute('''
            SELECT date(criado_em) as data, COUNT(*) as total
            FROM historico
            WHERE criado_em >= date('now', '-6 days', 'localtime')
            GROUP BY date(criado_em)
            ORDER BY data ASC
        ''').fetchall()
        conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ─── API CLIENTES ─────────────────────────────────────────────────────────────

@app.route('/api/clientes', methods=['GET'])
def api_clientes_list():
    try:
        conn = get_db()
        rows = conn.execute('SELECT * FROM clientes ORDER BY ultimo_uso DESC LIMIT 100').fetchall()
        conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/clientes', methods=['POST'])
def api_clientes_create():
    try:
        d = request.json or {}
        nome = (d.get('nome') or '').strip()
        if not nome:
            return jsonify({'error': 'Nome obrigatório'}), 400
        cpf = (d.get('cpf') or '').strip()
        endereco = (d.get('endereco') or '').strip()
        cep = (d.get('cep') or '').strip()
        conn = get_db()
        if cpf:
            existing = conn.execute('SELECT id FROM clientes WHERE cpf=?', (cpf,)).fetchone()
            if existing:
                conn.execute('''UPDATE clientes SET nome=?,endereco=?,cep=?,
                    ultimo_uso=datetime('now','localtime') WHERE cpf=?''',
                    (nome, endereco, cep, cpf))
            else:
                conn.execute('''INSERT INTO clientes (nome,cpf,endereco,cep,ultimo_uso)
                    VALUES (?,?,?,?,datetime('now','localtime'))''',
                    (nome, cpf, endereco, cep))
        else:
            conn.execute('''INSERT INTO clientes (nome,cpf,endereco,cep,ultimo_uso)
                VALUES (?,?,?,?,datetime('now','localtime'))''',
                (nome, cpf, endereco, cep))
        conn.commit(); conn.close()
        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ─── ADMIN API — BACKUP ───────────────────────────────────────────────────────

@app.route('/admin/api/backup')
def admin_backup():
    try:
        conn = get_db()
        tables = ['users','imoveis','historico','clientes','config','funil','logs','planos']
        backup = {}
        for t in tables:
            try:
                rows = conn.execute(f'SELECT * FROM {t}').fetchall()
                backup[t] = [dict(r) for r in rows]
            except Exception:
                backup[t] = []
        conn.close()
        import datetime
        fname = 'backup_litorano_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.json'
        resp = Response(json.dumps(backup, ensure_ascii=False, indent=2),
                        mimetype='application/json')
        resp.headers['Content-Disposition'] = f'attachment; filename="{fname}"'
        log_action('admin_backup')
        return resp
    except Exception as e:
        return jsonify({'error': str(e)}), 500

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
