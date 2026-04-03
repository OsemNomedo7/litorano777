import sqlite3, hashlib, os, json, re

BASE     = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get('DATA_DIR', os.path.join(BASE, 'data'))
DB_PATH  = os.path.join(DATA_DIR, 'litorano.db')

TURSO_URL   = os.environ.get('TURSO_URL')
TURSO_TOKEN = os.environ.get('TURSO_TOKEN')

# ─── CONEXÃO ──────────────────────────────────────────────────────────────────

def get_db():
    if TURSO_URL and TURSO_TOKEN:
        import libsql_experimental as libsql
        conn = libsql.connect(TURSO_URL, auth_token=TURSO_TOKEN)
        conn.row_factory = sqlite3.Row
        return conn
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def h(s):
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

# ─── DEFAULTS ─────────────────────────────────────────────────────────────────

FUNIL_DEFAULT = [
    (1, 'Primeiro contato',
     'Oi! Tudo bem? \U0001f60a O im\u00f3vel ainda est\u00e1 dispon\u00edvel sim! Posso te enviar as fotos agora? \U0001f4f8'),
    (2, 'Ap\u00f3s enviar fotos',
     'Essas s\u00e3o as fotos! \U0001f3e0 O que achou? Ficou com alguma d\u00favida sobre o im\u00f3vel?'),
    (3, 'Se insistir em visita presencial',
     'Boa pergunta! \U0001f60a No momento estou com a agenda bem cheia l\u00e1 em Ubatuba, mas posso te mandar mais fotos de qualquer c\u00f4modo agora mesmo! Tem algum espa\u00e7o espec\u00edfico que quer ver com mais detalhe? \U0001f4f8'),
    (4, 'Apresentar condi\u00e7\u00f5es de reserva',
     'Para fecharmos as datas, vou te enviar o <strong>contrato de loca\u00e7\u00e3o</strong> agora \U0001f4c4 Assim que voc\u00ea assinar, o sinal de <strong>35% do valor total via PIX</strong> confirma a reserva \U0001f512 O restante voc\u00ea paga no dia da entrada \u2014 aceitamos PIX, dinheiro ou d\u00e9bito. \u274c Cr\u00e9dito n\u00e3o trabalhamos.'),
    (5, 'Ap\u00f3s contrato assinado + sinal recebido',
     '\U0001f389 Perfeito! Contrato assinado e sinal recebido \u2014 <strong>reserva confirmada!</strong> Vou te enviar agora o <strong>laudo de vistoria</strong> e o <strong>comprovante de endere\u00e7o</strong> do im\u00f3vel. Qualquer d\u00favida estou aqui \u2705'),
    (6, 'Se pedir garantia do im\u00f3vel',
     'Claro! \U0001f60a Para sua seguran\u00e7a, vou te enviar tamb\u00e9m o <strong>IPTU do im\u00f3vel</strong> como comprovante de propriedade \U0001f4c4\u2705'),
    (7, 'Obje\u00e7\u00e3o "t\u00e1 caro"',
     'Entendo! \U0001f60a Considerando que \u00e9 um im\u00f3vel <span id="fm7-dest">bem localizado</span>, totalmente equipado e com documenta\u00e7\u00e3o em dia \u2014 o valor est\u00e1 bem competitivo para a regi\u00e3o. Quer que eu te mande as datas dispon\u00edveis?'),
]

ADS_DEFAULT = [
    ('ads_tipo',               'Engajamento \u2014 x1 de WhatsApp'),
    ('ads_orcamento_estrategia','Or\u00e7amento da campanha'),
    ('ads_orcamento',          'Or\u00e7amento total (m\u00ednimo R$30)'),
    ('ads_lances',             'Volume mais alto'),
    ('ads_destino',            'WhatsApp'),
    ('ads_meta',               'Maximizar n\u00famero de conversas'),
    ('ads_duracao',            '24 ou 48 horas'),
    ('ads_localizacoes',       'Brasil \u2014 MS \u00b7 MT \u00b7 MG \u00b7 PR \u00b7 SP \u00b7 GO'),
    ('ads_idade_min',          '18 anos'),
    ('ads_publico',            '18 a 65+ \u2014 Homem e Mulher'),
    ('ads_posicionamentos',    'Somente Facebook + Instagram (desativar o resto)'),
    ('ads_posicoes',           'Feeds \u00b7 Stories \u00b7 Status e Reels \u00b7 Resultado de pesquisa'),
    ('ads_parceria',           'Desativado'),
    ('ads_formato',            'Carrossel (m\u00ednimo 4 imagens)'),
    ('ads_criativo',           'Ativado'),
    ('ads_aprimoramentos',     'Retoques visuais \u00b7 Adicionar m\u00fasica \u00b7 Coment\u00e1rios relevantes \u00b7 Descri\u00e7\u00e3o din\u00e2mica \u00b7 Destacar cart\u00e3o do carrossel'),
    ('ads_varios_anunciantes', 'Sim'),
]

# ─── INIT ─────────────────────────────────────────────────────────────────────

def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = get_db()
    c = conn.cursor()
    c.executescript('''
        CREATE TABLE IF NOT EXISTS users (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            username    TEXT UNIQUE NOT NULL,
            pwd_hash    TEXT NOT NULL,
            role        TEXT NOT NULL DEFAULT 'user',
            ativo       INTEGER NOT NULL DEFAULT 1,
            criado_em   TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            ultimo_login TEXT
        );
        CREATE TABLE IF NOT EXISTS imoveis (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            slug        TEXT NOT NULL UNIQUE,
            nome        TEXT NOT NULL,
            endereco    TEXT DEFAULT '',
            cep         TEXT DEFAULT '',
            cidade      TEXT DEFAULT 'Ubatuba',
            estado      TEXT DEFAULT 'SP',
            cod_imovel  TEXT DEFAULT '',
            quartos     TEXT DEFAULT '',
            banheiros   TEXT DEFAULT '',
            area        TEXT DEFAULT '',
            mobiliado   TEXT DEFAULT 'Sim',
            destaque1   TEXT DEFAULT '',
            destaque2   TEXT DEFAULT '',
            destaque3   TEXT DEFAULT '',
            descricao   TEXT DEFAULT '',
            copy_txt    TEXT DEFAULT '',
            ativo       INTEGER NOT NULL DEFAULT 1,
            criado_em   TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            atualizado_em TEXT
        );
        CREATE TABLE IF NOT EXISTS fotos (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            imovel_id   INTEGER NOT NULL,
            nome_orig   TEXT NOT NULL,
            mime        TEXT NOT NULL DEFAULT 'image/jpeg',
            dados       BLOB NOT NULL,
            ordem       INTEGER NOT NULL DEFAULT 0,
            criado_em   TEXT NOT NULL DEFAULT (datetime('now','localtime')),
            FOREIGN KEY (imovel_id) REFERENCES imoveis(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS config (
            chave       TEXT PRIMARY KEY,
            valor       TEXT NOT NULL DEFAULT '',
            atualizado_em TEXT NOT NULL DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS funil (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ordem       INTEGER NOT NULL UNIQUE,
            label       TEXT NOT NULL,
            mensagem    TEXT NOT NULL,
            ativo       INTEGER NOT NULL DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS logs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER,
            user_nome   TEXT,
            acao        TEXT NOT NULL,
            detalhes    TEXT,
            ip          TEXT,
            criado_em   TEXT NOT NULL DEFAULT (datetime('now','localtime'))
        );
    ''')
    # Admin padrão — credenciais: milionariog7 / milionariog777
    c.execute('INSERT OR IGNORE INTO users (username, pwd_hash, role) VALUES (?,?,?)',
              ('milionariog7', h('milionariog777'), 'admin'))
    c.executemany('INSERT OR IGNORE INTO config (chave, valor) VALUES (?,?)', ADS_DEFAULT)
    c.executemany('INSERT OR IGNORE INTO funil (ordem, label, mensagem) VALUES (?,?,?)', FUNIL_DEFAULT)
    conn.commit()
    conn.close()

# ─── MIGRAÇÃO DE ARQUIVOS ─────────────────────────────────────────────────────

def _parse_desc(caminho):
    path = os.path.join(caminho, 'descricao.txt')
    if not os.path.exists(path):
        return {}
    dados = {}
    chave = None
    with open(path, encoding='utf-8', errors='replace') as f:
        for linha in f:
            linha = linha.strip()
            if not linha:
                continue
            if ':' in linha:
                p = linha.split(':', 1)
                chave = p[0].strip()
                dados[chave] = p[1].strip()
            elif chave:
                dados[chave] += ' ' + linha
    if 'Endereco' in dados and 'CEP' not in dados:
        m = re.search(r'(\d{5}-\d{3})', dados['Endereco'])
        if m:
            dados['CEP'] = m.group(1)
            dados['Endereco'] = dados['Endereco'].replace(', ' + m.group(1), '').replace(m.group(1), '').strip().rstrip(',')
    return dados

def migrate_from_files(imoveis_dir):
    if not os.path.isdir(imoveis_dir):
        return 0
    conn = get_db()
    c = conn.cursor()
    count = 0
    for pasta in sorted(os.listdir(imoveis_dir)):
        caminho = os.path.join(imoveis_dir, pasta)
        if not os.path.isdir(caminho):
            continue
        if c.execute('SELECT id FROM imoveis WHERE slug=?', (pasta,)).fetchone():
            continue
        dados = _parse_desc(caminho)
        if not dados:
            continue
        copy_path = os.path.join(caminho, 'copy.txt')
        copy_txt = open(copy_path, encoding='utf-8', errors='replace').read().strip() if os.path.exists(copy_path) else ''
        c.execute('''INSERT INTO imoveis
            (slug,nome,endereco,cep,cidade,estado,cod_imovel,quartos,banheiros,area,mobiliado,
             destaque1,destaque2,destaque3,descricao,copy_txt)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (
            pasta, dados.get('Nome', pasta),
            dados.get('Endereco', ''), dados.get('CEP', ''),
            dados.get('Cidade', 'Ubatuba'), dados.get('Estado', 'SP'),
            dados.get('Cod_Imovel', ''), dados.get('Quartos', ''),
            dados.get('Banheiros', ''), dados.get('Area', ''),
            dados.get('Mobiliado', 'Sim'),
            dados.get('Destaque_1', ''), dados.get('Destaque_2', ''), dados.get('Destaque_3', ''),
            dados.get('Descricao', ''), copy_txt,
        ))
        imovel_id = c.lastrowid
        ordem = 0
        for arq in sorted(os.listdir(caminho)):
            if arq.lower().endswith(('.jpg', '.jpeg', '.png')):
                try:
                    foto_bytes = open(os.path.join(caminho, arq), 'rb').read()
                    mime = 'image/png' if arq.lower().endswith('.png') else 'image/jpeg'
                    c.execute('INSERT INTO fotos (imovel_id,nome_orig,mime,dados,ordem) VALUES (?,?,?,?,?)',
                              (imovel_id, arq, mime, foto_bytes, ordem))
                    ordem += 1
                except Exception:
                    pass
        count += 1
    if count:
        c.execute('INSERT INTO logs (user_nome,acao,detalhes) VALUES (?,?,?)',
                  ('system', 'migrate_from_files', json.dumps({'count': count})))
        conn.commit()
    conn.close()
    return count
