# main.py — FastAPI + Front Chat + OpenAI v1 + SQL Server LocalDB (odbc_connect) + .env auto
import os, sys, re
from urllib.parse import quote_plus

import pandas as pd
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine, text as sql_text

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from openai import OpenAI
import uvicorn

# ========= 0) HTML do front (chat estilo ChatGPT) =========
INDEX_HTML = """<!doctype html>
<html lang="pt-br"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Agente SQL • Chat</title>
<style>
:root{--bg:#0e0f12;--panel:#121418;--ink:#e9eaee;--sub:#8a8f98;--me:#2b6ef7;--ai:#1c1f26}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:16px/1.5 system-ui,Segoe UI,Roboto,Arial}
.app{max-width:900px;margin:0 auto;height:100vh;display:grid;grid-template-rows:auto 1fr auto}
header{padding:14px 16px;border-bottom:1px solid #1c1f26;background:var(--panel);position:sticky;top:0}
header h1{margin:0;font-size:16px;font-weight:600;letter-spacing:.2px}
#chat{padding:18px;overflow:auto;scroll-behavior:smooth}
.row{display:flex;margin:10px 0}.row.me{justify-content:flex-end}
.bubble{max-width:72%;padding:12px 14px;border-radius:16px;white-space:pre-wrap;word-wrap:break-word}
.me .bubble{background:var(--me);color:#fff;border-bottom-right-radius:6px}
.ai .bubble{background:var(--ai);border-bottom-left-radius:6px}
.sql{margin-top:8px;padding:8px;border-radius:10px;background:#0b0d12;color:#b7c1d1;font-family:ui-monospace,Consolas,monaco,monospace;font-size:13px}
table{border-collapse:collapse;width:100%;margin-top:10px;background:#0b0d12}
th,td{border:1px solid #1e2230;padding:6px 8px;font-size:13px}
.input{display:flex;gap:10px;padding:12px;border-top:1px solid #1c1f26;background:var(--panel)}
#q{flex:1;resize:none;border:1px solid #222632;background:#0b0d12;color:var(--ink);padding:12px 14px;border-radius:12px;outline:none;height:52px}
.hint{color:var(--sub);font-size:12px;margin:6px 16px 14px}
</style></head><body>
<div class="app">
  <header><h1>Agente SQL • Chat</h1></header>
  <div id="chat"></div>
  <div class="input"><textarea id="q" placeholder="Pergunte e pressione Enter…" autofocus></textarea></div>
  <div class="hint">Ex.: “Liste as tabelas existentes” • “Top 10 de dbo.SUA_TABELA”</div>
</div>
<script>
const chat=document.getElementById("chat"), box=document.getElementById("q");
function addBubble(text,who){const r=document.createElement("div");r.className="row "+(who==="me"?"me":"ai");const b=document.createElement("div");b.className="bubble";b.textContent=text;r.appendChild(b);chat.appendChild(r);chat.scrollTop=chat.scrollHeight;return b}
function renderTable(cols,rows){if(!rows||!rows.length)return document.createTextNode("(Sem resultados)");let h="<table><thead><tr>";for(const c of cols)h+=`<th>${c}</th>`;h+="</tr></thead><tbody>";for(const r of rows){h+="<tr>";for(const c of cols)h+=`<td>${r[c]??""}</td>`;h+="</tr>"}h+="</tbody></table>";const w=document.createElement("div");w.innerHTML=h;return w}
async function ask(q){addBubble(q,"me");const hold=addBubble("pensando…","ai");try{const res=await fetch("/ask",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({question:q})});const data=await res.json();if(!res.ok) throw new Error(data.detail||"Erro");hold.textContent="SQL gerado:";const sql=document.createElement("div");sql.className="sql";sql.textContent=data.sql;hold.appendChild(sql);hold.appendChild(renderTable(data.columns,data.rows))}catch(e){hold.textContent="Erro: "+e.message}}
box.addEventListener("keydown",e=>{if(e.key==="Enter"&&!e.shiftKey){e.preventDefault();const q=box.value.trim();if(!q)return;box.value="";ask(q)}})
</script></body></html>
"""

# ========= 1) .env =========
ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
if not os.path.exists(ENV_PATH):
    with open(ENV_PATH, "w", encoding="utf-8") as f:
        f.write(
            "OPENAI_API_KEY=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\n"
            "DB_SERVER=(localdb)\\dev\n"
            "DB_DATABASE=PBS_PROCFIT_DADOS\n"
            "DB_SCHEMA=dbo\n"
        )
    print(f"[INFO] .env criado em {ENV_PATH}. Preencha a OPENAI_API_KEY e rode de novo.")
    sys.exit(0)

load_dotenv(ENV_PATH)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
DB_SERVER      = os.getenv("DB_SERVER", r"(localdb)\dev").strip()
DB_DATABASE    = os.getenv("DB_DATABASE", "PBS_PROCFIT_DADOS").strip()
DB_SCHEMA      = os.getenv("DB_SCHEMA", "dbo").strip()

if not OPENAI_API_KEY:
    print("ERRO: Defina OPENAI_API_KEY no .env")
    sys.exit(1)

client = OpenAI(api_key=OPENAI_API_KEY)

# ========= 2) Engine SQLAlchemy (ODBC robusto p/ LocalDB) =========
def make_engine():
    drivers = [d for d in pyodbc.drivers() if "ODBC Driver" in d and "SQL Server" in d]
    if not drivers:
        raise RuntimeError("Nenhum driver ODBC do SQL Server encontrado. Instale o Driver 17/18.")
    driver = "ODBC Driver 18 for SQL Server" if "ODBC Driver 18 for SQL Server" in drivers else "ODBC Driver 17 for SQL Server"
    odbc = (
        f"Driver={{{driver}}};"
        f"Server={DB_SERVER};"
        f"Database={DB_DATABASE};"
        "Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc)}", fast_executemany=True)

engine = make_engine()

# ========= 3) Snapshot de esquema (lazy cache) =========
_schema_cache = None
def get_schema_snapshot(limit: int = 120) -> str:
    global _schema_cache
    if _schema_cache:
        return _schema_cache
    try:
        q = sql_text("""
            SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :schema
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
        """)
        df = pd.read_sql(q, engine, params={"schema": DB_SCHEMA})
        lines = []
        for (schema, table), g in df.groupby(["TABLE_SCHEMA","TABLE_NAME"]):
            cols = ", ".join((g["COLUMN_NAME"] + " " + g["DATA_TYPE"]).tolist()[:20])
            lines.append(f"{schema}.{table}({cols})")
        _schema_cache = "\n".join(lines[:limit]) or "(sem colunas)"
    except Exception:
        _schema_cache = "(esquema indisponível)"
    return _schema_cache

# ========= 4) Regras + saneamento de SQL =========
def system_msg() -> str:
    return f"""Você é um gerador de SQL (T-SQL) para SQL Server.
- Responda APENAS com uma query SELECT válida (sem markdown, sem ``` e sem prefixos 'sql:'/'tsql:').
- Proibido: INSERT/UPDATE/DELETE/DROP/ALTER/MERGE/CREATE.
- Use o schema {DB_SCHEMA}. Limite: TOP 50.
- Não use LIMIT/OFFSET; use apenas TOP N do SQL Server.
- Pode usar expressões aritméticas no SELECT (ex.: col1*col2, (a*b)*aliquota).
Esquema (amostra):
{get_schema_snapshot()}
"""

def clean_sql(out: str) -> str:
    s = (out or "").strip()

    # remove cercas/prefixos
    s = re.sub(r"^```(?:sql|tsql)?\s*", "", s, flags=re.I)
    s = re.sub(r"\s*```$", "", s, flags=re.I)
    s = re.sub(r"^\s*(?:sql|tsql|query)\s*:?\s*", "", s, flags=re.I)
    s = re.sub(r"^\s*(?:sql|tsql)\s*\r?\n+", "", s, flags=re.I)
    lines = s.splitlines()
    if lines and re.fullmatch(r"\s*(?:sql|tsql)\s*", lines[0], flags=re.I):
        lines = lines[1:]
    s = "\n".join(lines).strip()
    s = re.sub(r"[ \t]+", " ", s).strip()

    # tem que ser SELECT
    if not re.search(r"\bSELECT\b", s, flags=re.I):
        raise ValueError("A IA não retornou um SELECT válido.")

    # REMOVE sintaxes não-SQL Server
    s = re.sub(r"\bOFFSET\s+\d+\s+ROWS\s+FETCH\s+NEXT\s+\d+\s+ROWS\s+ONLY;?", "", s, flags=re.I)
    s = re.sub(r"\bLIMIT\s+\d+\s*;?", "", s, flags=re.I)  # <--- remove LIMIT

    # INJETA TOP 50 (inclusive quando tem DISTINCT)
    if not re.search(r"\bTOP\s+\d+\b", s, flags=re.I):
        s = re.sub(r"(?i)^\s*SELECT\s+DISTINCT\s+", "SELECT DISTINCT TOP 50 ", s, count=1)
        s = re.sub(r"(?i)^\s*SELECT\s+", "SELECT TOP 50 ", s, count=1)

    # injeta schema padrão após FROM/JOIN quando faltar
    def _inject(m): return f"{m.group(1)}{DB_SCHEMA}.{m.group(2)}"
    s = re.sub(rf"(?i)\b(FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*)\b(?!\.)", _inject, s)

    # bloqueio DDL/DML
    if re.search(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|MERGE)\b", s, flags=re.I):
        raise ValueError("Comando não permitido.")

    return s.strip().rstrip(";") + ";"

def nl_to_sql(question: str) -> str:
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role":"system","content":system_msg()},
            {"role":"user","content":"Pergunta: " + question + "\nResponda APENAS com a query SELECT, sem markdown e sem prefixos."},
        ],
        temperature=0.0,
    )
    return clean_sql(resp.choices[0].message.content)

def run_query(sql: str) -> pd.DataFrame:
    with engine.begin() as con:
        return pd.read_sql(sql_text(sql), con)

# ========= 5) API + FRONT =========
app = FastAPI(title="Agente SQL")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

class AskBody(BaseModel):
    question: str

@app.get("/", response_class=HTMLResponse)
def home():  # front
    return INDEX_HTML

@app.get("/favicon.ico", response_class=PlainTextResponse)
def fav():  # evita 404 chato
    return ""

@app.get("/health")
def health():
    return {"status":"ok"}

@app.post("/ask")
def ask(body: AskBody):
    try:
        sql = nl_to_sql(body.question)
        df  = run_query(sql)
        return {"sql": sql, "columns": df.columns.tolist(), "rows": df.head(50).to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ========= 6) Main =========
if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=5050, reload=False)
