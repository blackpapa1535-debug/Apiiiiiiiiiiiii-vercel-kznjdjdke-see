# language: Python 3.11, target: Vercel serverless (AWS Lambda runtime)
# DuckDB runs in /tmp — cold start ~8-15s, warm ~1s. httpfs pulls parquet from HF over HTTPS.
import asyncio
import json
import os
import time
import threading
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor

import duckdb
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware

# ==================== CONFIG ====================
HF_INDEX_BASE = os.environ.get(
    "ICMR_HF_INDEX_BASE",
    "https://huggingface.co/datasets/Kzr0xx/icrm-hitek-full-db-mixed/resolve/main",
).rstrip("/")

PARALLELISM = int(os.environ.get("ICMR_PARALLEL", "2"))
THREADS_PER_CONN = int(os.environ.get("ICMR_THREADS_PER_CONN", "2"))
DUPLICATE_CAP = 2
TMP = "/tmp"

SEARCH_FIELDS = [
    "name", "fathersName", "phoneNumber", "aadharNumber", "otherNumber",
    "address", "district", "pincode", "state", "town",
]
NUMBER_FIELDS = ["phoneNumber", "aadharNumber", "otherNumber"]

REMOTE_INDEXES = {
    "phone": [f"{HF_INDEX_BASE}/idx_phone.{i}.parquet" for i in range(7)],
    "aadhar": [f"{HF_INDEX_BASE}/idx_aadhar.{i}.parquet" for i in range(7)],
}

DEV = "INSOUL"
MAKER = "JOCKER"

# ==================== APP ====================
app = FastAPI(
    title="ICMR + HITEK Search API",
    description="Search through 2.5 billion records — Vercel serverless",
    version="2.1.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== DUCKDB POOL ====================
_conns: list = []
_conns_lock = threading.Lock()
_thread_local = threading.local()
pool = ThreadPoolExecutor(max_workers=PARALLELISM, thread_name_prefix="duck")

LOAD_STATUS = {
    "phone_loaded": False,
    "aadhar_loaded": False,
    "loading": False,
    "load_start_time": None,
    "load_end_time": None,
    "error": None,
}

def _idx_ready(kind: str) -> bool:
    if kind == "phone":
        return LOAD_STATUS["phone_loaded"]
    if kind == "aadhar":
        return LOAD_STATUS["aadhar_loaded"]
    return False

def _new_conn() -> duckdb.DuckDBPyConnection:
    os.makedirs(f"{TMP}/duckdb_extensions", exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET home_directory='{TMP}'")
    con.execute(f"SET extension_directory='{TMP}/duckdb_extensions'")
    con.execute("INSTALL parquet; LOAD parquet;")
    con.execute("INSTALL httpfs; LOAD httpfs;")

    for kind, urls in REMOTE_INDEXES.items():
        view = f"people_{kind}"
        try:
            lst = ", ".join(f"'{u}'" for u in urls)
            con.execute(
                f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM read_parquet([{lst}])"
            )
            if kind == "phone":
                LOAD_STATUS["phone_loaded"] = True
            elif kind == "aadhar":
                LOAD_STATUS["aadhar_loaded"] = True
            print(f"✅ Loaded {kind} index")
        except Exception as e:
            print(f"❌ Failed to load {kind} index: {e}")
            LOAD_STATUS["error"] = str(e)

    con.execute(f"SET threads = {THREADS_PER_CONN}")
    return con

def _thread_id() -> int:
    tid = getattr(_thread_local, "id", None)
    if tid is None:
        with _conns_lock:
            tid = len(_conns)
            _thread_local.id = tid
    return tid

def _get_conn() -> duckdb.DuckDBPyConnection:
    ident = _thread_id()
    with _conns_lock:
        while len(_conns) <= ident:
            _conns.append(_new_conn())
    return _conns[ident]

# ==================== SEARCH ====================
def _person_key(row: dict) -> tuple:
    ph = (row.get("phoneNumber") or "").strip()
    ad = (row.get("aadharNumber") or "").strip()
    if ph or ad:
        return (ph, ad)
    return (row.get("name") or "").strip(), (row.get("fathersName") or "").strip()

def _connected_numbers(row: dict) -> list:
    connected, seen = [], set()
    for field in NUMBER_FIELDS:
        raw = row.get(field)
        if raw is None:
            continue
        value = str(raw).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        connected.append({"field": field, "value": value})
    return connected

def _cap_duplicates(rows: list) -> list:
    seen: dict = {}
    out = []
    for r in rows:
        k = _person_key(r)
        n = seen.get(k, 0)
        if n < DUPLICATE_CAP:
            seen[k] = n + 1
            record = dict(r)
            record.pop("source", None)
            record["connected_numbers"] = _connected_numbers(record)
            out.append(record)
    return out

def _run_field_search(field: str, value: str, mode: str, limit: int) -> dict:
    if field not in SEARCH_FIELDS:
        raise ValueError(f"Unknown field: {field}")
    v = value.replace("'", "''")

    if mode == "exact":
        if field == "phoneNumber" and _idx_ready("phone"):
            view = "people_phone"
        elif field == "aadharNumber" and _idx_ready("aadhar"):
            view = "people_aadhar"
        else:
            return {"field": field, "value": value, "mode": mode, "count": 0, "results": []}
        sql = f"SELECT * FROM {view} WHERE {field} = '{v}' LIMIT {limit * DUPLICATE_CAP + 20}"
    else:
        if field == "phoneNumber" and _idx_ready("phone"):
            view = "people_phone"
            v2 = v.replace("%", r"\%").replace("_", r"\_")
      sql = f"SELECT * FROM {view} WHERE {field} ILIKE '%{v2}%' ESCAPE '\\' LIMIT {limit * DUPLICATE_CAP + 20}"
        else:
            return {"field": field, "value": value, "mode": mode, "count": 0, "results": []}

    try:
        con = _# language: Python 3.11, target: Vercel serverless (AWS Lambda runtime)
# DuckDB runs in /tmp — cold start ~8-15s, warm ~1s. httpfs pulls parquet from HF over HTTPS.
import asyncio
import json
import os
import time
import threading
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor

import duckdb
from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware

# ==================== CONFIG ====================
HF_INDEX_BASE = os.environ.get(
    "ICMR_HF_INDEX_BASE",
    "https://huggingface.co/datasets/Kzr0xx/icrm-hitek-full-db-mixed/resolve/main",
).rstrip("/")

PARALLELISM = int(os.environ.get("ICMR_PARALLEL", "2"))
THREADS_PER_CONN = int(os.environ.get("ICMR_THREADS_PER_CONN", "2"))
DUPLICATE_CAP = 2
TMP = "/tmp"

SEARCH_FIELDS = [
    "name", "fathersName", "phoneNumber", "aadharNumber", "otherNumber",
    "address", "district", "pincode", "state", "town",
]
NUMBER_FIELDS = ["phoneNumber", "aadharNumber", "otherNumber"]

REMOTE_INDEXES = {
    "phone": [f"{HF_INDEX_BASE}/idx_phone.{i}.parquet" for i in range(7)],
    "aadhar": [f"{HF_INDEX_BASE}/idx_aadhar.{i}.parquet" for i in range(7)],
}

DEV = "INSOUL"
MAKER = "JOCKER"

# ==================== APP ====================
app = FastAPI(
    title="ICMR + HITEK Search API",
    description="Search through 2.5 billion records — Vercel serverless",
    version="2.1.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== DUCKDB POOL ====================
_conns: list = []
_conns_lock = threading.Lock()
_thread_local = threading.local()
pool = ThreadPoolExecutor(max_workers=PARALLELISM, thread_name_prefix="duck")

LOAD_STATUS = {
    "phone_loaded": False,
    "aadhar_loaded": False,
    "loading": False,
    "load_start_time": None,
    "load_end_time": None,
    "error": None,
}

def _idx_ready(kind: str) -> bool:
    if kind == "phone":
        return LOAD_STATUS["phone_loaded"]
    if kind == "aadhar":
        return LOAD_STATUS["aadhar_loaded"]
    return False

def _new_conn() -> duckdb.DuckDBPyConnection:
    os.makedirs(f"{TMP}/duckdb_extensions", exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET home_directory='{TMP}'")
    con.execute(f"SET extension_directory='{TMP}/duckdb_extensions'")
    con.execute("INSTALL parquet; LOAD parquet;")
    con.execute("INSTALL httpfs; LOAD httpfs;")

    for kind, urls in REMOTE_INDEXES.items():
        view = f"people_{kind}"
        try:
            lst = ", ".join(f"'{u}'" for u in urls)
            con.execute(
                f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM read_parquet([{lst}])"
            )
            if kind == "phone":
                LOAD_STATUS["phone_loaded"] = True
            elif kind == "aadhar":
                LOAD_STATUS["aadhar_loaded"] = True
            print(f"✅ Loaded {kind} index")
        except Exception as e:
            print(f"❌ Failed to load {kind} index: {e}")
            LOAD_STATUS["error"] = str(e)

    con.execute(f"SET threads = {THREADS_PER_CONN}")
    return con

def _thread_id() -> int:
    tid = getattr(_thread_local, "id", None)
    if tid is None:
        with _conns_lock:
            tid = len(_conns)
            _thread_local.id = tid
    return tid

def _get_conn() -> duckdb.DuckDBPyConnection:
    ident = _thread_id()
    with _conns_lock:
        while len(_conns) <= ident:
            _conns.append(_new_conn())
    return _conns[ident]

# ==================== SEARCH ====================
def _person_key(row: dict) -> tuple:
    ph = (row.get("phoneNumber") or "").strip()
    ad = (row.get("aadharNumber") or "").strip()
    if ph or ad:
        return (ph, ad)
    return (row.get("name") or "").strip(), (row.get("fathersName") or "").strip()

def _connected_numbers(row: dict) -> list:
    connected, seen = [], set()
    for field in NUMBER_FIELDS:
        raw = row.get(field)
        if raw is None:
            continue
        value = str(raw).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        connected.append({"field": field, "value": value})
    return connected

def _cap_duplicates(rows: list) -> list:
    seen: dict = {}
    out = []
    for r in rows:
        k = _person_key(r)
        n = seen.get(k, 0)
        if n < DUPLICATE_CAP:
            seen[k] = n + 1
            record = dict(r)
            record.pop("source", None)
            record["connected_numbers"] = _connected_numbers(record)
            out.append(record)
    return out

def _run_field_search(field: str, value: str, mode: str, limit: int) -> dict:
    if field not in SEARCH_FIELDS:
        raise ValueError(f"Unknown field: {field}")
    v = value.replace("'", "''")

    if mode == "exact":
        if field == "phoneNumber" and _idx_ready("phone"):
            view = "people_phone"
        elif field == "aadharNumber" and _idx_ready("aadhar"):
            view = "people_aadhar"
        else:
            return {"field": field, "value": value, "mode": mode, "count": 0, "results": []}
        sql = f"SELECT * FROM {view} WHERE {field} = '{v}' LIMIT {limit * DUPLICATE_CAP + 20}"
    else:
        if field == "phoneNumber" and _idx_ready("phone"):
            view = "people_phone"
            v2 = v.replace("%", r"\%").replace("_", r"\_")
            sql = f"SELECT * FROM {view} WHERE {field} ILIKE '%{v2}%' ESCAPE '\\' LIMIT {limit * DUPLICATE_CAP + 20}"
        else:
            return {"field": field, "value": value, "mode": mode, "count": 0, "results": []}

    try:
        con = _get_conn()
        rows = con.execute(sql).fetchall()
        cols = [d[0] for d in con.description]
        results = _cap_duplicates([dict(zip(cols, r)) for r in rows])[:limit]
        return {"field": field, "value": value, "mode": mode, "count": len(results), "results": results}
    except Exception as e:
        print(f"Search error: {e}")
        return {"field": field, "value": value, "mode": mode, "count": 0, "results": [], "error": str(e)}

def _unified_search(q: str, limit: int = 10) -> dict:
    q = q.strip()
    is_num = q.isdigit() and len(q) >= 8
    if not is_num:
        return {"query": q, "searched_fields": [], "count": 0, "results": []}

    all_rows, searched = [], []
    if _idx_ready("phone"):
        r = _run_field_search("phoneNumber", q, "exact", limit)
        all_rows.extend(r["results"])
        searched.append("phoneNumber")
    if not all_rows and _idx_ready("aadhar"):
        r = _run_field_search("aadharNumber", q, "exact", limit)
        all_rows.extend(r["results"])
        searched.append("aadharNumber")
    all_rows = _cap_duplicates(all_rows)[:limit]
    return {"query": q, "searched_fields": searched, "count": len(all_rows), "results": all_rows}

# ==================== ENDPOINTS ====================
@app.get("/")
async def root():
    return {
        "app": "ICMR + HITEK Search API",
        "version": "2.1.0",
        "status": "running",
        "records": "2,504,793,870 (2.5 Billion)",
        "indexes": {"phone": _idx_ready("phone"), "aadhar": _idx_ready("aadhar")},
        "load_status": LOAD_STATUS,
        "endpoints": {
            "search": "/search?q=<query>&mode=<exact|contains>&limit=<number>",
            "health": "/health",
            "docs": "/docs",
            "stats": "/stats",
        },
        "developer": DEV,
        "maker": MAKER,
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy" if not LOAD_STATUS["error"] else "degraded",
        "indexes_loaded": {"phone": _idx_ready("phone"), "aadhar": _idx_ready("aadhar")},
        "loading": LOAD_STATUS["loading"],
        "load_status": LOAD_STATUS,
        "timestamp": time.time(),
        "developer": DEV,
        "maker": MAKER,
    }

@app.get("/stats")
async def get_stats():
    return {
        "load_status": LOAD_STATUS,
        "indexes": {"phone": _idx_ready("phone"), "aadhar": _idx_ready("aadhar")},
        "search_fields": SEARCH_FIELDS,
        "number_fields": NUMBER_FIELDS,
        "developer": DEV,
        "maker": MAKER,
    }

@app.get("/search")
async def search(
    q: Optional[str] = Query(None),
    mobile: Optional[str] = Query(None),
    field: Optional[str] = Query(None),
    mode: str = Query("exact"),
    limit: int = Query(10, ge=1, le=1000),
    timeout: int = Query(30, ge=5, le=120),
    pretty: bool = Query(True),
):
    start_time = time.time()
    query = (q or mobile or "").strip()
    if not query:
        raise HTTPException(status_code=422, detail="Provide 'q' or 'mobile'")

    if not _idx_ready("phone") and not _idx_ready("aadhar"):
        return Response(
            content=json.dumps({
                "error": "Indexes loading. Please wait...",
                "query": query,
                "load_status": LOAD_STATUS,
                "retry_after": "30 seconds",
                "developer": DEV,
                "maker": MAKER,
            }, indent=2 if pretty else None),
            status_code=503,
            media_type="application/json",
        )

    try:
        loop = asyncio.get_running_loop()
        if field:
            data = await asyncio.wait_for(
                loop.run_in_executor(pool, _run_field_search, field, query, mode, limit),
                timeout=timeout,
            )
        else:
            data = await asyncio.wait_for(
                loop.run_in_executor(pool, _unified_search, query, limit),
                timeout=timeout,
            )

        response = {
            "success": data.get("count", 0) > 0,
            "query": query,
            "mode": mode,
            "count": data.get("count", 0),
            "results": data.get("results", []),
            "searched_fields": data.get("searched_fields", []),
            "execution_time": round(time.time() - start_time, 3),
            "indexes_used": data.get("searched_fields", []),
            "developer": DEV,
            "maker": MAKER,
        }
        return Response(
            content=json.dumps(response, indent=2 if pretty else None, ensure_ascii=False),
            media_type="application/json",
        )
    except asyncio.TimeoutError:
        return Response(
            content=json.dumps({
                "error": f"Search timed out after {timeout}s",
                "query": query,
                "execution_time": round(time.time() - start_time, 3),
                "developer": DEV,
                "maker": MAKER,
            }, indent=2 if pretty else None),
            status_code=504,
            media_type="application/json",
        )
    except Exception as e:
        return Response(
            content=json.dumps({
                "error": str(e),
                "query": query,
                "execution_time": round(time.time() - start_time, 3),
                "developer": DEV,
                "maker": MAKER,
            }, indent=2 if pretty else None),
            status_code=500,
            media_type="application/json",
        )

@app.post("/search/batch")
async def batch_search(queries: List[str], limit: int = 10, timeout: int = 60):
    if not queries:
        raise HTTPException(400, "queries list cannot be empty")
    if len(queries) > 20:
        raise HTTPException(400, "Maximum 20 queries per batch")

    results = {}
    loop = asyncio.get_running_loop()
    for query in queries:
        try:
            data = await asyncio.wait_for(
                loop.run_in_executor(pool, _unified_search, query, limit),
                timeout=timeout,
            )
            results[query] = data
        except asyncio.TimeoutError:
            results[query] = {"error": f"Timed out after {timeout}s"}
        except Exception as e:
            results[query] = {"error": str(e)}

    return {
        "total_queries": len(queries),
        "results": results,
        "successful": sum(1 for r in results.values() if "error" not in r),
        "developer": DEV,
        "maker": MAKER,
    }

# ==================== STARTUP ====================
@app.on_event("startup")
async def startup_event():
    print("🔥 Starting API...")
    LOAD_STATUS["loading"] = True
    LOAD_STATUS["load_start_time"] = time.time()
    loop = asyncio.get_running_loop()
    loop.run_in_executor(pool, _preload_indexes)
    print("✅ API ready (indexes loading in background)")

def _preload_indexes():
    try:
        _new_conn()
        LOAD_STATUS["load_end_time"] = time.time()
        LOAD_STATUS["loading"] = False
        duration = LOAD_STATUS["load_end_time"] - LOAD_STATUS["load_start_time"]
        print(f"✅ Indexes loaded in {duration:.2f}s")
    except Exception as e:
        LOAD_STATUS["error"] = str(e)
        LOAD_STATUS["loading"] = False
        print(f"❌ Failed: {e}")

# Vercel ASGI handler
handler = app￼Enterget_conn()
        rows = con.execute(sql).fetchall()
        cols = [d[0] for d in con.description]
        results = _cap_duplicates([dict(zip(cols, r)) for r in rows])[:limit]
        return {"field": field, "value": value, "mode": mode, "count": len(results), "results": results}
    except Exception as e:
        print(f"Search error: {e}")
        return {"field": field, "value": value, "mode": mode, "count": 0, "results": [], "error": str(e)}

def _unified_search(q: str, limit: int = 10) -> dict:
    q = q.strip()
    is_num = q.isdigit() and len(q) >= 8
    if not is_num:
        return {"query": q, "searched_fields": [], "count": 0, "results": []}

    all_rows, searched = [], []
    if _idx_ready("phone"):
        r = _run_field_search("phoneNumber", q, "exact", limit)
        all_rows.extend(r["results"])
        searched.append("phoneNumber")
    if not all_rows and _idx_ready("aadhar"):
        r = _run_field_search("aadharNumber", q, "exact", limit)
        all_rows.extend(r["results"])
        searched.append("aadharNumber")
    all_rows = _cap_duplicates(all_rows)[:limit]
    return {"query": q, "searched_fields": searched, "count": len(all_rows), "results": all_rows}

# ==================== ENDPOINTS ====================
@app.get("/")
async def root():
    return {
        "app": "ICMR + HITEK Search API",
        "version": "2.1.0",
        "status": "running",
        "records": "2,504,793,870 (2.5 Billion)",
        "indexes": {"phone": _idx_ready("phone"), "aadhar": _idx_ready("aadhar")},
        "load_status": LOAD_STATUS,
        "endpoints": {
            "search": "/search?q=<query>&mode=<exact|contains>&limit=<number>",
            "health": "/health",
            "docs": "/docs",
            "stats": "/stats",
        },
        "developer": DEV,
        "maker": MAKER,
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy" if not LOAD_STATUS["error"] else "degraded",
        "indexes_loaded": {"phone": _idx_ready("phone"), "aadhar": _idx_ready("aadhar")},
        "loading": LOAD_STATUS["loading"],
        "load_status": LOAD_STATUS,
        "timestamp": time.time(),
        "developer": DEV,
        "maker": MAKER,
    }

@app.get("/stats")
async def get_stats():
    return {
        "load_status": LOAD_STATUS,
        "indexes": {"phone": _idx_ready("phone"), "aadhar": _idx_ready("aadhar")},
        "search_fields": SEARCH_FIELDS,
        "number_fields": NUMBER_FIELDS,
        "developer": DEV,
        "maker": MAKER,
    }

@app.get("/search")
async def search(
    q: Optional[str] = Query(None),
    mobile: Optional[str] = Query(None),
    field: Optional[str] = Query(None),
    mode: str = Query("exact"),
    limit: int = Query(10, ge=1, le=1000),
    timeout: int = Query(30, ge=5, le=120),
    pretty: bool = Query(True),
):
    start_time = time.time()
    query = (q or mobile or "").strip()
    if not query:
        raise HTTPException(status_code=422, detail="Provide 'q' or 'mobile'")

    if not _idx_ready("phone") and not _idx_ready("aadhar"):
        return Response(
            content=json.dumps({
                "error": "Indexes loading. Please wait...",
                "query": query,
                "load_status": LOAD_STATUS,
                "retry_after": "30 seconds",
                "developer": DEV,
                "maker": MAKER,
            }, indent=2 if pretty else None),
            status_code=503,
            media_type="application/json",
        )

    try:
