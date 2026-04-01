"""
NL2SQL — FastAPI Application
Air-gapped, read-only, industry-grade Natural Language to SQL.

ENDPOINTS:
  POST /api/upload      — Upload CSV files to create database
  POST /api/query       — Submit NL query → get SQL + results + NL answer
  GET  /api/schema      — View current database schema
  GET  /api/health      — Health check
  GET  /api/tables/{name}/preview — Preview table data
  GET  /api/feedback/stats — Evaluation + feedback loop stats
  GET  /api/feedback/flagged — View flagged queries for review
  POST /api/feedback/approve — Approve a flagged query
  GET  /                — Serve frontend UI

NO endpoints for DELETE, UPDATE, or any destructive operations.
"""
import asyncio
import logging
import time
import zipfile
import io
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional

from config import Config
from core.schema_introspector import SchemaIntrospector
from core.query_preprocessor import QueryPreprocessor
from core.schema_linker import SchemaLinker
from core.sql_generator import SQLGenerator
from core.sql_executor import SQLExecutor
from core.answer_synthesizer import AnswerSynthesizer
from core.cache import QueryCache
from core.feedback_loop import FeedbackLoop
from core.value_grounder import value_grounder
from core.sql_ranker import sql_ranker
from core.query_classifier import classify_query
from core.query_logger import query_logger
from core.few_shot_retriever import few_shot_retriever
try:
    from core.column_pruner import column_pruner
    HAS_PRUNER = True
except Exception:
    HAS_PRUNER = False

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger("nl2sql")

# ── FastAPI App ──────────────────────────────────────────────
app = FastAPI(
    title="NL2SQL — Natural Language to SQL",
    description="Air-gapped, read-only NL-to-SQL engine",
    version="1.0.0",
)


# ── Global Error Handler (prevents raw 500 errors) ──────────
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Return JSON errors instead of raw 'Internal Server Error'."""
    logger.error(f"Unhandled error: {type(exc).__name__}: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "question": "",
            "preprocessed_question": "",
            "linked_question": "",
            "sql": "",
            "valid": False,
            "results": {"success": False, "columns": [], "rows": [],
                        "row_count": 0, "error": str(exc)},
            "answer": f"Error: {type(exc).__name__}: {str(exc)}",
            "confidence": 0,
            "query_type": "error",
            "cached": False,
            "generation_time_ms": 0,
            "execution_time_ms": 0,
            "total_time_ms": 0,
            "attempts": 0,
            "model_used": "",
            "evaluation": {},
        },
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],  # Read-only: no PUT/DELETE
    allow_headers=["*"],
)

# ── Core Components ──────────────────────────────────────────
introspector = SchemaIntrospector()
preprocessor = QueryPreprocessor()
schema_linker = SchemaLinker()
generator = SQLGenerator()
executor = SQLExecutor()
synthesizer = AnswerSynthesizer()
cache = QueryCache(
    max_size=Config.CACHE_MAX_SIZE,
    ttl_seconds=Config.CACHE_TTL_SECONDS,
)
feedback = FeedbackLoop()

# ── Load golden pairs for dynamic few-shot retrieval ──
few_shot_retriever.load()

# ── Pre-warm models (keep both in memory to avoid swap latency) ──
from models.llm_manager import llm_manager as _llm
try:
    logger.info("Pre-warming LLM models (keep_alive=-1)...")
    _llm.warmup_models()
    logger.info("Models pre-warmed — swap latency minimized")
except Exception as _e:
    logger.warning(f"Model warmup failed (non-critical): {_e}")

# ── State ────────────────────────────────────────────────────
app_state = {
    "schema_loaded": False,
    "tables": [],
    "total_rows": 0,
    "query_count": 0,
    "upload_time": None,
}


# ── Request/Response Models ──────────────────────────────────
class QueryRequest(BaseModel):
    question: str
    use_cache: bool = True


class QueryResponse(BaseModel):
    question: str
    preprocessed_question: str
    linked_question: str = ""
    sql: str
    valid: bool
    results: dict
    answer: str
    confidence: float
    query_type: str
    cached: bool
    generation_time_ms: int
    execution_time_ms: int
    total_time_ms: int
    attempts: int
    model_used: str
    evaluation: dict = {}


# ── Mount static files ───────────────────────────────────────
static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


# ── Routes ───────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    """Serve the main UI."""
    index_path = static_dir / "index.html"
    if index_path.exists():
        return HTMLResponse(index_path.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>NL2SQL</h1><p>Static files not found.</p>")


@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "schema_loaded": app_state["schema_loaded"],
        "tables": len(app_state["tables"]),
        "total_rows": app_state["total_rows"],
        "query_count": app_state["query_count"],
        "cache_stats": cache.stats,
    }


@app.post("/api/upload")
async def upload_csvs(files: list[UploadFile] = File(...)):
    """
    Upload CSV or ZIP files to create the database.
    Each CSV becomes a table (named after the file).
    ZIP files are extracted — all CSVs inside are loaded together.
    """
    if not files:
        raise HTTPException(400, "No files provided")

    csv_files = {}
    for f in files:
        if not f.filename:
            continue
        content = await f.read()
        fname = f.filename.lower()

        if fname.endswith(".zip"):
            # Extract CSVs from ZIP
            try:
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    for entry in zf.namelist():
                        if entry.lower().endswith(".csv") and not entry.startswith("__MACOSX"):
                            csv_files[entry] = zf.read(entry)
                            logger.info(f"Extracted from ZIP: {entry}")
            except zipfile.BadZipFile:
                raise HTTPException(400, f"{f.filename} is not a valid ZIP file")
        elif fname.endswith(".csv"):
            csv_files[f.filename] = content

    if not csv_files:
        raise HTTPException(400, "No CSV files found (upload .csv or .zip containing CSVs)")

    try:
        # Introspect schema from CSVs
        metadata = introspector.load_from_csvs(csv_files)

        # Set executor connection
        executor.set_connection(metadata.db_connection)
        value_grounder.set_connection(metadata.db_connection)
        sql_ranker.set_connection(metadata.db_connection)

        # Clear previous cache
        cache.clear()

        # Update state
        app_state["schema_loaded"] = True
        app_state["tables"] = metadata.tables
        app_state["total_rows"] = sum(metadata.row_counts.values())
        app_state["upload_time"] = time.strftime("%Y-%m-%d %H:%M:%S")

        # Build schema summary
        schema_summary = []
        for table in metadata.tables:
            cols = metadata.columns.get(table, [])
            col_names = [c.name for c in cols]
            row_count = metadata.row_counts.get(table, 0)
            schema_summary.append({
                "table": table,
                "columns": col_names,
                "column_details": [
                    {
                        "name": c.name,
                        "type": c.dtype,
                        "is_pk": c.is_pk,
                        "fk_ref": c.fk_ref,
                        "samples": c.sample_values[:3],
                    }
                    for c in cols
                ],
                "row_count": row_count,
            })

        relationships = [
            {
                "from": f"{r.from_table}.{r.from_column}",
                "to": f"{r.to_table}.{r.to_column}",
                "type": r.rel_type,
            }
            for r in metadata.relationships
        ]

        logger.info(
            f"Loaded {len(metadata.tables)} tables with "
            f"{app_state['total_rows']} total rows"
        )

        return {
            "success": True,
            "tables": schema_summary,
            "relationships": relationships,
            "total_rows": app_state["total_rows"],
            "message": f"Successfully loaded {len(metadata.tables)} table(s)",
        }

    except Exception as e:
        logger.error(f"Upload error: {e}")
        raise HTTPException(500, f"Error processing CSV files: {str(e)}")


@app.post("/api/load-army-data")
async def load_army_data():
    """
    Load the pre-built avalanche prediction dataset from test_data/.
    No file upload needed — loads directly from the backend.
    """
    csv_path = Path(__file__).parent / "test_data" / "avalanche_data.csv"
    if not csv_path.exists():
        raise HTTPException(404, "Army dataset not found at test_data/avalanche_data.csv")

    try:
        content = csv_path.read_bytes()
        csv_files = {"avalanche_data.csv": content}

        metadata = introspector.load_from_csvs(csv_files)
        executor.set_connection(metadata.db_connection)
        value_grounder.set_connection(metadata.db_connection)
        sql_ranker.set_connection(metadata.db_connection)
        cache.clear()

        app_state["schema_loaded"] = True
        app_state["tables"] = metadata.tables
        app_state["total_rows"] = sum(metadata.row_counts.values())
        app_state["upload_time"] = time.strftime("%Y-%m-%d %H:%M:%S")

        schema_summary = []
        for table in metadata.tables:
            cols = metadata.columns.get(table, [])
            col_names = [c.name for c in cols]
            row_count = metadata.row_counts.get(table, 0)
            schema_summary.append({
                "table": table,
                "columns": col_names,
                "column_details": [
                    {
                        "name": c.name,
                        "type": c.dtype,
                        "is_pk": c.is_pk,
                        "fk_ref": c.fk_ref,
                        "samples": c.sample_values[:3],
                    }
                    for c in cols
                ],
                "row_count": row_count,
            })

        relationships = [
            {
                "from": f"{r.from_table}.{r.from_column}",
                "to": f"{r.to_table}.{r.to_column}",
                "type": r.rel_type,
            }
            for r in metadata.relationships
        ]

        logger.info(f"Army dataset loaded: {len(metadata.tables)} tables, {app_state['total_rows']} rows")

        return {
            "success": True,
            "tables": schema_summary,
            "relationships": relationships,
            "total_rows": app_state["total_rows"],
            "message": f"Army avalanche dataset loaded — {app_state['total_rows']} rows, 152 columns",
        }

    except Exception as e:
        logger.error(f"Error loading army data: {e}")
        raise HTTPException(500, f"Error loading army dataset: {str(e)}")


@app.get("/api/download-army-data")
async def download_army_data():
    """Download the avalanche prediction CSV file."""
    csv_path = Path(__file__).parent / "test_data" / "avalanche_data.csv"
    if not csv_path.exists():
        raise HTTPException(404, "Army dataset not found")
    from fastapi.responses import FileResponse
    return FileResponse(
        path=str(csv_path),
        filename="avalanche_data.csv",
        media_type="text/csv",
    )


@app.post("/api/query", response_model=QueryResponse)
async def query(req: QueryRequest):
    """
    Submit a natural language question.
    Returns SQL, execution results, and NL answer.
    """
    if not app_state["schema_loaded"]:
        raise HTTPException(
            400,
            "No database loaded. Please upload CSV files first."
        )

    total_start = time.time()
    app_state["query_count"] += 1

    question = req.question.strip()
    if not question:
        raise HTTPException(400, "Question cannot be empty")

    # ── 1. CACHE CHECK (instant) ─────────────────────────────
    if req.use_cache:
        cached = cache.get(question)
        if cached:
            cached["cached"] = True
            cached["total_time_ms"] = int((time.time() - total_start) * 1000)
            return QueryResponse(**cached)

    metadata = introspector.metadata
    single_table = len(metadata.tables) == 1

    # ── 2. PREPROCESS + CLASSIFY (instant, no LLM) ───────────
    preprocessed = preprocessor.preprocess(question)
    business_hints = preprocessor.get_business_hints(preprocessed)
    query_type = classify_query(preprocessed)
    logger.info(f"Query type: {query_type['types']}, limit={query_type['limit_n']}")

    # Build query hints from classifier
    query_hints_parts = []
    for hint in query_type["hints"]:
        query_hints_parts.append(f"- {hint}")
    for rule in query_type.get("correlated", []):
        query_hints_parts.append(f"- For '{rule['keyword']}': use pattern {rule['pattern']}")
    query_hints_text = "### Query Type Hints\n" + "\n".join(query_hints_parts) if query_hints_parts else ""

    # ── 3. SCHEMA LINKING (domain dict, instant, no LLM) ─────
    linked_question = preprocessed
    if single_table:
        try:
            mapped, _ = schema_linker._dict_lookup(preprocessed)
            if mapped:
                for term, (col, cond) in mapped.items():
                    if col:
                        linked_question = linked_question.replace(term, col)
                logger.info(f"Schema linked (dict): {len(mapped)} terms")
        except Exception as link_err:
            logger.warning(f"Dict linking error: {link_err}")
    else:
        try:
            linked = await schema_linker.link(preprocessed, metadata)
            linked_question = linked.get("resolved_question", preprocessed) or preprocessed
        except Exception:
            pass

    # ── 4. COLUMN PRUNING (instant, no LLM) ──────────────────
    total_cols = sum(len(cols) for cols in metadata.columns.values())
    if HAS_PRUNER and total_cols > 50:
        pruned = column_pruner.prune(preprocessed)
        schema_text = pruned["schema_text"]
        selected_cols = [c["name"] for c in pruned["selected_columns"]]
        logger.info(f"Pruned: {pruned['pruned_count']}/{pruned['total_columns']} cols")
    else:
        target_tables = list(metadata.tables)
        augmented_tables = metadata.table_graph.get_augmented_tables(
            target_tables, hops=Config.FK_AUGMENTATION_HOPS
        )
        schema_text = introspector.get_schema_text(list(augmented_tables))
        selected_cols = []
        for t in augmented_tables:
            selected_cols.extend(c.name for c in metadata.columns.get(t, []))

    # ── 5. VALUE GROUNDING (DB queries, ~1ms) ────────────────
    grounding_text = ""
    if selected_cols:
        # Ground only key columns mentioned in the query
        key_cols = [c for c in selected_cols if any(
            kw in c.lower() for kw in
            ["prediction", "risk", "elevation", "zone", "flag", "positive"]
        )][:10]
        if key_cols:
            table_name = list(metadata.tables)[0] if single_table else "avalanche_data"
            grounding = value_grounder.ground_values(key_cols, table_name)
            grounding_text = value_grounder.build_grounding_text(grounding)
            logger.info(f"Grounded {len(grounding)} columns with actual values")

    # ── 5.5 TEMPLATE CACHE CHECK (instant, 0 LLM calls) ────────
    template_sql = few_shot_retriever.get_template_match(preprocessed)
    if template_sql:
        from core.sql_validator import validate_sql as _val
        tv = _val(template_sql, metadata)
        if tv.passed:
            logger.info("Template cache HIT — returning cached SQL")
            exec_result = executor.execute(template_sql)
            if exec_result.get("success"):
                answer = await synthesizer.synthesize(
                    question=preprocessed, sql=template_sql,
                    results=exec_result, use_llm=False,
                )
                total_time = int((time.time() - total_start) * 1000)
                response_data = {
                    "question": question, "preprocessed_question": preprocessed,
                    "linked_question": linked_question, "sql": template_sql,
                    "valid": True, "results": exec_result, "answer": answer,
                    "confidence": 0.95, "query_type": "template_cache",
                    "cached": False, "generation_time_ms": 0,
                    "execution_time_ms": exec_result.get("execution_time_ms", 0),
                    "total_time_ms": total_time, "attempts": 0,
                    "model_used": "template_cache", "evaluation": {},
                }
                cache.put(question, response_data)
                query_logger.log(question=question, sql=template_sql,
                    success=True, execution_time_ms=total_time, model_used="template_cache")
                return QueryResponse(**response_data)

    # ── 5.6 DYNAMIC FEW-SHOT RETRIEVAL (~1ms) ────────────────
    dynamic_examples = few_shot_retriever.build_few_shot_text(preprocessed, k=2)
    logger.info(f"Dynamic few-shot: {len(dynamic_examples)} chars of examples")

    # ── 6. SQL GENERATION (k=2 candidates, sqlcoder) ─────────
    gen_result = await generator.generate(
        question=linked_question,
        schema_text=schema_text,
        business_hints=business_hints,
        schema_metadata=metadata,
        value_grounding=grounding_text,
        query_hints=query_hints_text,
        dynamic_examples=dynamic_examples,
    )

    generation_time = gen_result["generation_time_ms"]

    # ── 7. RANK CANDIDATES (code-only, ~5ms) ─────────────────
    valid_candidates = gen_result.get("all_candidates", [])
    valid_sqls = [c["sql"] for c in valid_candidates if c.get("valid")]

    if len(valid_sqls) > 1:
        rank_result = sql_ranker.rank(valid_sqls, question, metadata)
        sql = rank_result["best_sql"]
        confidence = rank_result["confidence"]
        logger.info(f"Ranked {len(valid_sqls)} candidates, best score: {rank_result['score']}")
    elif valid_sqls:
        sql = valid_sqls[0]
        confidence = gen_result["confidence"]
    else:
        sql = gen_result["sql"]
        confidence = gen_result["confidence"]

    # ── 8. EXECUTE ───────────────────────────────────────────
    exec_result = {"success": False, "error": "SQL validation failed",
                   "columns": [], "rows": [], "row_count": 0,
                   "execution_time_ms": 0}

    if gen_result["valid"] and sql:
        exec_result = executor.execute(sql)

        # Execution-based self-correction: if error, retry with feedback
        if not exec_result["success"]:
            db_error = exec_result.get("error", "")
            logger.info(f"Execution error: {db_error[:100]}. Retrying...")
            try:
                from models.llm_manager import llm_manager
                from core.sql_validator import extract_clean_sql, validate_sql
                fix_prompt = (
                    f"The SQL below failed when executed:\n\n{sql}\n\n"
                    f"Database error: {db_error}\n\n"
                    f"Question: \"{question}\"\n\n{schema_text}\n\n"
                    f"Fix the SQL. Use ONLY columns from the CREATE TABLE.\n"
                    f"Return ONLY the corrected SQL:"
                )
                raw_fix = await llm_manager.generate(
                    prompt=fix_prompt,
                    model=Config.SQL_MODEL,
                    temperature=0.1,
                )
                fixed_sql = extract_clean_sql(raw_fix)
                val = validate_sql(fixed_sql, metadata)
                if val.passed:
                    sql = fixed_sql
                    exec_result = executor.execute(sql)
                    logger.info(f"Self-correction {'succeeded' if exec_result['success'] else 'failed'}")
            except Exception as fix_err:
                logger.error(f"Self-correction failed: {fix_err}")

        # Empty result retry: relax filters
        if exec_result["success"] and exec_result.get("row_count", 0) == 0:
            logger.info("Query returned 0 rows — may need relaxed filters")

    execution_time = exec_result.get("execution_time_ms", 0)

    # ── 9. ANSWER (smart template, instant) ──────────────────
    answer = ""
    if exec_result.get("success"):
        answer = await synthesizer.synthesize(
            question=preprocessed, sql=sql,
            results=exec_result, use_llm=False,
        )
    elif not gen_result["valid"]:
        answer = (
            f"I couldn't generate a valid SQL query. "
            f"Error: {gen_result.get('validation_error', 'Unknown')}. "
            f"Please try rephrasing."
        )
    else:
        answer = (
            f"Query error: {exec_result.get('error', 'Unknown')}. "
            f"Please try rephrasing your question."
        )

    total_time = int((time.time() - total_start) * 1000)

    # ── Evaluation: SKIP for now (saves ~10-15s model swap) ──
    evaluation = {}

    response_data = {
        "question": question,
        "preprocessed_question": preprocessed,
        "linked_question": linked_question,
        "sql": sql,
        "valid": gen_result["valid"] and exec_result.get("success", False),
        "results": exec_result,
        "answer": answer,
        "confidence": gen_result["confidence"],
        "query_type": "nl2sql",
        "cached": False,
        "generation_time_ms": generation_time,
        "execution_time_ms": execution_time,
        "total_time_ms": total_time,
        "attempts": gen_result["attempts"],
        "model_used": gen_result["model_used"],
        "evaluation": evaluation,
    }

    # Cache successful results
    if exec_result.get("success"):
        cache.put(question, response_data)

    return QueryResponse(**response_data)


@app.get("/api/schema")
async def get_schema():
    """Get current database schema metadata."""
    if not app_state["schema_loaded"]:
        return {"loaded": False, "tables": []}

    metadata = introspector.metadata
    tables = []
    for table in metadata.tables:
        cols = metadata.columns.get(table, [])
        tables.append({
            "name": table,
            "row_count": metadata.row_counts.get(table, 0),
            "columns": [
                {
                    "name": c.name,
                    "type": c.dtype,
                    "is_pk": c.is_pk,
                    "fk_ref": c.fk_ref,
                    "samples": c.sample_values[:3],
                }
                for c in cols
            ],
        })

    relationships = [
        {
            "from": f"{r.from_table}.{r.from_column}",
            "to": f"{r.to_table}.{r.to_column}",
        }
        for r in metadata.relationships
    ]

    return {
        "loaded": True,
        "tables": tables,
        "relationships": relationships,
    }


@app.get("/api/tables/{table_name}/preview")
async def preview_table(table_name: str):
    """Preview first 10 rows of a table."""
    if not app_state["schema_loaded"]:
        raise HTTPException(400, "No database loaded")
    return executor.get_table_preview(table_name, limit=10)


# ── Evaluation & Feedback Endpoints ──────────────────────────

@app.get("/api/feedback/stats")
async def feedback_stats():
    """Get evaluation and feedback loop statistics."""
    return {
        "feedback": feedback.get_stats(),
        "cache": cache.stats,
        "golden_pairs_loaded": len(golden_tester.pairs),
        "query_count": app_state["query_count"],
    }


@app.get("/api/feedback/flagged")
async def get_flagged():
    """View queries flagged for human review."""
    return {
        "flagged": feedback.flagged,
        "count": len(feedback.flagged),
    }


class ApproveRequest(BaseModel):
    index: int
    corrected_sql: str = None


@app.post("/api/feedback/approve")
async def approve_flagged(req: ApproveRequest):
    """Approve a flagged query (optionally with corrected SQL)."""
    result = feedback.approve_flagged(req.index, req.corrected_sql)
    return result


# ── Run ──────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting NL2SQL on {Config.HOST}:{Config.PORT}")
    uvicorn.run(app, host=Config.HOST, port=Config.PORT)
