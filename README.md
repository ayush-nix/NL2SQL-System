# NL2SQL — Avalanche Prediction Intelligence Engine

> **Air-gapped, read-only, industry-grade** Natural Language to SQL system for Indian Army avalanche safety operations.

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-green)
![Ollama](https://img.shields.io/badge/LLM-Ollama-orange)
![Security](https://img.shields.io/badge/Security-Read--Only-red)
![Architecture](https://img.shields.io/badge/Architecture-SOTA_2025-purple)

---

## What It Does

Enables military personnel to query a **152-column avalanche prediction dataset** using plain English — no SQL knowledge required. The system translates questions like *"Show me high-risk areas with heavy snowfall"* into precise SQL queries against satellite, seismic, meteorological, and terrain data.

---

## SOTA Architecture (2025)

Built on research from **CHESS**, **DIN-SQL**, **MAC-SQL**, **SDE-SQL**, and **VLDB 2025 Reasoning-SQL**.

```
User Query (vague, natural language)
    │
    ▼
┌──────────────────────────────────────────────────────────────────────┐
│ Layer 1: Query Understanding           [Brain: Mistral]             │
│   • Abbreviation expansion (NDSI, PGA, LST, etc.)                   │
│   • Domain dictionary: 50+ avalanche terms → SQL hints              │
│   • Confidence routing: SIMPLE / MODERATE / COMPLEX                 │
├──────────────────────────────────────────────────────────────────────┤
│ Layer 2: Schema Linking                [Brain: Mistral]             │
│   • Entity-to-column mapping from vague terms                       │
│   • Skipped for SIMPLE queries (40% latency reduction)              │
├──────────────────────────────────────────────────────────────────────┤
│ Layer 3: Column Pruning (NEW)          [BM25 Vectorless]            │
│   • CHESS-style schema selector: 152 → top-25 relevant columns     │
│   • Inverted index over semantic metadata (synonyms, ranges, units) │
│   • Zero embedding model needed — fully offline                     │
├──────────────────────────────────────────────────────────────────────┤
│ Layer 4: SQL Generation                [Coder: SQLCoder]            │
│   • Chain-of-Thought prompting with pruned schema                   │
│   • Few-shot examples from golden pairs + learned examples          │
├──────────────────────────────────────────────────────────────────────┤
│ Layer 5: 5-Pass Validation                                          │
│   • Syntax → Safety (SELECT whitelist) → Schema → JOINs → Critic   │
│   • Taxonomy-guided error correction (9 error categories)           │
├──────────────────────────────────────────────────────────────────────┤
│ Layer 6: Execution + Answer            [Read-only sandbox]          │
│   • 30s timeout, 10K row cap                                        │
│   • Natural language answer synthesis                                │
├──────────────────────────────────────────────────────────────────────┤
│ Layer 7: Evaluation + Feedback         [LLM Judge + Partial Rewards]│
│   • LLM-as-Judge: Faithfulness + Helpfulness (1-5)                  │
│   • VLDB 2025 Partial Rewards: Syntax + Schema + Execution + N-gram │
│   • High-scoring queries auto-become few-shot examples              │
└──────────────────────────────────────────────────────────────────────┘
    │
    ▼
Natural Language Answer + SQL + Results Table
```

---

## Key Innovations

| Feature | Technique | Reference |
|:--------|:----------|:----------|
| **Column Pruning** | BM25 inverted index over 152-column metadata | CHESS 2024, PageIndex |
| **Confidence Router** | Adaptive pipeline routing by query complexity | DIN-SQL |
| **Taxonomy Error Correction** | 9-category SQL error classifier with targeted fix hints | SQL-of-Thought |
| **Partial Reward Scoring** | 4 decomposed rewards: syntax, schema, execution, structure | VLDB 2025 Reasoning-SQL |
| **Semantic Metadata** | Rich column descriptions, units, ranges, synonyms | CHESS Schema Selector |
| **Dual-Model Architecture** | Brain (Mistral) + Coder (SQLCoder) separation | MAC-SQL |
| **Vectorless RAG** | No embedding model needed — keyword matching is more deterministic | PageIndex, BM25 |

---

## Quick Start

### Prerequisites
- Python 3.10+
- [Ollama](https://ollama.ai/) running locally

### Install
```bash
pip install -r requirements.txt
```

### Pull Models
```bash
ollama pull mistral          # Brain model (planning, schema linking, evaluation)
ollama pull sqlcoder          # Coder model (SQL generation)
```

### Run
```bash
python app.py
```
Open **http://localhost:8000** in your browser.

### Usage
1. **Upload CSV** — drag-and-drop `test_data/avalanche_data.csv`
2. **Ask questions** — *"Show high risk areas"*, *"Where was heavy snowfall with wind?"*
3. View generated SQL, results table, and natural language answer

---

## Project Structure

```
NL2SQL/
├── app.py                         # FastAPI orchestrator (all 7 layers)
├── config.py                      # Centralized configuration
├── requirements.txt               
├── core/
│   ├── schema_introspector.py     # Schema discovery (CSV → SQLite)
│   ├── query_preprocessor.py      # NL cleaning + abbreviation expansion
│   ├── schema_linker.py           # Entity-to-column mapping
│   ├── column_pruner.py           # BM25 column selector (CHESS pattern) ★ NEW
│   ├── confidence_router.py       # Adaptive complexity routing ★ NEW
│   ├── error_taxonomy.py          # 9-category error classifier ★ NEW
│   ├── sql_generator.py           # CoT SQL generation + self-correction
│   ├── sql_validator.py           # 5-pass whitelist validation
│   ├── sql_executor.py            # Read-only execution sandbox
│   ├── answer_synthesizer.py      # Results → NL answer
│   ├── evaluator.py               # LLM Judge + Partial Rewards ★ ENHANCED
│   ├── feedback_loop.py           # Auto-learning cycle
│   └── cache.py                   # LRU + TTL query cache
├── models/
│   └── llm_manager.py             # Ollama REST API client
├── utils/
│   ├── domain_dictionary.py       # Avalanche science terms + SQL hints ★ NEW
│   └── graph.py                   # FK relationship graph
├── data/
│   ├── column_metadata.json       # 152-column semantic metadata ★ NEW
│   └── golden_pairs.json          # Avalanche regression test pairs ★ NEW
├── static/
│   ├── index.html                 # Frontend UI
│   ├── style.css                  # Dark glassmorphism theme
│   └── app.js                     # Frontend logic
└── test_data/
    └── avalanche_data.csv         # 100-row realistic Himalayan data ★ NEW
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Frontend UI |
| `GET` | `/api/health` | Health check |
| `POST` | `/api/upload` | Upload CSV files |
| `POST` | `/api/query` | Submit NL question |
| `GET` | `/api/schema` | View database schema |
| `GET` | `/api/tables/{name}/preview` | Preview table data |
| `GET` | `/api/feedback/stats` | Evaluation statistics |

---

## Security

- **Whitelist-only SQL**: Only `SELECT` statements allowed — not a blacklist
- **5-pass validation**: Every generated query validated before execution
- **Read-only DB**: Production uses read-only connections
- **No destructive code**: Zero `os.remove`, `shutil.rmtree`, `subprocess` in codebase
- **Air-gapped**: No internet calls — all LLM inference via local Ollama
- **Encrypted locations**: lat/lon encoded for operational security
- **Timeout + row limits**: 30s timeout, 10K row cap

---

## Configuration

All settings in `config.py`:

| Variable | Default | Description |
|----------|---------|-------------|
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama server URL |
| `SQL_MODEL` | `sqlcoder:latest` | Coder model for SQL generation |
| `FAST_MODEL` | `mistral:latest` | Brain model for planning/evaluation |
| `HOST` | `0.0.0.0` | Server bind address |
| `PORT` | `8000` | Server port |

---

## Dataset: Avalanche Prediction (152 Columns)

| Group | Columns | Examples |
|:------|:--------|:---------|
| **Identity** (4) | id, encrypted_lat, encrypted_lon, prediction_date | |
| **Prediction** (5) | prediction, avalanche_probability, risk_scale, label | |
| **Satellite** (~20) | lst_day, ndsi, snowcover, soil_t1–t4, smp, ssm | Rolling averages (3d/7d/14d) |
| **Seismic** (~15) | pga, pgv, pgd, cav, arias_intensity, seismic_energy | |
| **Meteorological** (~35) | temp_2m, wind_speed, total_precipitation, surface_pressure | ERA5 reanalysis |
| **Snow** (~20) | snow_depth, snowfall, snow_density, snow_albedo, swe | |
| **Terrain** (6) | elevation, slope, aspect, tri, distance_to_ridge | |
| **Risk Indicators** (~20) | compound_risk_score, wind_loading_index, freeze_thaw_flag | |
| **Temporal** (~10) | hour_of_day, day_of_year, day_of_winter | Cyclical encodings |
| **Derived** (~20) | temp_change_24h, loading_rate_48h, rain_on_snow_ratio | |

---

## License

Internal use — Indian Army project.
