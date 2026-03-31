/* ═══════════════════════════════════════════════════════════════
   NL2SQL — Frontend Application Logic
   ═══════════════════════════════════════════════════════════════ */

// ── State ─────────────────────────────────────────────────────
const state = {
    schemaLoaded: false,
    tables: [],
    relationships: [],
    history: [],
    queryCount: 0,
    totalTime: 0,
    isQuerying: false,
};

// ── DOM References ────────────────────────────────────────────
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

const uploadZone = $("#uploadZone");
const fileInput = $("#fileInput");
const uploadContent = $("#uploadContent");
const uploadProgress = $("#uploadProgress");
const schemaSection = $("#schemaSection");
const schemaTree = $("#schemaTree");
const statsSection = $("#statsSection");
const historySection = $("#historySection");
const historyList = $("#historyList");
const welcomeScreen = $("#welcomeScreen");
const messagesContainer = $("#messages");
const chatArea = $("#chatArea");
const queryInput = $("#queryInput");
const sendBtn = $("#sendBtn");
const connectionStatus = $("#connectionStatus");
const sidebarToggle = $("#sidebarToggle");
const sidebar = $("#sidebar");

// ── Init ──────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
    initUpload();
    initInput();
    initSidebar();
    checkHealth();
});

// ── Sidebar Toggle ────────────────────────────────────────────
function initSidebar() {
    sidebarToggle.addEventListener("click", () => {
        sidebar.classList.toggle("collapsed");
    });
}

// ── File Upload ───────────────────────────────────────────────
function initUpload() {
    uploadZone.addEventListener("click", () => fileInput.click());

    uploadZone.addEventListener("dragover", (e) => {
        e.preventDefault();
        uploadZone.classList.add("drag-over");
    });

    uploadZone.addEventListener("dragleave", () => {
        uploadZone.classList.remove("drag-over");
    });

    uploadZone.addEventListener("drop", (e) => {
        e.preventDefault();
        uploadZone.classList.remove("drag-over");
        const files = Array.from(e.dataTransfer.files).filter(
            (f) => f.name.endsWith(".csv") || f.name.endsWith(".zip")
        );
        if (files.length) uploadFiles(files);
    });

    fileInput.addEventListener("change", () => {
        const files = Array.from(fileInput.files);
        if (files.length) uploadFiles(files);
        fileInput.value = "";
    });
}

async function uploadFiles(files) {
    uploadContent.style.display = "none";
    uploadProgress.style.display = "flex";

    const formData = new FormData();
    files.forEach((f) => formData.append("files", f));

    try {
        const res = await fetch("/api/upload", {
            method: "POST",
            body: formData,
        });

        if (!res.ok) {
            const err = await res.json();
            throw new Error(err.detail || "Upload failed");
        }

        const data = await res.json();
        onSchemaLoaded(data);
    } catch (err) {
        alert("Upload error: " + err.message);
    } finally {
        uploadContent.style.display = "block";
        uploadProgress.style.display = "none";
    }
}

function onSchemaLoaded(data) {
    state.schemaLoaded = true;
    state.tables = data.tables || [];
    state.relationships = data.relationships || [];

    // Update upload zone
    uploadContent.innerHTML = `
        <div class="upload-icon" style="color: var(--accent-green)">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <polyline points="20 6 9 17 4 12"/>
            </svg>
        </div>
        <p class="upload-text" style="color: var(--accent-green)">${state.tables.length} table(s) loaded</p>
        <p class="upload-hint">${data.total_rows?.toLocaleString() || 0} total rows · Click to re-upload</p>
    `;

    // Connection status
    connectionStatus.innerHTML = `
        <div class="status-dot online"></div>
        <span>${state.tables.length} tables · ${(data.total_rows || 0).toLocaleString()} rows</span>
    `;

    // Build schema tree
    buildSchemaTree(data.tables);
    schemaSection.style.display = "block";
    statsSection.style.display = "block";
    historySection.style.display = "block";
    updateStats();

    // Focus input
    queryInput.focus();
}

function buildSchemaTree(tables) {
    schemaTree.innerHTML = "";

    tables.forEach((t) => {
        const tableDiv = document.createElement("div");
        tableDiv.className = "schema-table";

        const header = document.createElement("div");
        header.className = "schema-table-header";
        header.innerHTML = `
            <span class="table-icon">▶</span>
            <span>${t.table || t.name}</span>
            <span class="row-count">${(t.row_count || 0).toLocaleString()} rows</span>
        `;

        const columnsDiv = document.createElement("div");
        columnsDiv.className = "schema-columns";

        const cols = t.column_details || t.columns || [];
        if (Array.isArray(cols) && cols.length > 0) {
            if (typeof cols[0] === "string") {
                cols.forEach((name) => {
                    const colEl = document.createElement("div");
                    colEl.className = "schema-col";
                    colEl.textContent = name;
                    columnsDiv.appendChild(colEl);
                });
            } else {
                cols.forEach((c) => {
                    const colEl = document.createElement("div");
                    colEl.className = "schema-col";
                    let badges = `<span class="col-type">${c.type || "TEXT"}</span>`;
                    if (c.is_pk) badges += `<span class="col-pk">PK</span>`;
                    if (c.fk_ref) badges += `<span class="col-fk">FK→${c.fk_ref}</span>`;
                    colEl.innerHTML = `<span>${c.name}</span>${badges}`;
                    columnsDiv.appendChild(colEl);
                });
            }
        }

        header.addEventListener("click", () => {
            const isOpen = columnsDiv.classList.toggle("open");
            header.querySelector(".table-icon").textContent = isOpen ? "▼" : "▶";
        });

        tableDiv.appendChild(header);
        tableDiv.appendChild(columnsDiv);
        schemaTree.appendChild(tableDiv);
    });
}

// ── Query Input ───────────────────────────────────────────────
function initInput() {
    queryInput.addEventListener("keydown", (e) => {
        if (e.key === "Enter" && !e.shiftKey) {
            e.preventDefault();
            submitQuery();
        }
    });

    // Auto-resize
    queryInput.addEventListener("input", () => {
        queryInput.style.height = "auto";
        queryInput.style.height =
            Math.min(queryInput.scrollHeight, 150) + "px";
    });
}

function askExample(btn) {
    queryInput.value = btn.textContent;
    submitQuery();
}

// ── Submit Query ──────────────────────────────────────────────
async function submitQuery() {
    const question = queryInput.value.trim();
    if (!question || state.isQuerying) return;

    if (!state.schemaLoaded) {
        alert("Please upload CSV files first.");
        return;
    }

    state.isQuerying = true;
    sendBtn.disabled = true;
    queryInput.value = "";
    queryInput.style.height = "auto";

    // Hide welcome
    welcomeScreen.style.display = "none";

    // Add user message
    addUserMessage(question);

    // Add loading
    const loadingId = addLoadingMessage();

    try {
        const res = await fetch("/api/query", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ question }),
        });

        const data = await res.json().catch(() => ({
            answer: `Server returned status ${res.status}. Please try again.`,
            sql: "", valid: false, results: { success: false, rows: [], columns: [], row_count: 0 },
            confidence: 0, total_time_ms: 0, attempts: 0, model_used: "", evaluation: {},
        }));

        removeMessage(loadingId);

        if (!res.ok && !data.answer) {
            addErrorMessage(data.detail || `Server error (${res.status})`);
        } else {
            addAssistantMessage(data);
        }

        // Update state
        state.queryCount++;
        state.totalTime += data.total_time_ms || 0;
        state.history.unshift({
            question,
            sql: data.sql,
            time: data.total_time_ms,
        });
        updateStats();
        updateHistory();
    } catch (err) {
        removeMessage(loadingId);
        addErrorMessage(err.message || "Network error. Is the server running?");
    } finally {
        state.isQuerying = false;
        sendBtn.disabled = false;
        queryInput.focus();
    }
}

// ── Message Rendering ─────────────────────────────────────────
function addUserMessage(text) {
    const div = document.createElement("div");
    div.className = "message message-user";
    div.innerHTML = `<div class="message-bubble">${escapeHtml(text)}</div>`;
    messagesContainer.appendChild(div);
    scrollToBottom();
}

function addLoadingMessage() {
    const id = "loading-" + Date.now();
    const div = document.createElement("div");
    div.className = "message message-assistant message-loading";
    div.id = id;
    div.innerHTML = `
        <div class="assistant-avatar">🔍</div>
        <div class="message-body">
            <div class="message-answer">
                <div class="loading-dots">
                    <span></span><span></span><span></span>
                </div>
                <span style="color: var(--text-muted); font-size: 0.82rem">Generating SQL and querying data...</span>
            </div>
        </div>
    `;
    messagesContainer.appendChild(div);
    scrollToBottom();
    return id;
}

function removeMessage(id) {
    const el = document.getElementById(id);
    if (el) el.remove();
}

function addAssistantMessage(data) {
    const div = document.createElement("div");
    div.className = "message message-assistant";

    // Build results table
    let resultsHtml = "";
    const results = data.results || {};
    if (results.success && results.rows && results.rows.length > 0) {
        const cols = results.columns || Object.keys(results.rows[0]);
        const displayRows = results.rows.slice(0, 50);

        resultsHtml = `
            <div class="results-card">
                <div class="results-header">
                    <span class="results-label">📊 Query Results</span>
                    <span class="results-count">${results.row_count} row${results.row_count !== 1 ? "s" : ""}${results.truncated ? " (truncated)" : ""}</span>
                </div>
                <div class="results-table-wrapper">
                    <table class="results-table">
                        <thead>
                            <tr>${cols.map((c) => `<th>${escapeHtml(c)}</th>`).join("")}</tr>
                        </thead>
                        <tbody>
                            ${displayRows.map((row) => `
                                <tr>${cols.map((c) => `<td>${escapeHtml(String(row[c] ?? "NULL"))}</td>`).join("")}</tr>
                            `).join("")}
                        </tbody>
                    </table>
                </div>
            </div>
        `;
    }

    // Confidence color
    const conf = data.confidence || 0;
    const confColor =
        conf >= 0.8 ? "var(--accent-green)" :
            conf >= 0.5 ? "var(--accent-amber)" : "var(--accent-rose)";

    // Evaluation scores
    const evalData = data.evaluation || {};
    let evalHtml = "";
    if (evalData.faithfulness || evalData.helpfulness) {
        const f = evalData.faithfulness || 0;
        const h = evalData.helpfulness || 0;
        const fColor = f >= 4 ? "var(--accent-green)" : f >= 3 ? "var(--accent-amber)" : "var(--accent-rose)";
        const hColor = h >= 4 ? "var(--accent-green)" : h >= 3 ? "var(--accent-amber)" : "var(--accent-rose)";
        evalHtml = `
            <div class="eval-card">
                <div class="eval-header">🧑‍⚖️ LLM Judge Evaluation</div>
                <div class="eval-scores">
                    <div class="eval-score">
                        <span class="eval-label">Faithfulness</span>
                        <span class="eval-value" style="color: ${fColor}">${f}/5</span>
                        <div class="eval-bar"><div class="eval-bar-fill" style="width: ${f * 20}%; background: ${fColor}"></div></div>
                    </div>
                    <div class="eval-score">
                        <span class="eval-label">Helpfulness</span>
                        <span class="eval-value" style="color: ${hColor}">${h}/5</span>
                        <div class="eval-bar"><div class="eval-bar-fill" style="width: ${h * 20}%; background: ${hColor}"></div></div>
                    </div>
                </div>
            </div>
        `;
    }

    // SQL card with copy button
    const sqlId = "sql-" + Date.now();
    const sqlCardHtml = data.sql ? `
        <div class="sql-card">
            <div class="sql-header">
                <span class="sql-label">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="12" height="12">
                        <polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/>
                    </svg>
                    Generated SQL
                </span>
                <div class="sql-actions">
                    <span class="sql-meta">⏱ ${data.generation_time_ms || 0}ms · ${data.attempts || 1} attempt${(data.attempts || 1) > 1 ? "s" : ""}</span>
                    <button class="copy-btn" onclick="copySQL('${sqlId}')" title="Copy SQL">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14">
                            <rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>
                            <path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/>
                        </svg>
                        <span>Copy</span>
                    </button>
                </div>
            </div>
            <pre class="sql-body" id="${sqlId}">${escapeHtml(data.sql)}</pre>
        </div>
    ` : "";

    div.innerHTML = `
        <div class="assistant-avatar">⚡</div>
        <div class="message-body">
            <div class="message-answer">${formatAnswer(data.answer || "No answer generated.")}</div>

            ${sqlCardHtml}
            ${resultsHtml}
            ${evalHtml}

            <div class="metrics-bar">
                <div class="metric">
                    <span>⏱</span>
                    <span class="metric-value">${data.total_time_ms || 0}ms</span>
                    <span>total</span>
                </div>
                <div class="metric">
                    <span>🎯</span>
                    <span class="metric-value" style="color: ${confColor}">${Math.round(conf * 100)}%</span>
                    <span>confidence</span>
                </div>
                <div class="metric">
                    <span>🤖</span>
                    <span class="metric-value">${escapeHtml(data.model_used || "N/A")}</span>
                </div>
                ${data.cached ? `
                <div class="metric" style="border-color: var(--accent-green)">
                    <span>⚡</span>
                    <span class="metric-value" style="color: var(--accent-green)">Cached</span>
                </div>` : ""}
            </div>
        </div>
    `;

    messagesContainer.appendChild(div);
    scrollToBottom();
}

function addErrorMessage(text) {
    const div = document.createElement("div");
    div.className = "message message-assistant";
    div.innerHTML = `
        <div class="assistant-avatar">⚠️</div>
        <div class="message-body">
            <div class="message-answer error-text">
                <strong>Error:</strong> ${escapeHtml(text)}
            </div>
        </div>
    `;
    messagesContainer.appendChild(div);
    scrollToBottom();
}

// ── Helpers ───────────────────────────────────────────────────
function escapeHtml(str) {
    const map = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;" };
    return String(str).replace(/[&<>"']/g, (m) => map[m]);
}

function copySQL(id) {
    const el = document.getElementById(id);
    if (!el) return;
    const text = el.textContent;
    navigator.clipboard.writeText(text).then(() => {
        const btn = el.parentElement.querySelector(".copy-btn span");
        if (btn) {
            btn.textContent = "Copied!";
            setTimeout(() => { btn.textContent = "Copy"; }, 2000);
        }
    }).catch(() => {
        // Fallback for non-HTTPS
        const textarea = document.createElement("textarea");
        textarea.value = text;
        document.body.appendChild(textarea);
        textarea.select();
        document.execCommand("copy");
        document.body.removeChild(textarea);
        const btn = el.parentElement.querySelector(".copy-btn span");
        if (btn) {
            btn.textContent = "Copied!";
            setTimeout(() => { btn.textContent = "Copy"; }, 2000);
        }
    });
}

function formatAnswer(text) {
    // Bold: **text**
    text = text.replace(/\*\*(.*?)\*\*/g, "<strong>$1</strong>");
    // Newlines
    text = text.replace(/\n/g, "<br>");
    return text;
}

function scrollToBottom() {
    requestAnimationFrame(() => {
        chatArea.scrollTop = chatArea.scrollHeight;
    });
}

function updateStats() {
    $("#statQueries").textContent = state.queryCount;
    $("#statTables").textContent = state.tables.length;
    const avgTime =
        state.queryCount > 0
            ? Math.round(state.totalTime / state.queryCount)
            : 0;
    $("#statAvgTime").textContent = avgTime + "ms";
}

function updateHistory() {
    historyList.innerHTML = "";
    state.history.slice(0, 20).forEach((h) => {
        const item = document.createElement("div");
        item.className = "history-item";
        item.textContent = h.question;
        item.title = h.question;
        item.addEventListener("click", () => {
            queryInput.value = h.question;
            queryInput.focus();
        });
        historyList.appendChild(item);
    });
}

async function checkHealth() {
    try {
        const res = await fetch("/api/health");
        const data = await res.json();
        if (data.schema_loaded) {
            // Reload schema info
            const schemaRes = await fetch("/api/schema");
            const schemaData = await schemaRes.json();
            if (schemaData.loaded) {
                onSchemaLoaded({
                    tables: schemaData.tables.map((t) => ({
                        table: t.name,
                        column_details: t.columns,
                        row_count: t.row_count,
                    })),
                    relationships: schemaData.relationships,
                    total_rows: schemaData.tables.reduce(
                        (sum, t) => sum + (t.row_count || 0),
                        0
                    ),
                });
            }
        }
    } catch (e) {
        // Server not running yet
    }
}
