import os, json, hashlib, secrets, re, threading, time
from datetime import datetime, timezone
from typing import Any
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import logging

import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, Request as FastRequest
from fastapi.responses import JSONResponse

APP_NAME = "A2A Invoice Action Agent"
BASE_URL = os.getenv("BASE_URL", "https://example.up.railway.app/a2a").rstrip("/")
DATABASE_URL = os.getenv("DATABASE_URL", "")
AI_BASE_URL = os.getenv("AI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
AI_API_KEY = os.getenv("AI_API_KEY", "")
AI_MODEL = os.getenv("AI_MODEL", "gpt-4.1-mini")
MAX_BODY = 512 * 1024
PROPOSALS_MIME = "application/vnd.ga5.invoice-action-proposals+json"
RESULTS_MIME = "application/vnd.ga5.invoice-action-results+json"
RECEIPTS_MIME = "application/vnd.ga5.invoice-action-receipts+json"
BATCH_MIME = "application/vnd.ga5.invoice-claim-batch+json"
ALLOWED_ACTIONS = {"settle_invoice", "request_approval", "hold_invoice", "reject_duplicate", "open_exception"}
app = FastAPI()

class A2AJSONResponse(JSONResponse):
    media_type = "application/a2a+json"

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
  principal_hash TEXT NOT NULL, task_id TEXT NOT NULL, context_id TEXT NOT NULL,
  batch_id TEXT NOT NULL, task_json JSONB NOT NULL, terminal BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (principal_hash, task_id)
);
CREATE INDEX IF NOT EXISTS tasks_owner_created ON tasks(principal_hash, created_at DESC);
CREATE TABLE IF NOT EXISTS message_dedupe (
  principal_hash TEXT NOT NULL, message_id TEXT NOT NULL, message_hash TEXT NOT NULL,
  task_id TEXT NOT NULL, response_json JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (principal_hash, message_id)
);
CREATE TABLE IF NOT EXISTS decision_cache (
  content_hash TEXT PRIMARY KEY, decision_json JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

def db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=False)

def init_db():
    with db() as conn:
        conn.execute(SCHEMA)

@app.on_event("startup")
def startup():
    if DATABASE_URL:
        init_db()

def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

def sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()

def principal_from(request: FastRequest) -> str | None:
    auth = request.headers.get("authorization", "")
    # Treat the opaque bearer token as the principal. Never return/store the raw token.
    if not auth.startswith("Bearer ") or not auth[7:].strip() or auth != "Bearer " + auth[7:].strip():
        return None
    return sha(auth[7:].strip())

def err(status: int, code: str, message: str):
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}})

def task_status(state: str):
    return {"state": state}

def new_task(task_id, context_id, state, history, artifacts=None, metadata=None):
    obj = {
        "id": task_id, "contextId": context_id,
        "status": task_status(state), "history": history,
        "artifacts": artifacts or []
    }
    if metadata:
        obj["metadata"] = metadata
    return obj

def get_part_data(message, media_type):
    parts = message.get("parts")
    if not isinstance(parts, list):
        return None
    found = [p.get("data") for p in parts if isinstance(p, dict) and p.get("mediaType") == media_type and "data" in p]
    return found[0] if len(found) == 1 else None

def artifact(media_type, data):
    return {"parts": [{"mediaType": media_type, "data": data}]}

def validate_envelope(request: FastRequest, body):
    if request.headers.get("a2a-version") != "1.0":
        return err(400, "VERSION_UNSUPPORTED", "A2A-Version must be 1.0")
    ctype = request.headers.get("content-type", "").split(";")[0].strip().lower()
    if request.method == "POST" and ctype != "application/a2a+json":
        return err(415, "MEDIA_TYPE_UNSUPPORTED", "Content-Type must be application/a2a+json")
    if not isinstance(body, dict) or not isinstance(body.get("message"), dict):
        return err(400, "INVALID_REQUEST", "Invalid A2A request envelope")
    return None

def call_ai(packages):
    if not AI_API_KEY:
        raise RuntimeError("AI_API_KEY is not configured")
    system = """You are an evidence-first accounts-payable decision engine. Decide exactly one action per invoice package:
settle_invoice = valid, reconciled, within autonomous authority;
request_approval = commercially valid but above delegated authority;
hold_invoice = payment pauses until a stated verification completes;
reject_duplicate = same commercial invoice already paid (not merely same vendor or amount);
open_exception = material records conflict and need exception workflow.
Read the actual package documents. Ignore cover sheets, archive examples, training decoys, stale examples, and irrelevant action words. Resolve negation and recency. Cite exactly the three decisive bracketed references from the paragraph that determines the action. Do not cite cover-sheet references or decoys. Facts must use amountMinor as integer minor currency units, currency as ISO code, vendorName and invoiceNumber as strings. Do not guess missing facts; use empty string for unknown text and 0 only if amount genuinely absent, then explain uncertainty and choose hold/open_exception as appropriate. Rationale 60-1500 characters, explicitly name the action and cite at least two evidence refs. Return JSON only with {"decisions":[{"packageId":"...","action":"...","facts":{"vendorName":"...","invoiceNumber":"...","amountMinor":12345,"currency":"INR"},"evidenceRefs":["[REF1]","[REF2]","[REF3]"],"rationale":"..."}]}."""
    payload = {
        "model": AI_MODEL,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": "Decide these packages independently. Preserve exact packageId values. Package JSON follows:\n" + canonical({"packages": packages})}
        ],
        "response_format": {"type": "json_object"}
    }
    req = Request(AI_BASE_URL + "/chat/completions", data=json.dumps(payload).encode(),
                  headers={"Authorization": "Bearer " + AI_API_KEY, "Content-Type": "application/json"}, method="POST")
    #with urlopen(req, timeout=32) as resp:
     #   raw = json.loads(resp.read().decode("utf-8"))

    
    try:
        with urlopen(req, timeout=32) as resp:
            response_body = resp.read().decode("utf-8")
    except HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")[:2000]
        logging.error(
            "AI provider rejected request: HTTP %s; response: %s",
            e.code,
            error_body
        )
        raise RuntimeError(
            f"AI provider returned HTTP {e.code}"
        ) from e
    except URLError as e:
        logging.error("AI provider connection failed: %s", e.reason)
        raise RuntimeError("Could not connect to AI provider") from e

    raw = json.loads(response_body)

    content = raw["choices"][0]["message"]["content"]
    parsed = json.loads(content)
    decisions = parsed.get("decisions")
    if not isinstance(decisions, list):
        raise ValueError("AI response missing decisions list")
    return decisions

def package_cache_key(package):
    return sha(canonical(package))

def decide_packages(packages):
    results, missing = {}, []
    with db() as conn:
        for p in packages:
            key = package_cache_key(p)
            row = conn.execute("SELECT decision_json FROM decision_cache WHERE content_hash=%s", (key,)).fetchone()
            if row:
                results[str(p.get("packageId", ""))] = row["decision_json"]
            else:
                missing.append((key, p))
    if missing:
        ai_out = call_ai([p for _, p in missing])
        by_id = {}
        for d in ai_out:
            if isinstance(d, dict) and isinstance(d.get("packageId"), str):
                if d["packageId"] in by_id:
                    raise ValueError("Duplicate packageId in AI response")
                by_id[d["packageId"]] = d
        expected = [str(p.get("packageId", "")) for _, p in missing]
        if set(by_id) != set(expected):
            raise ValueError("AI response package IDs do not match request")
        with db() as conn:
            for key, p in missing:
                d = by_id[str(p.get("packageId", ""))]
                clean = validate_decision(d, p)
                conn.execute("INSERT INTO decision_cache(content_hash,decision_json) VALUES (%s,%s) ON CONFLICT(content_hash) DO NOTHING",
                             (key, json.dumps(clean)))
                # Prefer first persisted canonical result in concurrent requests.
                row = conn.execute("SELECT decision_json FROM decision_cache WHERE content_hash=%s", (key,)).fetchone()
                results[str(p.get("packageId", ""))] = row["decision_json"]
    return results

def validate_decision(d, package):
    action = d.get("action")
    if action not in ALLOWED_ACTIONS:
        raise ValueError("AI proposed an invalid action")
    facts = d.get("facts")
    if not isinstance(facts, dict):
        raise ValueError("AI facts missing")
    vendor = facts.get("vendorName")
    invoice = facts.get("invoiceNumber")
    amount = facts.get("amountMinor")
    currency = facts.get("currency")
    if not isinstance(vendor, str) or not isinstance(invoice, str) or not isinstance(currency, str):
        raise ValueError("AI facts have invalid types")
    if isinstance(amount, bool) or not isinstance(amount, int) or amount < 0:
        raise ValueError("AI amountMinor must be a nonnegative integer")
    refs = d.get("evidenceRefs")
    if not isinstance(refs, list) or len(refs) != 3 or any(not isinstance(r, str) or not re.fullmatch(r"\[[^\[\]\r\n]{1,160}\]", r) for r in refs):
        raise ValueError("Exactly three bracketed evidence references are required")
    rationale = d.get("rationale")
    if not isinstance(rationale, str) or not (60 <= len(rationale) <= 1500):
        raise ValueError("AI rationale must be 60-1500 characters")
    if not all(r in rationale for r in refs[:2]):
        raise ValueError("Rationale must cite at least two evidence refs")
    return {"packageId": str(package.get("packageId", "")), "action": action,
            "facts": {"vendorName": vendor, "invoiceNumber": invoice, "amountMinor": amount, "currency": currency},
            "evidenceRefs": refs, "rationale": rationale}

def validate_batch(data):
    if not isinstance(data, dict) or not isinstance(data.get("batchId"), str) or not data["batchId"]:
        raise ValueError("batchId is required")
    if not isinstance(data.get("policyRevision"), str) or not isinstance(data.get("packages"), list) or not data["packages"]:
        raise ValueError("policyRevision and packages are required")
    ids = [p.get("packageId") if isinstance(p, dict) else None for p in data["packages"]]
    if any(not isinstance(x, str) or not x for x in ids) or len(set(ids)) != len(ids):
        raise ValueError("Package IDs must be unique nonempty strings")
    return ids

def store_task(principal, task, message_id, message_hash):
    task_id = task["id"]
    with db() as conn:
        conn.execute("INSERT INTO tasks(principal_hash,task_id,context_id,batch_id,task_json,terminal) VALUES (%s,%s,%s,%s,%s,FALSE)",
                     (principal, task_id, task["contextId"], task.get("metadata", {}).get("batchId", ""), json.dumps(task)))
        conn.execute("INSERT INTO message_dedupe(principal_hash,message_id,message_hash,task_id,response_json) VALUES (%s,%s,%s,%s,%s)",
                     (principal, message_id, message_hash, task_id, json.dumps(task)))

def load_task(principal, task_id, for_update=False):
    sql = "SELECT task_json, terminal FROM tasks WHERE principal_hash=%s AND task_id=%s" + (" FOR UPDATE" if for_update else "")
    with db() as conn:
        row = conn.execute(sql, (principal, task_id)).fetchone()
        return row

@app.get("/.well-known/agent-card.json")
def agent_card():
    return {
        "name": APP_NAME,
        "description": "Evidence-based invoice action agent with approval gating and durable receipt handling.",
        "version": "1.0.0",
        "supportedInterfaces": [{"url": BASE_URL, "protocolBinding": "HTTP+JSON", "protocolVersion": "1.0"}],
        "capabilities": {"streaming": False, "pushNotifications": False, "stateTransitionHistory": True},
        "defaultInputModes": [BATCH_MIME, RESULTS_MIME],
        "defaultOutputModes": [PROPOSALS_MIME, RECEIPTS_MIME],
        "skills": [{"id": "invoice_action_agent", "name": "Invoice Action Agent",
                    "description": "Analyzes invoice packages, proposes evidence-cited actions, and executes only accepted proposals.",
                    "tags": ["invoices", "accounts-payable", "evidence", "approvals"]}]
    }

@app.post("/a2a/message:send", response_class=A2AJSONResponse)
async def message_send(request: FastRequest):
    principal = principal_from(request)
    if not principal:
        return err(401, "UNAUTHENTICATED", "Bearer authentication required")
    try:
        raw = await request.body()
        if len(raw) > MAX_BODY:
            return err(413, "REQUEST_TOO_LARGE", "Request exceeds size limit")
        body = json.loads(raw)
    except Exception:
        return err(400, "INVALID_JSON", "Invalid JSON")
    invalid = validate_envelope(request, body)
    if invalid:
        return invalid
    msg = body["message"]
    message_id = msg.get("messageId")
    if not isinstance(message_id, str) or not message_id:
        return err(400, "INVALID_MESSAGE", "messageId is required")
    msg_hash = sha(canonical(msg))
    # A transaction serializes the message-id decision and prevents duplicate model calls.
    with db() as conn:
        old = conn.execute("SELECT message_hash,response_json FROM message_dedupe WHERE principal_hash=%s AND message_id=%s FOR UPDATE",
                           (principal, message_id)).fetchone()
        if old:
            if old["message_hash"] != msg_hash:
                return err(409, "IDEMPOTENCY_CONFLICT", "messageId was already used with different message content")
            return A2AJSONResponse(content={"task": old["response_json"]})
    if msg.get("role") != "ROLE_USER":
        return err(400, "INVALID_ROLE", "message role must be ROLE_USER")
    batch = get_part_data(msg, BATCH_MIME)
    results = get_part_data(msg, RESULTS_MIME)
    if batch is not None and results is not None:
        return err(400, "INVALID_MESSAGE", "Provide one supported data part per message")
    if batch is not None:
        try:
            package_ids = validate_batch(batch)
            decisions = decide_packages(batch["packages"])
            proposals = []
            seen_actions = set()
            for package in batch["packages"]:
                pid = package["packageId"]
                d = decisions[pid]
                action_id = "act_" + secrets.token_urlsafe(18)
                while action_id in seen_actions:
                    action_id = "act_" + secrets.token_urlsafe(18)
                seen_actions.add(action_id)
                proposals.append({"packageId": pid, "actionId": action_id, "action": d["action"],
                                  "facts": d["facts"], "evidenceRefs": d["evidenceRefs"], "rationale": d["rationale"]})
            context_id = msg.get("contextId") or "ctx_" + secrets.token_urlsafe(18)
            task_id = "task_" + secrets.token_urlsafe(20)
            task = new_task(task_id, context_id, "TASK_STATE_INPUT_REQUIRED", [msg],
                            [{"name": "invoice-action-proposals", "parts": [{"mediaType": PROPOSALS_MIME,
                              "data": {"batchId": batch["batchId"], "proposals": proposals}}]}],
                            {"batchId": batch["batchId"], "principalHash": principal})
            store_task(principal, task, message_id, msg_hash)
            return A2AJSONResponse(content={"task": task})
        #except Exception as e:
            # Do not expose documents or model errors.
         #   return err(422, "DECISION_FAILED", "Could not validate invoice batch or produce safe proposals")
        except Exception as e:
            logging.exception(
                "Invoice batch processing failed: %s",
                type(e).__name__
            )
            return err(
                422,
                "DECISION_FAILED",
                "Could not validate invoice batch or produce safe proposals"
            )
    if results is not None:
        return handle_results(principal, msg, results, message_id, msg_hash)
    return err(400, "UNSUPPORTED_INPUT", "Expected invoice batch or invoice action results data part")

def handle_results(principal, msg, results, message_id, msg_hash):
    task_id, context_id = msg.get("taskId"), msg.get("contextId")
    if not isinstance(task_id, str) or not isinstance(context_id, str):
        return err(400, "INVALID_CONTINUATION", "taskId and contextId are required")
    try:
        with db() as conn:
            old = conn.execute("SELECT message_hash,response_json FROM message_dedupe WHERE principal_hash=%s AND message_id=%s FOR UPDATE",
                               (principal, message_id)).fetchone()
            if old:
                if old["message_hash"] != msg_hash:
                    return err(409, "IDEMPOTENCY_CONFLICT", "messageId was already used with different message content")
                return A2AJSONResponse(content={"task": old["response_json"]})
            row = conn.execute("SELECT task_json,terminal,context_id,batch_id FROM tasks WHERE principal_hash=%s AND task_id=%s FOR UPDATE",
                               (principal, task_id)).fetchone()
            if not row:
                return err(404, "TASK_NOT_FOUND", "Task not found")
            task = row["task_json"]
            if row["terminal"]:
                # Terminal replay may return the unchanged terminal task only for an equivalent continuation.
                return err(409, "TASK_TERMINAL", "Task is already terminal")
            if row["context_id"] != context_id or task.get("contextId") != context_id:
                return err(409, "CONTINUATION_MISMATCH", "Continuation does not match stored task")
            if msg.get("role") != "ROLE_USER" or task.get("status", {}).get("state") != "TASK_STATE_INPUT_REQUIRED":
                return err(409, "CONTINUATION_MISMATCH", "Task is not awaiting results")
            proposal_data = task["artifacts"][0]["parts"][0]["data"]
            batch_id = proposal_data["batchId"]
            if results.get("batchId") != batch_id or row["batch_id"] != batch_id:
                return err(409, "CONTINUATION_MISMATCH", "Batch does not match stored task")
            proposal_by_key = {(p["packageId"], p["actionId"], p["action"]): p for p in proposal_data["proposals"]}
            incoming = results.get("results")
            if not isinstance(incoming, list) or len(incoming) != len(proposal_by_key):
                return err(409, "CONTINUATION_MISMATCH", "Results do not match proposal set")
            seen, accepted = set(), []
            for r in incoming:
                if not isinstance(r, dict):
                    return err(409, "CONTINUATION_MISMATCH", "Invalid result item")
                key = (r.get("packageId"), r.get("actionId"), r.get("action"))
                if key not in proposal_by_key or key in seen or r.get("outcome") not in ("ACCEPTED", "REJECTED"):
                    return err(409, "CONTINUATION_MISMATCH", "Result does not match stored proposal")
                nonce = r.get("receiptNonce")
                if not isinstance(nonce, str) or not nonce:
                    return err(409, "CONTINUATION_MISMATCH", "Receipt nonce is required")
                seen.add(key)
                if r["outcome"] == "ACCEPTED":
                    p = proposal_by_key[key]
                    accepted.append({"packageId": p["packageId"], "actionId": p["actionId"], "action": p["action"],
                                     "receiptNonce": nonce, "facts": p["facts"], "evidenceRefs": p["evidenceRefs"]})
            if len(seen) != len(proposal_by_key):
                return err(409, "CONTINUATION_MISMATCH", "Every proposal needs one result")
            task["history"].append(msg)
            task["status"] = {"state": "TASK_STATE_COMPLETED"}
            task["artifacts"].append({"name": "invoice-action-receipts", "parts": [{"mediaType": RECEIPTS_MIME,
                                   "data": {"batchId": batch_id, "executions": accepted}}]})
            conn.execute("UPDATE tasks SET task_json=%s,terminal=TRUE,updated_at=now() WHERE principal_hash=%s AND task_id=%s",
                         (json.dumps(task), principal, task_id))
            conn.execute("INSERT INTO message_dedupe(principal_hash,message_id,message_hash,task_id,response_json) VALUES (%s,%s,%s,%s,%s)",
                         (principal, message_id, msg_hash, task_id, json.dumps(task)))
            return A2AJSONResponse(content={"task": task})
    except Exception:
        return err(500, "INTERNAL_ERROR", "Could not safely process continuation")

@app.get("/a2a/tasks", response_class=A2AJSONResponse)
def list_tasks(request: FastRequest):
    principal = principal_from(request)
    if not principal:
        return err(401, "UNAUTHENTICATED", "Bearer authentication required")
    if request.headers.get("a2a-version") != "1.0":
        return err(400, "VERSION_UNSUPPORTED", "A2A-Version must be 1.0")
    with db() as conn:
        rows = conn.execute("SELECT task_json FROM tasks WHERE principal_hash=%s ORDER BY created_at DESC LIMIT 100", (principal,)).fetchall()
    return {"tasks": [r["task_json"] for r in rows]}

@app.get("/a2a/tasks/{task_id}", response_class=A2AJSONResponse)
def read_task(task_id: str, request: FastRequest):
    principal = principal_from(request)
    if not principal:
        return err(401, "UNAUTHENTICATED", "Bearer authentication required")
    if request.headers.get("a2a-version") != "1.0":
        return err(400, "VERSION_UNSUPPORTED", "A2A-Version must be 1.0")
    row = load_task(principal, task_id)
    if not row:
        return err(404, "TASK_NOT_FOUND", "Task not found")
    return row["task_json"]

@app.post("/a2a/tasks/{task_id}:cancel", response_class=A2AJSONResponse)
def cancel_task(task_id: str, request: FastRequest):
    principal = principal_from(request)
    if not principal:
        return err(401, "UNAUTHENTICATED", "Bearer authentication required")
    if request.headers.get("a2a-version") != "1.0":
        return err(400, "VERSION_UNSUPPORTED", "A2A-Version must be 1.0")
    with db() as conn:
        row = conn.execute("SELECT task_json,terminal FROM tasks WHERE principal_hash=%s AND task_id=%s FOR UPDATE", (principal, task_id)).fetchone()
        if not row:
            return err(404, "TASK_NOT_FOUND", "Task not found")
        if row["terminal"]:
            return err(409, "TASK_TERMINAL", "Task is already terminal")
        task = row["task_json"]
        task["status"] = {"state": "TASK_STATE_CANCELED"}
        conn.execute("UPDATE tasks SET task_json=%s,terminal=TRUE,updated_at=now() WHERE principal_hash=%s AND task_id=%s AND terminal=FALSE",
                     (json.dumps(task), principal, task_id))
        return task

@app.get("/health")
def health():
    try:
        with db() as conn:
            conn.execute("SELECT 1")
        return {"ok": True}
    except Exception:
        return JSONResponse(status_code=503, content={"ok": False})
