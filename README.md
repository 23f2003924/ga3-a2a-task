# A2A Invoice Action Agent (starter implementation)

FastAPI + PostgreSQL + an OpenAI-compatible chat-completions endpoint. The app proposes actions first; only a matching grader continuation with accepted outcomes creates receipt executions.

## Deploy to Railway

1. Create a Railway project and add a PostgreSQL database.
2. Deploy this folder from a GitHub repository (or Railway CLI). Railway builds the included Dockerfile.
3. Add environment variables to the web service:
   - `DATABASE_URL`: reference Railway PostgreSQL's connection URL.
   - `BASE_URL`: `https://YOUR-PUBLIC-DOMAIN/a2a` (no trailing slash).
   - `AI_API_KEY`: your provider API key. Never commit it.
   - `AI_BASE_URL` and `AI_MODEL` if using another OpenAI-compatible provider.
4. Expose the service publicly over HTTPS. Verify `GET https://YOUR-PUBLIC-DOMAIN/.well-known/agent-card.json` and `/health`.
5. Submit `https://YOUR-PUBLIC-DOMAIN/a2a` as the A2A interface base URL.

The service initializes its PostgreSQL tables on startup. Railway normally provides TLS at the public edge. Use the database URL supplied by Railway; if your provider requires SSL, include `?sslmode=require`.

## Protocol paths

- `GET /.well-known/agent-card.json` (public)
- `POST /a2a/message:send`
- `GET /a2a/tasks/{id}`
- `GET /a2a/tasks`
- `POST /a2a/tasks/{id}:cancel`

All non-card routes require `Authorization: Bearer <opaque-token>` and `A2A-Version: 1.0`. POST requests require `Content-Type: application/a2a+json`.

## Local run

Use Python 3.12+ and a PostgreSQL database.

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL='postgresql://...'
export BASE_URL='http://localhost:8000/a2a'
export AI_API_KEY='...'
uvicorn main:app --reload
```

## Important validation before submission

This is a strong starter, not a certified grader-passing solution. Before using it for the limited-check assignment, run the grader's Check once and inspect every failure code. In particular, test all lifecycle and race cases, request body limits, generated package evidence references, and whether the grader expects a specific envelope shape. Do not use Save until checks are passing; the assignment says only the most recent Save counts.

### Design notes

- Task data, message idempotency, and semantic decision cache live in PostgreSQL.
- Bearer tokens are represented internally by SHA-256 hashes and never stored raw.
- Message hash covers canonicalized `message` only; `configuration` is ignored.
- The semantic decision cache key is canonical package JSON; action IDs remain unique per batch.
- Result continuations must match stored task/context/batch and each package/action/action ID. Accepted outcomes alone produce receipt executions.
- Errors avoid reflecting task IDs or invoice documents.

### Known hardening work before relying on this for grading

The assignment has strict concurrent races and exact protocol requirements. Test and harden these before Save:
- Serialize initial same-message requests across concurrent transactions (e.g. PostgreSQL advisory lock on principal+message ID); a uniqueness constraint alone can race before model invocation.
- Make cancel and result continuation use the same transaction/row-lock path and ensure exactly one wins. Confirm transaction contexts commit on all early return paths.
- Implement terminal continuation replay semantics if the grader retries the same result message after completion.
- Validate the exact set and uniqueness of proposal package IDs/action IDs and all result fields; reject unexpected fields if required by grader.
- Check whether the A2A spec/grader expects task status timestamps, artifact IDs, or additional envelope fields.
- Avoid relying on the model to perfectly identify decisive references; add a source-reference allowlist derived from the package text and reject citations not present in the decisive paragraph.
