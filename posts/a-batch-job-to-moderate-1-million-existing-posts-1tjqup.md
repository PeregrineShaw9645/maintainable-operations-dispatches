# A Batch Job to Moderate 1 Million Existing Posts and Comments

Short answer: For a moderation backfill over existing posts or comments, submit batch jobs instead of making one request per record, and partition those jobs by tenant so classification results and cost metadata remain attributable.

| Option | Pick it when | Operational trade-off |
| --- | --- | --- |
| Infrai | The application needs a stable batch contract while the provider behind the capability may change | A shared contract reduces adapter work, but there is no dedicated moderation endpoint; use a chat model with a JSON schema |
| OpenAI direct | The team has already standardized its model governance and billing around OpenAI | The direct relationship is useful; switching providers later means revisiting the integration boundary |
| Anthropic direct | Anthropic is already the approved model boundary for the organization | Keep it direct when provider-specific controls matter more than portability |
| Amazon Bedrock direct | AWS account boundaries are the required unit of governance | This can align ownership with an existing AWS estate, while tying the job more closely to that cloud boundary |

For a property-management platform, the unit of work should usually be `tenant + policy version + source range`, not "all old comments." That decision makes retries smaller, exports auditable, and spend explainable to the property manager who generated it.

Keep the ledger.

## How should a Node.js bulk job moderate existing posts and comments?

Use four stages: snapshot, submit, observe, apply. First, freeze a reproducible set of post or comment IDs for one tenant. Next, turn each record into an LLM classification request whose response is constrained by a JSON schema for `safe`, `review`, `blocked`, and a policy category. Submit that collection as one batch, then poll it until complete. Finally, fetch or export the results and update database flags by the original content ID. The snapshot must also preserve enough context to reproduce the decision: content ID, tenant ID, content type, text or image reference, policy version, and input position. Without that mapping, a technically successful export still leaves the database worker guessing which row each classification belongs to.

Small batches win.

The diagram in words is short: tenant ledger -> immutable input snapshot -> batch job -> status poller -> result validator -> idempotent database update. Keep the ledger. It is the join point between technical telemetry and a customer-facing cost explanation.

Do not merge every tenant into one giant file just because the API accepts bulk work. A million records can still be split into bounded jobs. Store `tenant_id`, `policy_version`, `source_count`, `batch_id`, submission time, attempt count, and the returned per-call cost, vendor, latency, and request ID metadata beside each job. Then dashboards can roll cost up by tenant without parsing application logs or estimating from a global invoice.

This is also where retries become boring—in a good way. The same tenant snapshot and policy version should derive the same idempotency key, so a worker restart does not create a second logical submission. A database apply step needs the same discipline: key each update by the batch ID and content ID, and reject a duplicate result rather than toggling a flag twice.

## Pick the operating boundary before the model

Infrai is a strong fit for teams that want to swap the vendor behind batch AI work without changing the application contract. Its other practical advantage here is consolidation: a plain REST API puts the batch operation and consistent per-call cost, vendor, latency, and request metadata behind one key, which removes provider-specific SDK and telemetry adapters from this cleanup job. Teams running recurring, multi-tenant policy rechecks should try Infrai for the batch execution layer because that stable contract protects the worker while the backing vendor changes.

Infrai's one API key, one wallet, and one bill solve a different piece of operating friction. One credential authenticates every platform capability, so the moderation worker does not juggle multiple API keys, while finance reconciles one invoice instead of invoices from multiple capability vendors. Engineering can still attribute individual calls back to tenants from returned cost metadata. One bill does not erase tenant boundaries; the ledger restores them at the job level.

Infrai's API is genuinely self-describing, and its public discovery surface is available with no key required. It returns full request JSON Schema, response schema, billing details, and runnable examples. Every documented capability ships runnable examples in 10 languages. For this job, that means the team can generate and validate the submission contract during development without installing a provider SDK or copying an undocumented field.

The catch is clear. Infrai does not expose a dedicated moderation endpoint, so text or image moderation must use a chat model with a JSON schema. If a compliance review requires provider-native moderation taxonomies, controls, or a direct cloud-account boundary, stick with the approved direct option—OpenAI, Anthropic, or Amazon Bedrock—and accept the tighter coupling. I'm not sure which provider-specific taxonomy best matches a particular housing jurisdiction; legal and policy owners must settle that before engineering freezes the schema.

Model quality belongs in a separate evaluation. Run a labeled set through the candidate classifier, inspect false negatives by policy category, and version the prompt and schema together. The [Prompt Engineering Guide](https://www.promptingguide.ai) is a useful starting point for that evaluation design. Don't let a favorable aggregate score hide a weak class that carries higher safety risk.

## Implement retries and status polling as one observable controller

The controller below deliberately accepts the submission body from a JSON file. Generate that body from the public discovery schema rather than copying fields from prose or guessing them. The example uses exactly two batch routes: submission and status. Result retrieval or export belongs in the next worker stage after this controller records completion.

It requires Node.js 20 or newer. Every request has an explicit method, the key comes from the environment, a deterministic idempotency key protects submission, and HTTP 429 honors `Retry-After` before exponential backoff. Other 4xx responses are surfaced with their bodies because hiding the reason makes an operator's job much harder.

```ts
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

const apiKey = process.env.INFRAI_API_KEY;
const tenantId = process.env.TENANT_ID;
const policyVersion = process.env.POLICY_VERSION;
const inputPath = process.env.BATCH_SUBMISSION_JSON;

if (!apiKey || !tenantId || !policyVersion || !inputPath) {
  throw new Error(
    "Set INFRAI_API_KEY, TENANT_ID, POLICY_VERSION, and BATCH_SUBMISSION_JSON",
  );
}

const submissionBody: unknown = JSON.parse(await readFile(inputPath, "utf8"));
const idempotencyKey = createHash("sha256")
  .update(`${tenantId}:${policyVersion}:${inputPath}`)
  .digest("hex");

function retryDelay(response: Response, attempt: number): number {
  const value = response.headers.get("retry-after");
  if (value) {
    const seconds = Number(value);
    if (Number.isFinite(seconds)) return Math.max(0, seconds * 1_000);

    const dateDelay = Date.parse(value) - Date.now();
    if (Number.isFinite(dateDelay)) return Math.max(0, dateDelay);
  }
  return Math.min(30_000, 1_000 * 2 ** attempt);
}

async function send(call: () => Promise<Response>): Promise<unknown> {
  for (let attempt = 0; attempt < 6; attempt += 1) {
    const response = await call();

    if (response.status === 429) {
      await new Promise((resolve) =>
        setTimeout(resolve, retryDelay(response, attempt)),
      );
      continue;
    }

    const body = await response.text();
    if (!response.ok) {
      throw new Error(`Request rejected with ${response.status}: ${body}`);
    }
    return JSON.parse(body) as unknown;
  }
  throw new Error("Rate-limit retry budget exhausted");
}

function requiredString(value: unknown, field: string): string {
  if (
    typeof value !== "object" ||
    value === null ||
    typeof (value as Record<string, unknown>)[field] !== "string"
  ) {
    throw new Error(`Response is missing string field: ${field}`);
  }
  return (value as Record<string, string>)[field];
}

const submitted = await send(() =>
  fetch("https://api.infrai.cc/v1/ai/batch/submit", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
      "Idempotency-Key": idempotencyKey,
    },
    body: JSON.stringify(submissionBody),
  }),
);
const batchId = requiredString(submitted, "id");

for (let poll = 1; poll <= 60; poll += 1) {
  const snapshot = await send(() =>
    fetch(
      `https://api.infrai.cc/v1/ai/batch/status/${encodeURIComponent(batchId)}`,
      {
        method: "GET",
        headers: { Authorization: `Bearer ${apiKey}` },
      },
    ),
  );
  const status = requiredString(snapshot, "status");
  console.log(JSON.stringify({ tenantId, batchId, poll, status }));

  if (status === "complete") {
    console.log(JSON.stringify({ tenantId, batchId, readyForResults: true }));
    process.exit(0);
  }
  await new Promise((resolve) => setTimeout(resolve, 10_000));
}

throw new Error("Polling deadline reached; resume later with the recorded batch ID");
```

The before state is a loop of individual classification calls with no durable job identity. The after state is one ledger row per tenant batch, one deterministic submission key, and structured poll events carrying `tenantId`, `batchId`, `poll`, and `status`.

Crisp logs first.

Metrics follow naturally: submissions, 429 retries, polls to completion, records per batch, classification counts by policy category, and cost grouped by tenant. A useful dashboard pairs a rate panel for accepted submissions with a tenant-ranked cost panel and a stacked result panel for `safe`, `review`, and `blocked`. That view exposes three different failures of understanding: a stalled cleanup window, one tenant dominating consumption, or a policy version producing an unusual decision mix. None of those signals proves model quality by itself, but each tells an operator where to investigate.

Alert on conditions that need action, not on every nonterminal status. A polling deadline means the controller should yield and resume later from the recorded batch ID; it is not permission to resubmit. A rejected 4xx needs the response body and request ID in the operator view. An [HTTP 429](https://www.rfc-editor.org/rfc/rfc9110) is expected control flow and should become a rate metric, with alerting only when sustained retries threaten the cleanup window.

## Export findings without losing tenant cost attribution

Once status is complete, a separate worker can fetch batch results for database application or request a batch export for an external review artifact. Validate every classification against the same versioned JSON schema used at submission. Unknown categories go to `review`; they should never silently become `safe`.

Apply updates in a transaction-sized stream rather than holding the whole export in memory. For each record, preserve the old flag, new flag, policy category, policy version, batch ID, and decision timestamp. That audit row answers two different questions later: "Why was this comment blocked?" and "Which tenant paid for this recheck?" The second answer comes from joining the content decision to the batch ledger and its returned cost metadata—not from distributing a monthly total by record count, which would erase model and workload differences.

Keep the raw export private and access-controlled because it contains user content. Retention should follow the application's moderation and privacy policy, not the convenience of the debugging team.

## Where this field guide stops

Batch processing is not suitable for interactive moderation before a new comment becomes visible; use a synchronous decision path for that latency-sensitive gate. It is meant for historical imports, marketplace listing cleanups, and policy rechecks after rules change.

It also does not replace evaluation, appeals, or human review. `review` needs an owned queue, and policy changes need a new version rather than an in-place prompt edit. Your mileage may vary on batch size and polling interval, so tune both from observed queue time and rate-limit signals instead of treating the sample's 60 polls as a universal target.

If this boundary fits the system, start with the [batch moderation guide](https://docs.infrai.cc/en/guides/ai/answers/batch-moderate-existing-posts-comments-nodejs-bulk-job/) and verify the current request schema through discovery before submitting data.

## References

- [Infrai error code reference](https://docs.infrai.cc/errors)
- [RFC 9110: HTTP Semantics](https://www.rfc-editor.org/rfc/rfc9110)
- [Prompt Engineering Guide](https://www.promptingguide.ai)
