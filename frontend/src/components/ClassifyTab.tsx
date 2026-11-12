import { useState } from "react";
import { Cpu, Database, Loader2, Radio, Send, Server } from "lucide-react";
import { api, pct, type EmailRow } from "../api";
import { SAMPLES } from "../samples";
import { Card, ErrorNote, LabelBadge } from "./ui";

export default function ClassifyTab() {
  const [subject, setSubject] = useState("");
  const [body, setBody] = useState("");
  const [sender, setSender] = useState("");
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<EmailRow | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function submit() {
    if (!body.trim()) return;
    setBusy(true); setError(null); setResult(null);
    try {
      const r = await api<EmailRow>("/api/classify", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ subject, body, sender: sender || "user@dashboard.local" }),
      });
      setResult(r);
    } catch (e) {
      setError(String((e as Error).message));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="grid gap-5 lg:grid-cols-[1.15fr_1fr]">
      <Card title="Classify an email" subtitle="Your email is sent through Kafka → Spark Streaming → HBase on the cluster">
        <div className="mb-4 flex flex-wrap gap-2">
          {SAMPLES.map((s) => (
            <button
              key={s.title}
              onClick={() => { setSubject(s.subject); setBody(s.body); setSender(s.sender); setResult(null); }}
              className="inline-flex items-center gap-1.5 rounded-lg border border-line bg-surface-2 px-2.5 py-1.5 text-[12px] font-medium text-ink-2 transition hover:border-ink-2"
            >
              <span className="h-1.5 w-1.5 rounded-full" style={{ background: s.kind === "spam" ? "var(--spam)" : "var(--ham)" }} />
              {s.title}
            </button>
          ))}
        </div>
        <div className="space-y-3">
          <input value={sender} onChange={(e) => setSender(e.target.value)} placeholder="From (optional)"
            className="w-full rounded-lg border border-line bg-surface px-3 py-2 text-[14px] outline-none focus:border-ham" />
          <input value={subject} onChange={(e) => setSubject(e.target.value)} placeholder="Subject"
            className="w-full rounded-lg border border-line bg-surface px-3 py-2 text-[14px] outline-none focus:border-ham" />
          <textarea value={body} onChange={(e) => setBody(e.target.value)} placeholder="Paste or type the email body…" rows={9}
            className="w-full resize-y rounded-lg border border-line bg-surface px-3 py-2 text-[14px] leading-relaxed outline-none focus:border-ham" />
          <div className="flex items-center justify-between gap-3">
            <span className="text-[12px] text-muted">{body.length.toLocaleString()} characters</span>
            <div className="flex gap-2">
              <button onClick={() => { setSubject(""); setBody(""); setSender(""); setResult(null); setError(null); }}
                className="rounded-lg px-3 py-2 text-[13px] font-medium text-ink-2 hover:bg-surface-2">Clear</button>
              <button onClick={submit} disabled={busy || !body.trim()}
                className="inline-flex items-center gap-2 rounded-lg px-4 py-2 text-[13px] font-semibold text-white transition disabled:opacity-50"
                style={{ background: "var(--ham)" }}>
                {busy ? <Loader2 size={15} className="animate-spin" /> : <Send size={15} />}
                {busy ? "Classifying on cluster…" : "Classify"}
              </button>
            </div>
          </div>
        </div>
      </Card>

      <div className="space-y-5">
        {error && <ErrorNote>{error}</ErrorNote>}
        {!result && !error && (
          <Card title="Result">
            <div className="flex h-56 flex-col items-center justify-center gap-2 text-center text-[13px] text-muted">
              {busy ? <Loader2 className="animate-spin" /> : <Radio />}
              {busy ? "Waiting for the Spark streaming job to score your email…" : "Pick a sample or write an email, then press Classify."}
            </div>
          </Card>
        )}
        {result && <ResultCard r={result} />}
      </div>
    </div>
  );
}

function ResultCard({ r }: { r: EmailRow }) {
  const color = r.label === "spam" ? "var(--spam)" : "var(--ham)";
  const steps = [
    { icon: Server, title: "FastAPI → Kafka", detail: <>topic <b>emails</b> · partition {r.kafka_partition} · offset {r.kafka_offset}</> },
    { icon: Cpu, title: "Spark Structured Streaming", detail: <>micro-batch #{r.batch_id} · {r.model} model loaded from HDFS</> },
    { icon: Database, title: "HBase", detail: <>table <b>emails</b> · row key <code className="font-mono text-[11px]">{r.rowkey}</code></> },
  ];
  return (
    <>
      <Card title="Result" right={<LabelBadge label={r.label} size="lg" />}>
        <div className="flex items-end justify-between">
          <div>
            <div className="text-[12px] font-medium uppercase tracking-wide text-muted">Confidence</div>
            <div className="tnum text-[40px] font-semibold leading-none" style={{ color }}>{pct(r.confidence)}</div>
          </div>
          <div className="tnum text-right text-[13px] text-ink-2">
            <div>P(spam) = <b className="text-ink">{r.p_spam.toFixed(4)}</b></div>
            <div>pipeline latency <b className="text-ink">{r.latency_ms} ms</b></div>
            <div>round trip <b className="text-ink">{r.roundtrip_ms} ms</b></div>
          </div>
        </div>
        <div className="mt-4 h-2.5 overflow-hidden rounded-full bg-surface-2" title={`P(spam) ${pct(r.p_spam)}`}>
          <div className="h-full rounded-full" style={{ width: `${Math.max(2, r.p_spam * 100)}%`, background: "var(--spam)" }} />
        </div>
        <div className="mt-1 flex justify-between text-[11px] text-muted"><span>ham</span><span>P(spam)</span><span>spam</span></div>
      </Card>
      <Card title="How this email travelled through the cluster">
        <ol className="space-y-3">
          {steps.map((s, i) => (
            <li key={i} className="flex gap-3">
              <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-surface-2 text-ink-2"><s.icon size={16} /></div>
              <div className="min-w-0">
                <div className="text-[13px] font-semibold text-ink">{s.title}</div>
                <div className="break-all text-[12px] text-ink-2">{s.detail}</div>
              </div>
            </li>
          ))}
        </ol>
      </Card>
    </>
  );
}
