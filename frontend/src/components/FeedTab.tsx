import { Fragment, useState } from "react";
import { Check, X } from "lucide-react";
import { pct, usePoll, type EmailRow, type Stats } from "../api";
import { Card, Empty, ErrorNote, LabelBadge, Stat } from "./ui";

const FILTERS = [
  { key: "", label: "All" },
  { key: "spam", label: "Spam" },
  { key: "ham", label: "Ham" },
];

export default function FeedTab() {
  const [filter, setFilter] = useState("");
  const [open, setOpen] = useState<string | null>(null);
  const { data: rows, error } = usePoll<EmailRow[]>(`/api/emails?limit=60${filter ? `&label=${filter}` : ""}`, 2000);
  const { data: s } = usePoll<Stats>("/api/stats?minutes=5", 2000);

  const lastMin = s?.timeline.at(-1);
  return (
    <div className="space-y-5">
      <div className="grid grid-cols-2 gap-4 md:grid-cols-5">
        <Stat label="Classified" value={s ? s.total.toLocaleString() : "–"} hint="all time" />
        <Stat label="Spam" accent="var(--spam)" value={s ? s.spam.toLocaleString() : "–"} hint={s ? `${pct(s.spam_rate)} of all` : ""} />
        <Stat label="Ham" accent="var(--ham)" value={s ? s.ham.toLocaleString() : "–"} hint={s ? `${pct(1 - s.spam_rate)} of all` : ""} />
        <Stat label="Live accuracy" value={s?.live_accuracy != null ? pct(s.live_accuracy) : "–"} hint={s ? `on ${s.labelled.toLocaleString()} labelled emails` : ""} />
        <Stat label="This minute" value={lastMin ? (lastMin.spam + lastMin.ham).toLocaleString() : "–"} hint="emails / min" />
      </div>

      <Card
        title={<span className="inline-flex items-center gap-2"><span className="pulse-dot h-2 w-2 rounded-full" style={{ background: "var(--good)" }} />Live classification feed</span>}
        subtitle="Newest first · read from HBase every 2 seconds"
        right={
          <div className="flex rounded-lg border border-line p-0.5">
            {FILTERS.map((f) => (
              <button key={f.key} onClick={() => setFilter(f.key)}
                className={`rounded-md px-3 py-1 text-[12px] font-medium ${filter === f.key ? "bg-surface-2 text-ink" : "text-muted"}`}>
                {f.label}
              </button>
            ))}
          </div>
        }
      >
        {error && <ErrorNote>{error}</ErrorNote>}
        {rows && rows.length === 0 && <Empty>No emails yet. Run <code>scripts/run-producer.sh</code> or classify one.</Empty>}
        {rows && rows.length > 0 && (
          <div className="-mx-5 overflow-x-auto">
            <table className="w-full min-w-[760px] text-left text-[13px]">
              <thead className="text-[11px] uppercase tracking-wide text-muted">
                <tr className="border-b border-line">
                  <th className="px-5 py-2 font-medium">Prediction</th>
                  <th className="py-2 font-medium">Email</th>
                  <th className="py-2 text-right font-medium">Confidence</th>
                  <th className="py-2 text-right font-medium">Latency</th>
                  <th className="py-2 pl-4 font-medium">Source</th>
                  <th className="px-5 py-2 text-center font-medium">Correct?</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <Fragment key={r.rowkey}>
                    <tr onClick={() => setOpen(open === r.rowkey ? null : r.rowkey)}
                      className="cursor-pointer border-b border-line hover:bg-surface-2">
                      <td className="px-5 py-2.5"><LabelBadge label={r.label} /></td>
                      <td className="max-w-[380px] py-2.5">
                        <div className="truncate font-medium text-ink">{r.subject || "(no subject)"}</div>
                        <div className="truncate text-[12px] text-muted">{r.sender}</div>
                      </td>
                      <td className="tnum py-2.5 text-right">{pct(r.confidence)}</td>
                      <td className="tnum py-2.5 text-right text-ink-2">{r.latency_ms} ms</td>
                      <td className="py-2.5 pl-4 text-ink-2">{r.source}</td>
                      <td className="px-5 py-2.5 text-center">
                        {r.true_label == null ? <span className="text-muted">–</span>
                          : (r.true_label === 1) === (r.label === "spam")
                            ? <Check size={16} className="inline" style={{ color: "var(--good)" }} aria-label="correct" />
                            : <X size={16} className="inline" style={{ color: "var(--bad)" }} aria-label="wrong" />}
                      </td>
                    </tr>
                    {open === r.rowkey && (
                      <tr className="border-b border-line bg-surface-2">
                        <td colSpan={6} className="px-5 py-3 text-[12px] text-ink-2">
                          <p className="whitespace-pre-wrap">{r.body || "(empty body)"}</p>
                          <p className="mt-2 font-mono text-[11px] text-muted">
                            rowkey {r.rowkey} · kafka p{r.kafka_partition}@{r.kafka_offset} · batch #{r.batch_id} · P(spam) {r.p_spam.toFixed(4)}
                          </p>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </div>
  );
}
