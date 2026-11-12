import { Bar, BarChart, CartesianGrid, Legend, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { fmt, pct, usePoll, type ModelInfo, type Stats } from "../api";
import { Card, Empty, ErrorNote, SplitBar, Stat } from "./ui";

const axis = { stroke: "var(--muted)", fontSize: 11, tickLine: false, axisLine: false };
const tooltipStyle = {
  contentStyle: { background: "var(--surface)", border: "1px solid var(--border)", borderRadius: 10, fontSize: 12, color: "var(--text)" },
  labelStyle: { color: "var(--text)", fontWeight: 600 },
  itemStyle: { color: "var(--text-2)" },
  cursor: { fill: "var(--surface-2)" },
};

export default function AnalyticsTab() {
  const { data: s } = usePoll<Stats>("/api/stats?minutes=30", 3000);
  const { data: m, error } = usePoll<ModelInfo>("/api/model", 15000);

  return (
    <div className="space-y-5">
      <div className="grid gap-5 lg:grid-cols-[2fr_1fr]">
        <Card title="Emails classified per minute" subtitle="Last 30 minutes · HBase atomic counters updated by Spark Streaming">
          {s ? (
            <ResponsiveContainer width="100%" height={250}>
              <BarChart data={s.timeline} margin={{ top: 4, right: 4, left: -18, bottom: 0 }} barCategoryGap="18%">
                <CartesianGrid vertical={false} stroke="var(--grid)" />
                <XAxis dataKey="time" {...axis} interval={4} />
                <YAxis {...axis} allowDecimals={false} />
                <Tooltip {...tooltipStyle} />
                <Legend iconType="circle" iconSize={8} wrapperStyle={{ fontSize: 12, color: "var(--text-2)" }} />
                <Bar dataKey="ham" name="Ham" stackId="a" fill="var(--ham)" stroke="var(--surface)" strokeWidth={1} />
                <Bar dataKey="spam" name="Spam" stackId="a" fill="var(--spam)" stroke="var(--surface)" strokeWidth={1} radius={[4, 4, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          ) : <Empty>Loading…</Empty>}
        </Card>
        <Card title="Live traffic mix" subtitle="All emails classified so far">
          {s && s.total > 0 ? (
            <div className="space-y-5">
              <div className="tnum text-[40px] font-semibold leading-none text-ink">{fmt(s.total)}<span className="ml-2 text-[14px] font-normal text-muted">emails</span></div>
              <SplitBar spam={s.spam} ham={s.ham} />
              <div className="rounded-lg bg-surface-2 p-3 text-[13px] text-ink-2">
                Live accuracy on labelled stream: <b className="text-ink">{s.live_accuracy != null ? pct(s.live_accuracy) : "–"}</b>
                <span className="text-muted"> ({fmt(s.labelled)} emails with known label)</span>
              </div>
            </div>
          ) : <Empty>No traffic yet.</Empty>}
        </Card>
      </div>

      {error && <ErrorNote>{error}</ErrorNote>}
      {m && <ModelSection m={m} />}
    </div>
  );
}

function ModelSection({ m }: { m: ModelInfo }) {
  const md = m.model;
  const dep = md.algorithms[md.deployed];
  const c = dep.confusion;
  const ins = m.insights;
  return (
    <>
      <div className="grid grid-cols-2 gap-4 md:grid-cols-5">
        <Stat label="Accuracy" value={pct(dep.accuracy)} hint={dep.name} />
        <Stat label="Precision" value={pct(dep.precision)} hint="of predicted spam" />
        <Stat label="Recall" value={pct(dep.recall)} hint="of actual spam caught" />
        <Stat label="F1 score" value={dep.f1.toFixed(3)} />
        <Stat label="ROC AUC" value={dep.auc.toFixed(3)} />
      </div>

      <div className="grid gap-5 lg:grid-cols-2">
        <Card title="Algorithm comparison" subtitle={`Trained with Spark MLlib on ${fmt(md.train_rows)} emails, tested on ${fmt(md.test_rows)} · TF-IDF ${fmt(md.num_features)} features`}>
          <div className="-mx-5 overflow-x-auto">
            <table className="w-full text-[13px]">
              <thead className="text-[11px] uppercase tracking-wide text-muted">
                <tr className="border-b border-line">
                  <th className="px-5 py-2 text-left font-medium">Algorithm</th>
                  {["Accuracy", "Precision", "Recall", "F1", "AUC", "Train"].map((h) => <th key={h} className="py-2 pr-5 text-right font-medium">{h}</th>)}
                </tr>
              </thead>
              <tbody className="tnum">
                {Object.entries(md.algorithms).map(([k, a]) => (
                  <tr key={k} className="border-b border-line last:border-0">
                    <td className="px-5 py-2.5 font-medium text-ink">
                      {a.name}
                      {k === md.deployed && <span className="ml-2 rounded-full bg-surface-2 px-2 py-0.5 text-[10px] font-semibold uppercase text-ink-2">deployed</span>}
                    </td>
                    <td className="py-2.5 pr-5 text-right">{pct(a.accuracy, 2)}</td>
                    <td className="py-2.5 pr-5 text-right">{pct(a.precision, 2)}</td>
                    <td className="py-2.5 pr-5 text-right">{pct(a.recall, 2)}</td>
                    <td className="py-2.5 pr-5 text-right">{a.f1.toFixed(4)}</td>
                    <td className="py-2.5 pr-5 text-right">{a.auc.toFixed(4)}</td>
                    <td className="py-2.5 pr-5 text-right text-ink-2">{a.train_seconds}s</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="mt-3 text-[12px] text-muted">Trained {new Date(md.trained_at).toLocaleString()} in {md.total_seconds}s on {md.executors} Spark executor(s).</p>
        </Card>

        <Card title="Confusion matrix" subtitle={`${dep.name} · ${fmt(dep.test_rows)} held-out test emails`}>
          <div className="grid grid-cols-[auto_1fr_1fr] gap-2 text-[13px]">
            <div />
            <div className="text-center text-[11px] font-medium uppercase tracking-wide text-muted">Predicted ham</div>
            <div className="text-center text-[11px] font-medium uppercase tracking-wide text-muted">Predicted spam</div>
            <div className="flex items-center pr-2 text-[11px] font-medium uppercase tracking-wide text-muted">Actual ham</div>
            <Cell n={c.tn} label="True negatives" good />
            <Cell n={c.fp} label="False positives (ham lost)" />
            <div className="flex items-center pr-2 text-[11px] font-medium uppercase tracking-wide text-muted">Actual spam</div>
            <Cell n={c.fn} label="False negatives (spam missed)" />
            <Cell n={c.tp} label="True positives" good />
          </div>
        </Card>
      </div>

      {ins && (
        <>
          <div className="grid gap-5 lg:grid-cols-2">
            <WordChart title="Most spam-indicative words" subtitle="Words far more common in spam (Spark aggregation over the dataset)" data={ins.top_spam_words} color="var(--spam)" />
            <WordChart title="Most ham-indicative words" subtitle="Words far more common in legitimate (Enron) mail" data={ins.top_ham_words} color="var(--ham)" />
          </div>
          <div className="grid gap-5 lg:grid-cols-[1fr_1fr]">
            <Card title="Email length distribution" subtitle="Number of words after stop-word removal">
              <ResponsiveContainer width="100%" height={230}>
                <BarChart data={ins.length_hist} margin={{ top: 4, right: 4, left: -10, bottom: 0 }} barGap={2}>
                  <CartesianGrid vertical={false} stroke="var(--grid)" />
                  <XAxis dataKey="bucket" {...axis} />
                  <YAxis {...axis} />
                  <Tooltip {...tooltipStyle} />
                  <Legend iconType="circle" iconSize={8} wrapperStyle={{ fontSize: 12, color: "var(--text-2)" }} />
                  <Bar dataKey="ham" name="Ham" fill="var(--ham)" radius={[4, 4, 0, 0]} />
                  <Bar dataKey="spam" name="Spam" fill="var(--spam)" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </Card>
            <Card title="Training dataset" subtitle="Enron-Spam corpus stored in HDFS">
              <div className="space-y-4">
                <div className="tnum text-[32px] font-semibold text-ink">{fmt(ins.total)}<span className="ml-2 text-[14px] font-normal text-muted">unique emails</span></div>
                <SplitBar spam={ins.spam} ham={ins.ham} />
                <table className="w-full text-[13px]">
                  <thead className="text-[11px] uppercase tracking-wide text-muted">
                    <tr><th className="py-1 text-left font-medium">Signal</th><th className="py-1 text-right font-medium">in spam</th><th className="py-1 text-right font-medium">in ham</th></tr>
                  </thead>
                  <tbody className="tnum text-ink-2">
                    <tr><td className="py-1">Average words</td><td className="text-right">{ins.avg_words.spam}</td><td className="text-right">{ins.avg_words.ham}</td></tr>
                    {Object.entries(ins.signals).map(([k, v]) => (
                      <tr key={k}><td className="py-1">Contains {k === "url" ? "a link" : k === "money" ? "a money amount" : "an e-mail address"}</td>
                        <td className="text-right">{pct(v.spam ?? 0)}</td><td className="text-right">{pct(v.ham ?? 0)}</td></tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          </div>
        </>
      )}
    </>
  );
}

function Cell({ n, label, good }: { n: number; label: string; good?: boolean }) {
  const color = good ? "var(--good)" : "var(--bad)";
  return (
    <div className="rounded-xl p-4 text-center" style={{ background: `color-mix(in srgb, ${color} ${good ? 12 : 9}%, transparent)` }}>
      <div className="tnum text-[26px] font-semibold text-ink">{fmt(n)}</div>
      <div className="text-[11px] text-ink-2">{label}</div>
    </div>
  );
}

function WordChart({ title, subtitle, data, color }: { title: string; subtitle: string; data: { word: string; count: number }[]; color: string }) {
  return (
    <Card title={title} subtitle={subtitle}>
      <ResponsiveContainer width="100%" height={Math.max(200, data.length * 22)}>
        <BarChart data={data} layout="vertical" margin={{ top: 0, right: 16, left: 8, bottom: 0 }} barCategoryGap={3}>
          <XAxis type="number" hide />
          <YAxis type="category" dataKey="word" {...axis} interval={0} width={90} tick={{ fill: "var(--text-2)", fontSize: 12 }} />
          <Tooltip {...tooltipStyle} formatter={(v: number) => [`${fmt(v)} emails`, "appears in"]} />
          <Bar dataKey="count" fill={color} radius={[0, 4, 4, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </Card>
  );
}
