import type { ReactNode } from "react";
import { CheckCircle2, XCircle } from "lucide-react";

export function Card({ title, subtitle, right, children, className = "" }:
  { title?: ReactNode; subtitle?: ReactNode; right?: ReactNode; children: ReactNode; className?: string }) {
  return (
    <section className={`card p-5 ${className}`}>
      {(title || right) && (
        <header className="mb-4 flex items-start justify-between gap-3">
          <div>
            {title && <h3 className="text-[15px] font-semibold text-ink">{title}</h3>}
            {subtitle && <p className="mt-0.5 text-[13px] text-muted">{subtitle}</p>}
          </div>
          {right}
        </header>
      )}
      {children}
    </section>
  );
}

export function Stat({ label, value, hint, accent }: { label: string; value: ReactNode; hint?: ReactNode; accent?: string }) {
  return (
    <div className="card p-4">
      <div className="flex items-center gap-2 text-[12px] font-medium uppercase tracking-wide text-muted">
        {accent && <span className="h-2 w-2 rounded-full" style={{ background: accent }} />}
        {label}
      </div>
      <div className="tnum mt-1.5 text-[28px] font-semibold leading-tight text-ink">{value}</div>
      {hint && <div className="mt-0.5 text-[12px] text-muted">{hint}</div>}
    </div>
  );
}

export function LabelBadge({ label, size = "sm" }: { label: "spam" | "ham"; size?: "sm" | "lg" }) {
  const color = label === "spam" ? "var(--spam)" : "var(--ham)";
  const big = size === "lg";
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full font-semibold uppercase tracking-wide ${big ? "px-4 py-1.5 text-[15px]" : "px-2 py-0.5 text-[11px]"}`}
      style={{ color, background: `color-mix(in srgb, ${color} 14%, transparent)` }}
    >
      <span className={`${big ? "h-2.5 w-2.5" : "h-1.5 w-1.5"} rounded-full`} style={{ background: color }} />
      {label === "spam" ? "Spam" : "Ham"}
    </span>
  );
}

export function StatusPill({ name, up }: { name: string; up: boolean | undefined }) {
  const Icon = up ? CheckCircle2 : XCircle;
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full border border-line bg-surface px-2.5 py-1 text-[12px] font-medium text-ink-2">
      <Icon size={14} style={{ color: up ? "var(--good)" : up === undefined ? "var(--muted)" : "var(--bad)" }} />
      {name}
      <span className="text-muted">{up ? "up" : up === undefined ? "…" : "down"}</span>
    </span>
  );
}

/** Two-part proportion bar (spam vs ham), labelled so colour is never the only cue. */
export function SplitBar({ spam, ham }: { spam: number; ham: number }) {
  const total = spam + ham || 1;
  const s = (spam / total) * 100;
  return (
    <div>
      <div className="flex h-3 gap-[2px] overflow-hidden rounded-full">
        <div style={{ width: `${s}%`, background: "var(--spam)" }} className="rounded-l-full" />
        <div style={{ width: `${100 - s}%`, background: "var(--ham)" }} className="rounded-r-full" />
      </div>
      <div className="tnum mt-2 flex justify-between text-[12px] text-ink-2">
        <span><b className="text-ink">{s.toFixed(1)}%</b> spam · {spam.toLocaleString()}</span>
        <span>{ham.toLocaleString()} · ham <b className="text-ink">{(100 - s).toFixed(1)}%</b></span>
      </div>
    </div>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return <div className="rounded-xl border border-dashed border-line p-8 text-center text-[13px] text-muted">{children}</div>;
}

export function ErrorNote({ children }: { children: ReactNode }) {
  return (
    <div className="rounded-xl border px-4 py-3 text-[13px]" style={{ borderColor: "var(--bad)", color: "var(--bad)" }}>
      {children}
    </div>
  );
}
