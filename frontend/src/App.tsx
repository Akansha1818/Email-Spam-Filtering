import { useState } from "react";
import { Activity, BarChart3, Mail, Network, ShieldCheck } from "lucide-react";
import { usePoll, type Cluster } from "./api";
import AnalyticsTab from "./components/AnalyticsTab";
import ClassifyTab from "./components/ClassifyTab";
import ClusterTab from "./components/ClusterTab";
import FeedTab from "./components/FeedTab";
import { StatusPill } from "./components/ui";

const TABS = [
  { key: "classify", label: "Classify", icon: Mail },
  { key: "feed", label: "Live Feed", icon: Activity },
  { key: "analytics", label: "Analytics", icon: BarChart3 },
  { key: "cluster", label: "Cluster", icon: Network },
] as const;
type Tab = (typeof TABS)[number]["key"];

export default function App() {
  const initial = window.location.hash.slice(1) as Tab;
  const [tab, setTabState] = useState<Tab>(TABS.some((t) => t.key === initial) ? initial : "classify");
  const setTab = (t: Tab) => { setTabState(t); window.history.replaceState(null, "", `#${t}`); };
  const { data: cluster } = usePoll<Cluster>("/api/cluster", 5000);

  return (
    <div className="min-h-screen">
      <header className="border-b border-line bg-surface">
        <div className="mx-auto flex max-w-7xl flex-wrap items-center justify-between gap-3 px-4 py-4 sm:px-6">
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-xl text-white" style={{ background: "var(--ham)" }}>
              <ShieldCheck size={22} />
            </div>
            <div>
              <h1 className="text-[17px] font-semibold leading-tight text-ink">MailShield</h1>
              <p className="text-[12px] text-muted">Email spam classification on Hadoop · Kafka · Spark · HBase</p>
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            <StatusPill name="HDFS" up={cluster?.hdfs.up} />
            <StatusPill name="Spark" up={cluster?.spark.up} />
            <StatusPill name="Kafka" up={cluster?.kafka.up} />
            <StatusPill name="HBase" up={cluster?.hbase.up} />
          </div>
        </div>
        <nav className="mx-auto flex max-w-7xl gap-1 overflow-x-auto px-4 sm:px-6">
          {TABS.map((t) => (
            <button
              key={t.key}
              onClick={() => setTab(t.key)}
              className={`inline-flex items-center gap-2 border-b-2 px-3 py-2.5 text-[13px] font-medium transition ${
                tab === t.key ? "border-ham text-ink" : "border-transparent text-muted hover:text-ink-2"
              }`}
            >
              <t.icon size={15} />
              {t.label}
            </button>
          ))}
        </nav>
      </header>

      <main className="mx-auto max-w-7xl px-4 py-6 sm:px-6">
        {tab === "classify" && <ClassifyTab />}
        {tab === "feed" && <FeedTab />}
        {tab === "analytics" && <AnalyticsTab />}
        {tab === "cluster" && <ClusterTab c={cluster} />}
      </main>
    </div>
  );
}
