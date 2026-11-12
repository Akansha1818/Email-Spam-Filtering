import type { ReactNode } from "react";
import { Cpu, Database, ExternalLink, HardDrive, Radio } from "lucide-react";
import { bytes, fmt, type Cluster } from "../api";
import { Card, Empty, StatusPill } from "./ui";

function Meter({ value, max, label }: { value: number; max: number; label: string }) {
  const p = max ? Math.min(100, (value / max) * 100) : 0;
  return (
    <div>
      <div className="flex justify-between text-[11px] text-muted"><span>{label}</span><span className="tnum">{p.toFixed(0)}%</span></div>
      <div className="mt-1 h-1.5 overflow-hidden rounded-full bg-surface-2">
        <div className="h-full rounded-full" style={{ width: `${Math.max(p, 1)}%`, background: "var(--ham)" }} />
      </div>
    </div>
  );
}

function Service({ icon: Icon, name, role, up, ui, children }:
  { icon: typeof Cpu; name: string; role: string; up?: boolean; ui?: string; children: ReactNode }) {
  return (
    <Card
      title={<span className="inline-flex items-center gap-2"><Icon size={17} className="text-ink-2" />{name}</span>}
      subtitle={role}
      right={<div className="flex items-center gap-2">
        <StatusPill name="" up={up} />
        {ui && <a href={ui} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 text-[12px] font-medium text-ham hover:underline">UI <ExternalLink size={12} /></a>}
      </div>}
    >
      {children}
    </Card>
  );
}

const Node = ({ children }: { children: ReactNode }) => (
  <div className="rounded-xl border border-line bg-surface-2/50 p-3">{children}</div>
);

export default function ClusterTab({ c }: { c: Cluster | null }) {
  if (!c) return <Empty>Loading cluster status…</Empty>;
  return (
    <div className="space-y-5">
      <div className="card flex flex-wrap items-center gap-x-6 gap-y-2 p-4 text-[13px] text-ink-2">
        <span><b className="text-ink">Master</b> {c.master_ip} · NameNode, Spark Master, Kafka, HBase, API</span>
        <span><b className="text-ink">Workers</b> {c.hdfs.live_datanodes ?? 0} DataNode(s) · {c.spark.alive_workers ?? 0} Spark Worker(s)</span>
      </div>
      <div className="grid gap-5 lg:grid-cols-2">
        <Service icon={HardDrive} name="HDFS" role="Distributed storage: dataset, model, checkpoints, HBase files" up={c.hdfs.up} ui={c.hdfs.ui}>
          {c.hdfs.up ? (
            <div className="space-y-3">
              <div className="tnum grid grid-cols-3 gap-3 text-[13px]">
                <div><div className="text-[11px] text-muted">Live DataNodes</div><b className="text-[20px] text-ink">{c.hdfs.live_datanodes}</b></div>
                <div><div className="text-[11px] text-muted">Files</div><b className="text-[20px] text-ink">{fmt(c.hdfs.files ?? 0)}</b></div>
                <div><div className="text-[11px] text-muted">Blocks</div><b className="text-[20px] text-ink">{fmt(c.hdfs.blocks ?? 0)}</b></div>
              </div>
              {c.hdfs.datanodes?.map((d) => (
                <Node key={d.name}>
                  <div className="mb-2 flex justify-between text-[13px]"><b className="text-ink">DataNode {d.name}</b><span className="tnum text-ink-2">{fmt(d.blocks)} blocks · {bytes(d.used)}</span></div>
                  <Meter value={d.used} max={d.capacity} label={`HDFS used of ${bytes(d.capacity)}`} />
                </Node>
              ))}
            </div>
          ) : <p className="text-[13px] text-bad">{c.hdfs.error}</p>}
        </Service>

        <Service icon={Cpu} name="Apache Spark" role="Standalone cluster: training + streaming classification" up={c.spark.up} ui={c.spark.ui}>
          {c.spark.up ? (
            <div className="space-y-3">
              {c.spark.workers?.length ? c.spark.workers.map((w) => (
                <Node key={w.host}>
                  <div className="mb-2 flex justify-between text-[13px]"><b className="text-ink">Worker {w.host}</b><span className="text-ink-2">{w.state}</span></div>
                  <div className="grid grid-cols-2 gap-3">
                    <Meter value={w.cores_used} max={w.cores} label={`${w.cores_used}/${w.cores} cores`} />
                    <Meter value={w.memory_used} max={w.memory} label={`${w.memory_used}/${w.memory} MB`} />
                  </div>
                </Node>
              )) : <p className="text-[13px] text-muted">No workers connected.</p>}
              <div className="text-[13px]">
                <div className="mb-1 text-[11px] uppercase tracking-wide text-muted">Running applications</div>
                {c.spark.apps?.length ? c.spark.apps.map((a) => (
                  <div key={a.name} className="flex justify-between py-0.5"><span className="font-medium text-ink">{a.name}</span><span className="text-ink-2">{a.cores} cores · {a.state}</span></div>
                )) : <span className="text-muted">none</span>}
              </div>
            </div>
          ) : <p className="text-[13px] text-bad">{c.spark.error}</p>}
        </Service>

        <Service icon={Radio} name="Apache Kafka" role="Single KRaft broker: buffers incoming emails" up={c.kafka.up}>
          {c.kafka.up ? (
            <div className="space-y-3 text-[13px]">
              <div className="text-ink-2">Broker <b className="text-ink">{c.kafka.broker}</b> · topic <b className="text-ink">{c.kafka.topic}</b> · <b className="text-ink">{fmt(c.kafka.messages ?? 0)}</b> messages</div>
              <div className="grid grid-cols-2 gap-3">
                {c.kafka.partitions?.map((p) => (
                  <Node key={p.partition}><div className="text-[11px] text-muted">Partition {p.partition}</div><b className="tnum text-[18px] text-ink">{fmt(p.end_offset)}</b> <span className="text-[11px] text-muted">offset</span></Node>
                ))}
              </div>
            </div>
          ) : <p className="text-[13px] text-bad">{c.kafka.error}</p>}
        </Service>

        <Service icon={Database} name="Apache HBase" role="Stores every prediction + live counters (data files in HDFS)" up={c.hbase.up} ui={c.hbase.ui}>
          {c.hbase.up ? (
            <div className="space-y-2 text-[13px] text-ink-2">
              <div>Thrift gateway <b className="text-ink">{c.hbase.thrift}</b></div>
              {c.hbase.tables?.map((t) => (
                <Node key={t}>
                  <b className="font-mono text-ink">{t}</b>
                  <span className="ml-2 text-[12px]">{t === "emails" ? "row per email · families d (data), p (prediction)" : "counters c:*, JSON m:* (metrics, insights)"}</span>
                </Node>
              ))}
            </div>
          ) : <p className="text-[13px] text-bad">{c.hbase.error}</p>}
        </Service>
      </div>
    </div>
  );
}
