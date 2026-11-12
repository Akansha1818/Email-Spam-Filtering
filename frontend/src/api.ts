import { useEffect, useRef, useState } from "react";

export type EmailRow = {
  rowkey: string;
  id: string;
  subject: string;
  body: string;
  sender: string;
  source: string;
  label: "spam" | "ham";
  p_spam: number;
  confidence: number;
  true_label: number | null;
  latency_ms: number;
  sent_at: number;
  processed_at: number;
  model: string;
  batch_id: number;
  kafka_partition: number;
  kafka_offset: number;
  roundtrip_ms?: number;
};

export type Stats = {
  total: number;
  spam: number;
  ham: number;
  spam_rate: number;
  labelled: number;
  live_accuracy: number | null;
  timeline: { time: string; spam: number; ham: number }[];
};

export type AlgoMetrics = {
  name: string;
  accuracy: number;
  precision: number;
  recall: number;
  f1: number;
  auc: number;
  train_seconds: number;
  test_rows: number;
  confusion: { tp: number; tn: number; fp: number; fn: number };
};

export type ModelInfo = {
  model: {
    deployed: string;
    deployed_name: string;
    algorithms: Record<string, AlgoMetrics>;
    train_rows: number;
    test_rows: number;
    num_features: number;
    trained_at: number;
    total_seconds: number;
    executors: number;
  };
  insights: {
    total: number;
    spam: number;
    ham: number;
    avg_words: { spam: number; ham: number };
    top_spam_words: { word: string; count: number }[];
    top_ham_words: { word: string; count: number }[];
    signals: Record<string, { spam: number; ham: number }>;
    length_hist: { bucket: string; spam: number; ham: number }[];
  } | null;
};

export type Cluster = {
  master_ip: string;
  hdfs: { up: boolean; error?: string; live_datanodes?: number; dead_datanodes?: number; files?: number; blocks?: number;
    capacity?: number; used?: number; datanodes?: { name: string; used: number; capacity: number; blocks: number }[]; ui?: string };
  spark: { up: boolean; error?: string; alive_workers?: number; cores?: number; cores_used?: number;
    workers?: { host: string; cores: number; cores_used: number; memory: number; memory_used: number; state: string }[];
    apps?: { name: string; cores: number; state: string }[]; ui?: string };
  kafka: { up: boolean; error?: string; broker?: string; topic?: string; messages?: number;
    partitions?: { partition: number; end_offset: number }[] };
  hbase: { up: boolean; error?: string; tables?: string[]; thrift?: string; ui?: string };
};

export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, init);
  if (!res.ok) {
    let msg = res.statusText;
    try { msg = (await res.json()).detail ?? msg; } catch { /* not json */ }
    throw new Error(msg);
  }
  return res.json();
}

/** Fetches `path` now and then every `ms` milliseconds. */
export function usePoll<T>(path: string, ms: number) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const live = useRef(true);
  useEffect(() => {
    live.current = true;
    const load = () =>
      api<T>(path)
        .then((d) => { if (live.current) { setData(d); setError(null); } })
        .catch((e) => { if (live.current) setError(String(e.message ?? e)); });
    load();
    const t = setInterval(load, ms);
    return () => { live.current = false; clearInterval(t); };
  }, [path, ms]);
  return { data, error };
}

export const pct = (v: number, digits = 1) => `${(v * 100).toFixed(digits)}%`;
export const fmt = (v: number) => v.toLocaleString();
export const bytes = (b: number) => {
  const u = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
  return `${b.toFixed(i ? 1 : 0)} ${u[i]}`;
};
