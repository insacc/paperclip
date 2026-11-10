/**
 * Time faults (TM1–TM3): the fourth fault family, next to restarts (K*), storage
 * (ST*) and provider messages (PV*). Every other wave is minutes long on a fresh
 * instance, so nothing time-bound ever expires. These faults make deadlines lapse
 * during a run or across a restart, without changing product code:
 *
 * - TM1 freeze: SIGSTOP the server process alone (not its runners, not Postgres)
 *   for 95–150 s, then SIGCONT. Longer than the controller-instance lease (90 s),
 *   the execution-control deadline (60 s) and the legacy run lease (60 s), so the
 *   server's own leases lapse while it believes it is healthy (a GC pause), while
 *   the runner keeps working and calling a server that does not answer.
 * - TM2 sleep: SIGSTOP the server and every process under it (runners, providers),
 *   shift every timestamp in the wave's database back by the age (default 26 h),
 *   then SIGCONT. A laptop-sleep stand-in: the database says the age has passed,
 *   while the frozen processes' own monotonic timers barely moved.
 * - TM3 aged outage: SIGKILL the server tree (as K1), shift every timestamp back
 *   by the age while it is down, then restart. The replacement must reconcile
 *   runs, leases, wakes and waits that look hours old. With `--workloads G1
 *   --triggers approval` this is the aged wait: an approval pending for a day
 *   across a restart (the class of #20340).
 *
 * Aging shifts every `timestamp` column of every table in the `public` schema by
 * the same interval, so the database is exactly what it would be had everything
 * happened the age earlier. Triggers are disabled for the shift (replica role)
 * so derived columns are not rewritten. The rows of the time-bound tables are
 * recorded with their values before the shift in `time-faults/<faultId>.json`.
 *
 * Grading reads run times through `TimeShiftedApi`, which adds the age back to
 * any timestamp the shift moved, so R4 overlaps and speed budgets are judged on
 * real time. The product itself sees only the aged values.
 */
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { createDb } from "../../packages/db/src/index.js";
import { ChaosApi } from "./api.js";
import type { TimeFaultKind } from "./time-faults-config.js";

export { DEFAULT_TIME_AGE_MS, TIME_FAULT_KINDS, TIME_FREEZE_HOLD_RANGE_MS, isTimeFault, timeFaultHoldMs, type TimeFaultKind } from "./time-faults-config.js";

/** Tables whose rows carry the deadlines a run depends on; their rows are recorded in the evidence. */
export const TIME_BOUND_TABLES = [
  "heartbeat_runs", "native_run_finalizations", "controller_instances", "agent_wakeup_requests", "environment_leases",
  "execution_workspace_runtime_leases", "workspace_operations", "approvals", "decisions", "issue_thread_interactions",
  "budget_resume_claims", "routine_triggers", "issue_recovery_actions",
] as const;
const RECORDED_ROWS_PER_TABLE = 200;

export interface AgingShift {
  faultId: string;
  ageMs: number;
  /** Harness clock when the shift committed. Every value it moved is now at or before `appliedAt - ageMs`. */
  appliedAt: number;
}

export interface AgedTable { table: string; columns: string[]; rows: number }

export interface AgingResult extends AgingShift {
  startedAt: number;
  tables: AgedTable[];
  totalRows: number;
  /** False when the database role could not disable triggers (the shift then ran with them on). */
  triggersDisabled: boolean;
  /** TM2: lock waits on a frozen server's connection that needed a brief thaw before retrying. */
  lockRetries: number;
  evidencePath: string;
}

export interface TimeFaultRecord {
  kind: TimeFaultKind;
  /** TM1/TM2: the processes frozen (server first). */
  frozenPids?: number[];
  frozenAt?: number;
  thawedAt?: number;
  aging?: AgingResult;
  /** The frozen server exited on its own after SIGCONT (for example a lost-lease self-fence); a replacement was started. */
  serverExitedAfterThaw?: { code: number | null; signal: string | null };
}

export interface ProcessRow { pid: number; ppid: number; command: string }

export function parseProcessTable(output: string): ProcessRow[] {
  return output.split("\n").flatMap(line => {
    const match = /^\s*(\d+)\s+(\d+)\s+(.*)$/.exec(line);
    return match ? [{ pid: Number(match[1]), ppid: Number(match[2]), command: match[3]!.trim() }] : [];
  });
}

/**
 * Every process under `rootPid`, parents before children. Postgres never runs
 * under the server (it is wrapper-owned, beside it), and anything that looks like
 * Postgres is excluded anyway: freezing the database is a different fault.
 */
export function descendantPids(rootPid: number, table: readonly ProcessRow[]): number[] {
  const children = new Map<number, ProcessRow[]>();
  for (const row of table) children.set(row.ppid, [...(children.get(row.ppid) ?? []), row]);
  const out: number[] = [];
  const queue = [...(children.get(rootPid) ?? [])];
  while (queue.length) {
    const row = queue.shift()!;
    if (/(^|\/)postgres(\s|$)|postmaster/.test(row.command)) continue;
    out.push(row.pid);
    queue.push(...(children.get(row.pid) ?? []));
  }
  return out;
}

function readProcessTable(): ProcessRow[] {
  return parseProcessTable(execFileSync("ps", ["-A", "-o", "pid=,ppid=,command="], { encoding: "utf8", maxBuffer: 16 * 1024 * 1024 }));
}

const frozenEverywhere = new Set<number>();
let exitHookInstalled = false;

/**
 * SIGSTOP/SIGCONT for processes this wave started. Whatever happens to the
 * harness, a frozen process is continued on exit, so it can still be stopped.
 */
export class ProcessFreezer {
  private frozen: number[] = [];

  constructor(private readonly signal: (pid: number, signal: NodeJS.Signals) => void = (pid, sig) => process.kill(pid, sig),
    private readonly table: () => ProcessRow[] = readProcessTable) {
    if (!exitHookInstalled) {
      exitHookInstalled = true;
      process.on("exit", () => { for (const pid of frozenEverywhere) try { process.kill(pid, "SIGCONT"); } catch { /* gone */ } });
    }
  }

  get pids(): readonly number[] { return this.frozen; }

  /** Freezes the root first (it then cannot spawn), then its tree, rescanning once for late children. */
  freeze(rootPid: number, tree: boolean): number[] {
    this.stop(rootPid);
    if (tree) {
      for (let pass = 0; pass < 2; pass += 1) {
        for (const pid of descendantPids(rootPid, this.table())) if (!this.frozen.includes(pid)) this.stop(pid);
      }
    }
    return [...this.frozen];
  }

  thaw(): void {
    for (const pid of this.frozen) {
      try { this.signal(pid, "SIGCONT"); } catch { /* exited meanwhile */ }
      frozenEverywhere.delete(pid);
    }
    this.frozen = [];
  }

  private stop(pid: number) {
    try { this.signal(pid, "SIGSTOP"); } catch { return; }
    this.frozen.push(pid);
    frozenEverywhere.add(pid);
  }
}

/** The wave's database URL from the instance config the supervisor wrote. */
export function databaseUrlFromConfig(configPath: string): string {
  const config = JSON.parse(readFileSync(configPath, "utf8")) as { database?: { connectionString?: string } };
  const url = config.database?.connectionString;
  if (!url) throw new Error("Time fault: the instance config has no database connection string");
  return url;
}

const quote = (identifier: string) => `"${identifier.replace(/"/g, "\"\"")}"`;

/** One UPDATE per table, shifting all of its timestamp columns at once. */
export function agingStatements(columns: ReadonlyArray<{ table: string; column: string }>, ageMs: number): Array<{ table: string; columns: string[]; sql: string }> {
  if (!Number.isSafeInteger(ageMs) || ageMs <= 0) throw new Error("Aging needs a positive whole number of milliseconds");
  const byTable = new Map<string, string[]>();
  for (const { table, column } of columns) byTable.set(table, [...(byTable.get(table) ?? []), column]);
  return [...byTable.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([table, cols]) => ({
    table, columns: cols,
    sql: `update public.${quote(table)} set ${cols.map(col => `${quote(col)} = ${quote(col)} - interval '${ageMs} milliseconds'`).join(", ")}`
      + ` where ${cols.map(col => `${quote(col)} is not null`).join(" or ")}`,
  }));
}

const LOCK_NOT_AVAILABLE = "55P03";
const MAX_LOCK_ATTEMPTS = 6;

/**
 * Shifts every timestamp in the wave's database back by `ageMs` in one
 * transaction. `onLockTimeout` runs between attempts when a lock is held (TM2:
 * a frozen server's open transaction); it may thaw and refreeze the server.
 */
export async function ageWaveDatabase(input: { connectionString: string; faultId: string; ageMs: number; evidenceDir: string;
  /** Wave start on the harness clock: the shift must move every row the wave wrote below anything it did not move. */
  waveStartedAt: number; onLockTimeout?: () => Promise<void>; now?: () => number }): Promise<AgingResult> {
  const now = input.now ?? Date.now;
  const startedAt = now();
  if (startedAt - input.waveStartedAt >= input.ageMs - 60_000) {
    throw new Error(`Time fault ${input.faultId}: the wave has run ${Math.round((startedAt - input.waveStartedAt) / 60_000)} min, `
      + "too close to the age; raise --time-age-hours so shifted and unshifted times cannot overlap");
  }
  const db = createDb(input.connectionString);
  const sql = db.$client;
  try {
    const columns = await sql.unsafe(`select table_name as "table", column_name as "column" from information_schema.columns
      where table_schema = 'public' and data_type in ('timestamp with time zone', 'timestamp without time zone')
      order by table_name, ordinal_position`) as unknown as Array<{ table: string; column: string }>;
    const statements = agingStatements(columns, input.ageMs);
    const identityColumns = await sql.unsafe(`select table_name as "table", column_name as "column" from information_schema.columns
      where table_schema = 'public' and table_name = any($1) and column_name in ('id', 'boot_id', 'run_id', 'issue_id', 'status', 'phase', 'state')
      order by table_name, ordinal_position`, [[...TIME_BOUND_TABLES]]) as unknown as Array<{ table: string; column: string }>;
    const [role] = await sql.unsafe("select rolsuper from pg_roles where rolname = current_user") as unknown as Array<{ rolsuper: boolean }>;
    const triggersDisabled = role?.rolsuper === true;
    const recorded = new Set<string>(TIME_BOUND_TABLES);
    let lockRetries = 0;
    for (let attempt = 1; ; attempt += 1) {
      try {
        const outcome = await sql.begin(async tx => {
          await tx.unsafe("set local lock_timeout = '3s'");
          if (triggersDisabled) await tx.unsafe("set local session_replication_role = replica");
          const before: Record<string, unknown[]> = {};
          for (const statement of statements) {
            if (!recorded.has(statement.table)) continue;
            // Identity, state and times only: payload columns can carry run output.
            const keys = identityColumns.filter(key => key.table === statement.table).map(key => key.column);
            const select = [...keys, ...statement.columns].map(quote).join(", ");
            const order = statement.columns.includes("created_at") ? " order by created_at desc" : "";
            before[statement.table] = await tx.unsafe(`select ${select} from public.${quote(statement.table)}${order} limit ${RECORDED_ROWS_PER_TABLE}`) as unknown as unknown[];
          }
          const tables: AgedTable[] = [];
          for (const statement of statements) {
            const result = await tx.unsafe(statement.sql);
            tables.push({ table: statement.table, columns: statement.columns, rows: result.count ?? 0 });
          }
          return { before, tables };
        });
        const appliedAt = now();
        const tables = outcome.tables.filter(table => table.rows > 0);
        const evidencePath = path.join(input.evidenceDir, `${input.faultId}.json`);
        await mkdir(input.evidenceDir, { recursive: true, mode: 0o700 });
        await writeFile(evidencePath, `${JSON.stringify({ faultId: input.faultId, ageMs: input.ageMs, startedAt, appliedAt,
          note: "Rows of the time-bound tables as they were before the shift; after the shift every timestamp shown is ageMs earlier.",
          tables, rowsBefore: outcome.before }, null, 2)}\n`, { mode: 0o600 });
        return { faultId: input.faultId, ageMs: input.ageMs, appliedAt, startedAt, tables,
          totalRows: tables.reduce((sum, table) => sum + table.rows, 0), triggersDisabled, lockRetries, evidencePath };
      } catch (error) {
        if ((error as { code?: string }).code !== LOCK_NOT_AVAILABLE || attempt >= MAX_LOCK_ATTEMPTS) throw error;
        lockRetries += 1;
        await input.onLockTimeout?.();
      }
    }
  } finally {
    await sql.end({ timeout: 5 }).catch(() => undefined);
  }
}

const ISO_TIMESTAMP = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})$/;
/** Slack between a shifted value and the shift's commit on the harness clock. */
const SHIFT_SLACK_MS = 5_000;

/** The real time of a database time the shifts moved; other values are returned unchanged. Latest shift first. */
export function unshiftMs(value: number, shifts: readonly AgingShift[]): number {
  let real = value;
  for (const shift of [...shifts].sort((a, b) => b.appliedAt - a.appliedAt)) {
    if (real <= shift.appliedAt - shift.ageMs + SHIFT_SLACK_MS) real += shift.ageMs;
  }
  return real;
}

/** Copies `value`, with every ISO timestamp the shifts moved put back at its real time. */
export function unshiftJson<T>(value: T, shifts: readonly AgingShift[]): T {
  if (!shifts.length) return value;
  const visit = (node: unknown): unknown => {
    if (typeof node === "string") {
      if (!ISO_TIMESTAMP.test(node)) return node;
      const ms = Date.parse(node);
      if (!Number.isFinite(ms)) return node;
      const real = unshiftMs(ms, shifts);
      return real === ms ? node : new Date(real).toISOString();
    }
    if (Array.isArray(node)) return node.map(visit);
    if (node && typeof node === "object") return Object.fromEntries(Object.entries(node).map(([key, child]) => [key, visit(child)]));
    return node;
  };
  return visit(value) as T;
}

/**
 * The board API as the grader should read it in a wave with aged rows: run and
 * event times come back at their real times. Writes are unchanged.
 */
export class TimeShiftedApi extends ChaosApi {
  constructor(baseUrl: string, readonly shifts: AgingShift[]) { super(baseUrl); }

  override get<T>(route: string, timeoutMs?: number): Promise<T> {
    return super.get<T>(route, timeoutMs).then(value => unshiftJson(value, this.shifts));
  }
}
