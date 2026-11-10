import { describe, expect, it } from "vitest";
import { parseCommand } from "./config.js";
import { planTasks } from "./scheduler.js";
import { TIME_FREEZE_HOLD_RANGE_MS } from "./time-faults-config.js";
import { ProcessFreezer, agingStatements, descendantPids, parseProcessTable, unshiftJson, unshiftMs } from "./time-faults.js";

const HOUR = 3_600_000;

describe("time fault options", () => {
  it("accepts TM faults locally with a default 26 h age", () => {
    const parsed = parseCommand(["--faults", "TM1,TM2,TM3", "--adapters", "codex", "--workloads", "A1", "--seed", "1"]);
    if (parsed.mode !== "wave") throw new Error("expected a wave");
    expect(parsed.options.faults).toEqual(["TM1", "TM2", "TM3"]);
    expect(parsed.options.timeAgeMs).toBe(26 * HOUR);
  });

  it("takes --time-age-hours only with an aging fault", () => {
    const parsed = parseCommand(["--faults", "TM3", "--time-age-hours", "6", "--workloads", "A1", "--seed", "1"]);
    expect(parsed.mode === "wave" && parsed.options.timeAgeMs).toBe(6 * HOUR);
    expect(() => parseCommand(["--faults", "TM1", "--time-age-hours", "6", "--workloads", "A1"])).toThrow(/add TM2 or TM3/);
    expect(() => parseCommand(["--faults", "K1", "--time-age-hours", "6", "--workloads", "A1"])).toThrow(/add TM2 or TM3/);
    expect(() => parseCommand(["--faults", "TM3", "--time-age-hours", "0", "--workloads", "A1"])).toThrow(/between 1 and 72/);
    expect(() => parseCommand(["--faults", "TM3", "--time-age-hours", "100", "--workloads", "A1"])).toThrow(/between 1 and 72/);
  });

  it("refuses combinations a freeze or an age cannot serve", () => {
    expect(() => parseCommand(["--faults", "TM1", "--storm-minutes", "10"])).toThrow(/storm/);
    expect(() => parseCommand(["--faults", "TM2", "--env", "daytona", "--daytona-image", `r/i@sha256:${"a".repeat(64)}`])).toThrow(/--env local/);
    expect(() => parseCommand(["--faults", "TM3,K14", "--upgrade-from", "/tmp/x"])).toThrow(/K14/);
  });

  it("gives TM1 and TM2 a seeded freeze and TM3 none", () => {
    const plan = planTasks({ seed: 7, tasks: 9, workloads: ["A1"], adapters: ["codex"], faults: ["TM1", "TM2", "TM3"] });
    for (const task of plan) {
      if (task.fault === "TM3") expect(task.holdMs).toBe(0);
      else {
        const [min, max] = TIME_FREEZE_HOLD_RANGE_MS[task.fault as "TM1" | "TM2"];
        expect(task.holdMs).toBeGreaterThanOrEqual(min);
        expect(task.holdMs).toBeLessThanOrEqual(max);
      }
    }
    // TM1 outlasts the 90 s controller-instance lease.
    expect(TIME_FREEZE_HOLD_RANGE_MS.TM1[0]).toBeGreaterThan(90_000);
  });
});

describe("process freezing", () => {
  const table = parseProcessTable([
    "  100     1 node server-entry.ts",
    "  200   100 paperclip-runnerd --serve",
    "  300   200 codex app-server",
    "  400   100 /opt/pg/bin/postgres -D data",
    "  500   400 postgres: checkpointer",
    "  600     1 node other-session main.ts",
  ].join("\n"));

  it("walks the server's tree, parents first, and never freezes Postgres", () => {
    expect(descendantPids(100, table)).toEqual([200, 300]);
    expect(descendantPids(600, table)).toEqual([]);
  });

  it("freezes the root first and continues everything it froze", () => {
    const signals: string[] = [];
    const freezer = new ProcessFreezer((pid, signal) => { signals.push(`${signal}:${pid}`); }, () => table);
    expect(freezer.freeze(100, true)).toEqual([100, 200, 300]);
    freezer.thaw();
    expect(signals).toEqual(["SIGSTOP:100", "SIGSTOP:200", "SIGSTOP:300", "SIGCONT:100", "SIGCONT:200", "SIGCONT:300"]);
    expect(freezer.pids).toEqual([]);
  });

  it("TM1 freezes only the server", () => {
    const signals: string[] = [];
    const freezer = new ProcessFreezer((pid, signal) => { signals.push(`${signal}:${pid}`); }, () => table);
    expect(freezer.freeze(100, false)).toEqual([100]);
    freezer.thaw();
    expect(signals).toEqual(["SIGSTOP:100", "SIGCONT:100"]);
  });

  it("skips a process that exited before it could be frozen", () => {
    const freezer = new ProcessFreezer(pid => { if (pid === 200) throw Object.assign(new Error("gone"), { code: "ESRCH" }); }, () => table);
    expect(freezer.freeze(100, true)).toEqual([100, 300]);
  });
});

describe("aging", () => {
  it("shifts every timestamp column of a table in one statement", () => {
    const statements = agingStatements([
      { table: "heartbeat_runs", column: "created_at" }, { table: "heartbeat_runs", column: "controller_lease_expires_at" },
      { table: "agents", column: "updated_at" },
    ], 2 * HOUR);
    expect(statements.map(statement => statement.table)).toEqual(["agents", "heartbeat_runs"]);
    expect(statements[1]!.sql).toBe("update public.\"heartbeat_runs\" set \"created_at\" = \"created_at\" - interval '7200000 milliseconds', "
      + "\"controller_lease_expires_at\" = \"controller_lease_expires_at\" - interval '7200000 milliseconds' "
      + "where \"created_at\" is not null or \"controller_lease_expires_at\" is not null");
    expect(() => agingStatements([], 0)).toThrow(/positive/);
  });

  it("quotes identifiers", () => {
    expect(agingStatements([{ table: "we\"ird", column: "at" }], 1)[0]!.sql).toContain("public.\"we\"\"ird\"");
  });
});

describe("reading aged times at their real time", () => {
  const applied = Date.parse("2026-11-09T12:00:00.000Z");
  const shift = { faultId: "f01", ageMs: 26 * HOUR, appliedAt: applied };

  it("restores values the shift moved and leaves later writes alone", () => {
    const writtenBefore = Date.parse("2026-11-09T11:55:00.000Z");
    expect(unshiftMs(writtenBefore - shift.ageMs, [shift])).toBe(writtenBefore);
    const writtenAfter = applied + 30_000;
    expect(unshiftMs(writtenAfter, [shift])).toBe(writtenAfter);
  });

  it("undoes two shifts in order", () => {
    const second = { faultId: "f02", ageMs: 26 * HOUR, appliedAt: applied + 10 * 60_000 };
    const beforeBoth = applied - 60_000;
    const between = applied + 5 * 60_000;
    expect(unshiftMs(beforeBoth - shift.ageMs - second.ageMs, [shift, second])).toBe(beforeBoth);
    expect(unshiftMs(between - second.ageMs, [shift, second])).toBe(between);
  });

  it("rewrites ISO strings anywhere in a response and nothing else", () => {
    const run = { id: "r1", status: "running", startedAt: new Date(applied - 60_000 - shift.ageMs).toISOString(),
      finishedAt: null, note: "2026-11-08", events: [{ createdAt: new Date(applied + 1_000).toISOString() }] };
    expect(unshiftJson(run, [shift])).toEqual({ ...run, startedAt: new Date(applied - 60_000).toISOString() });
    expect(unshiftJson(run, [])).toBe(run);
  });
});
