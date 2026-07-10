# systemd Service Guide — Concepts, Operational Model & Lifecycle

**Scope:** how `fetch-agent-ibkr` (and future agents) run as systemd services on the
EC2 connectivity plane. Conceptual companion to the command-focused
[fetch_agent_runbook.md](fetch_agent_runbook.md) — read that for day-to-day commands;
read this to understand WHY the operational model works.
**Last updated:** 2026-07-05

---

## 1. What systemd is

systemd is Linux's **init system and service manager** — PID 1, the first process
the kernel starts at boot and the ancestor of every other process. Its job:

- start services in dependency order at boot
- keep them running (restart on crash, per policy)
- capture everything they print into the journal (structured, rotated logs)
- stop them cleanly at shutdown

Ubuntu already runs dozens of systemd services on the box — `sshd` (how you
connect), `docker`, `cron`. Registering `fetch-agent-ibkr` makes our agent one
more first-class citizen: supervised, auto-restarted, boot-started, logged.

**The unit file is the service's contract** (`agents/fetch_agent/fetch-agent-ibkr.service`
in the repo, installed to `/etc/systemd/system/`):

| Directive | Meaning for us |
|---|---|
| `ExecStart=` | the venv python + agent script |
| `User=ubuntu` | never runs as root |
| `Restart=always`, `RestartSec=15` | crash → restarted within 15s, forever |
| `After=network-online.target docker.service` | boot ordering: network + Docker first |
| `Environment=TA_VENDOR=ibkr` | vendor pinned at the service level |
| `WantedBy=multi-user.target` | included in normal boot (via `systemctl enable`) |

## 2. systemd vs SSH — who owns the process

The key mental shift:

- **Foreground (SSH-owned):** the agent is a child of your login session. Its
  lifetime is coupled to your terminal, your network connection, your TTY.
  This coupling is exactly what produced the 2026-07-04 incidents: Ctrl+C over
  a no-TTY ssh killed only the local client (orphaned agents), and closed
  laptops stranded processes.
- **systemd-owned:** the agent is a child of PID 1. It belongs to the MACHINE,
  not to any session. SSH becomes a pure management console — connect, issue
  `systemctl` commands, disconnect; the service never notices.

Rule: after systemd installation, NEVER start the agent by hand. `systemctl`
only. (The in-agent flock guard will refuse a manual duplicate anyway.)

## 3. Managing from the Mac

All management is one-liners over SSH (alias `ibkrbox`, runbook §1a):

```bash
ibkrbox "systemctl status fetch-agent-ibkr --no-pager"     # state, PID, uptime, recent log
ibkrbox "journalctl -u fetch-agent-ibkr -n 50 --no-pager"  # recent logs
ibkrbox "journalctl -u fetch-agent-ibkr -f"                # follow live (Ctrl+C stops WATCHING, not the service)
ibkrbox "sudo systemctl restart fetch-agent-ibkr"
ibkrbox "sudo systemctl stop fetch-agent-ibkr"             # queue freezes safely; nothing is lost
```

Two complementary monitoring layers — use both:
- **Process liveness:** journal heartbeat (hourly when idle) + `systemctl status`
- **Work liveness:** Databricks SQL on `control.fetch_request` (stuck PENDING =
  unhealthy work, even if the process looks fine)

## 4. Host process vs Docker container — why the box runs one of each

| | IB Gateway | Fetch agent |
|---|---|---|
| Model | Docker container (`gnzsnz/ib-gateway:stable`) | Plain host process under systemd |
| Why | Complex 3rd-party stack (Java GUI + Xvfb + IBC auto-login) shipped pre-assembled by its maintainer; container isolates it | ~250 lines of our own Python, two pip deps — a container would add build/registry machinery for zero benefit |
| Supervised by | Docker restart policy (`restart: always`); the Docker daemon itself is a systemd service, so the chain still roots at PID 1 | systemd directly |

They interact ONLY via `localhost:4004`. Gateway container restarts (nightly
IBKR maintenance, manual `docker compose restart`) need no coordination: the
agent detects the closed port, logs "gateway is down — waiting", and resumes
when it returns. Loose coupling by design.

## 5. Reboot behaviour — the recovery cascade (zero manual steps)

`systemctl enable` registers the service for boot. After ANY reboot:

```
kernel → systemd (PID 1)
  ├─ network up
  ├─ docker.service → restart:always → ibkr-gateway container
  │     └─ IBC auto-login to IBKR paper account          [~60–90s]
  └─ fetch-agent-ibkr.service
        └─ polls; gateway not ready yet → "waiting" → resumes when up
```

Fully operational ~2 minutes after power-on, unattended. Manifests that arrived
during the outage simply wait in `pending/`.

**Honest limits:** EC2 *hardware* failure is not self-healing — that's the
~30-minute manual rebuild (runbook §6.8), acceptable for a daily-batch pipeline
because the box holds no state. Revisit before Phase 5 live execution.

## 6. Logs & diagnosis

The journal captures all agent stdout/stderr, timestamped and auto-rotated:

```bash
ibkrbox "journalctl -u fetch-agent-ibkr --since '2 hours ago' --no-pager"
ibkrbox "journalctl -u fetch-agent-ibkr --since today --no-pager | grep -E 'FAILED|ERROR'"
```

Diagnosis flow: `systemctl status` (running? crash-looping? restart count) →
journal tail (what did it say?) → runbook §6 symptom table → queue/Delta state
(is work actually moving?).

## 7. Lifecycle & best practices

**Deploy (code change):**
```
edit in repo → commit/push → scp the one file → sudo systemctl restart → verify banner
```
- The restart is NEVER optional (a running Python doesn't reload its file).
- Never edit the file directly on the box — the repo is the single source of
  truth; box↔repo drift is how mystery bugs are born.

**Day-to-day:** nothing — that is the design goal. Planner emits on schedule,
agent drains, heartbeat proves liveness. Weekly glance: one SQL
(`SELECT status, COUNT(*) FROM control.fetch_request GROUP BY status`) and
optionally one `systemctl status`.

**Monitoring ladder (now → later):**
1. Now: journal heartbeat + stuck-PENDING SQL
2. Chunk 3+: Databricks monitor job alerts on stuck requests (email)
3. Before unattended nightly runs: CloudWatch alarm on instance health + SNS

**Recovery hierarchy (all automatic except the last):**
| Failure | Recovery | Time |
|---|---|---|
| Agent crash | systemd `Restart=always` | 15 s |
| Gateway container down | agent waits; Docker restarts container | ~1–2 min |
| EC2 reboot | full boot cascade (§5) | ~2 min |
| EC2 destroyed | manual rebuild, runbook §6.8 — zero data loss | ~30 min |

**The disaster principle:** the agent owns NO state. Every "what if X dies?"
reduces to "work waits in S3 until something comes back." That resilience comes
from the queue architecture; systemd just minimizes how long the wait is.

## 8. Adding future agents (publisher, enrichment, execution)

Same pattern per agent: unit file in the repo (`<agent>-<vendor>.service`),
own directory + venv on the box, own flock lock, own journal stream via
`journalctl -u <name>`. The execution agent (Phase 5) additionally gets its own
instance/container, Secrets Manager credentials, and stricter alerting — see
CLAUDE.md §14 two-plane roadmap.
