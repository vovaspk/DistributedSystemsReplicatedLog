# Distributed Systems — Replicated Log

Replicated Log for the Distributed Systems course (UCU, 2026–2027).

**Current state: Iteration 2** — one Master and two Secondaries with an in-memory log, parallel replication
with a tunable **write concern** `w` (the master answers after `w-1` secondary ACKs), **total order** of
messages, and a test harness that can delay and reorder messages.

## Tech stack
- Java 25, Spring Boot 4.0.8 (Spring Web, Actuator), Gradle 9 (wrapper included)
- JUnit 5 for tests
- Docker + Docker Compose to run everything

## Requirements
Only **Docker** (Docker Desktop, or Docker Engine + the Compose plugin). The Docker images build the app
themselves, so no local JDK or Gradle is needed to run it. The tests can also run inside a container
(see below); a local JDK 25 is only needed if you prefer running Gradle directly.

## Project structure
```
core/     plain-Java library: MasterNode, SecondaryNode, Message, Transport (no Spring)
          + test harness (core/src/test/.../harness): MockedTransport, LinkRules, TestCluster, Timing
master/   Spring Boot app: HTTP API + HttpTransport (real transport) around MasterNode
sec/      Spring Boot app: HTTP API around SecondaryNode
docker-compose.yml   1 master + 2 secondaries
```

## Run
```bash
docker compose up --build
```
The first build takes about a minute (it downloads the JDK images and Gradle dependencies); later runs are cached. Wait until all three services log `Started ...Application`.

Services and ports on your machine:

| Service | URL | Artificial replication delay |
|---|---|---|
| master | http://localhost:8080 | – |
| sec1 | http://localhost:8081 | 0 ms |
| sec2 | http://localhost:8082 | 7000 ms |

The delay is configured in `docker-compose.yml` (`SEC_DELAY_MS`) and is what makes the replication lag
and the write concern visible. Stop with `Ctrl+C`, then `docker compose down`.

## API
| Service | Method | Path | Description |
|---|---|---|---|
| master | `POST` | `/messages?w=N` | Appends the message (request body = plain text), replicates it to all secondaries and returns `201` once the write concern `w` is met (see below). `w` defaults to master + all secondaries; an out-of-range `w` gives `400` |
| master | `GET` | `/messages` | Lists all messages |
| secondary | `GET` | `/messages` | Lists the replicated messages, in master order and without gaps |
| all | `GET` | `/actuator/health` | Health check |

Secondaries also have an internal `POST /replicate` used by the master (not part of the public API; its
`200` response is the ACK).

## Write concern and ordering
`w` says how many nodes must have the message before the client gets its answer: `w=1` – master only,
`w=2` – master + one secondary, `w=3` – master + two secondaries. The master always replicates to **all**
secondaries in parallel; `w` only decides how many ACKs it waits for, the rest finish in the background.
Clients with different `w` do not block each other.

Total order: the master gives every message a sequence number (`seq`). A secondary shows messages only as a
gap-free prefix by `seq`: if `msg4` arrives before `msg3` it is buffered and stays hidden until `msg3` arrives.
So a lagging secondary returns a shorter list than the master (eventual consistency), never a different order.

## Try it
```bash
# w=1: returns immediately, sec2 (7 s delay) catches up later
curl -i -X POST -H 'Content-Type: text/plain' -d 'Msg1' 'localhost:8080/messages?w=1'
# w=2: waits for sec1 (no delay)
curl -i -X POST -H 'Content-Type: text/plain' -d 'Msg2' 'localhost:8080/messages?w=2'
# w=3: blocks ~7 s for sec2; in another terminal send Msg4 with w=1 - it returns at once
curl -i -X POST -H 'Content-Type: text/plain' -d 'Msg3' 'localhost:8080/messages?w=3'
curl -i -X POST -H 'Content-Type: text/plain' -d 'Msg4' 'localhost:8080/messages?w=1'

curl localhost:8080/messages   # ["Msg1","Msg2","Msg3","Msg4"]
curl localhost:8081/messages   # same as master
curl localhost:8082/messages   # shorter (a prefix) until the delay has passed
curl -i -X POST -d x 'localhost:8080/messages?w=4'   # 400: w must be between 1 and 3
```
Watch the logs (`docker compose logs -f`): the master sends to both secondaries at the same time and finishes
the request as soon as the required ACKs are in.

## Tests
The test harness replaces the network with an in-process `MockedTransport` that can delay messages per
direction, so the scenarios run in seconds without Docker networking. The assignment's delays (5 s / 6 s)
are scaled down by 10 (`TestTime.SCALE`).

Iteration 1 scenarios (`core/src/test/.../Iteration1ReplicationTest`):
1. delay on S1 → `append` blocks until S1 ACKs;
2. delays 5 s and 6 s → `append` takes ≈ 6 s, not 11 s (replication is parallel);
3. the full assignment scenario: `m1` blocks 5 s, `m2` blocks 6 s, delays removed → `m3` does not block, and master, S1, S2 list `m1, m2, m3`.

Iteration 2 scenarios (`core/src/test/.../Iteration2WriteConcernTest`):
1. the assignment's acceptance test: `Msg1` (w=1), `Msg2` (w=2) return at once, `Msg3` (w=3) waits for the slow S2,
   `Msg4` (w=1) is not blocked by it; meanwhile S2 shows only a prefix of the log, and later catches up;
2. `w=1` never waits, `w=2` waits for the fastest secondary, `w=3` for the slowest; invalid `w` is rejected;
3. `transport.reorderMessages(master, s1, 2)` delivers `m2` before `m1`, yet S1 lists `m1, m2`;
4. `SecondaryNode` hides a message until the gap before it is filled and ignores duplicates;
5. 30 concurrent clients with random `w` end with identical logs on master, S1 and S2;
6. reordering also works with `w=2`, and a reorder group that never fills up is flushed after a timeout;
7. a write concern that can no longer be satisfied (a secondary is unreachable) fails instead of hanging.

Run with Docker only (nothing else installed; the build fails if a test fails):
```bash
docker build -f Dockerfile.test --no-cache --progress=plain .
```
Or, with a local JDK 25:
```bash
./gradlew test
```

## Configuration
| Property / env var | Service | Meaning |
|---|---|---|
| `replication.secondaries` / `REPLICATION_SECONDARIES` | master | Comma-separated secondary base URLs |
| `sec.node-id` / `SEC_NODE_ID` | secondary | Name used in log lines (default `secondary`) |
| `sec.delay-ms` / `SEC_DELAY_MS` | secondary | Sleep before storing a replicated message (default 0) |

## Notes
- Iterations 1–2 assume a perfect link: no retries, timeouts or failure handling yet (Iteration 3).
- Each iteration lives on its own branch.
