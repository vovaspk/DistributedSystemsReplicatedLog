# Distributed Systems — Replicated Log

Replicated Log for the Distributed Systems course (UCU, 2026–2027).

**Current state: Iteration 1** — one Master and two Secondaries with an in-memory log, blocking and
parallel replication (the master answers the client only after **all** secondaries ACK), and a test harness.

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
Services and ports on your machine:

| Service | URL | Artificial replication delay |
|---|---|---|
| master | http://localhost:8080 | – |
| sec1 | http://localhost:8081 | 2000 ms |
| sec2 | http://localhost:8082 | 5000 ms |

The delay is configured in `docker-compose.yml` (`SEC_DELAY_MS`) and is what makes the blocking replication
visible. Stop with `Ctrl+C`, then `docker compose down`.

## API
| Service | Method | Path | Description |
|---|---|---|---|
| master | `POST` | `/messages` | Appends the message (request body = plain text), replicates it to all secondaries, returns `201` only after all ACKs |
| master | `GET` | `/messages` | Lists all messages |
| secondary | `GET` | `/messages` | Lists all replicated messages |
| all | `GET` | `/actuator/health` | Health check |

Secondaries also have an internal `POST /replicate` used by the master (not part of the public API; its
`200` response is the ACK).

## Try it
```bash
# blocks ~5 s (the slowest secondary); replication to sec1 and sec2 runs in parallel
curl -i -X POST -H 'Content-Type: text/plain' -d 'hello' localhost:8080/messages

curl localhost:8080/messages   # ["hello"]
curl localhost:8081/messages   # ["hello"]
curl localhost:8082/messages   # ["hello"]
```
Watch the logs (`docker compose logs -f`): the master sends to both secondaries at the same time, receives the
two ACKs at different times, and only then finishes the request.

## Tests
The test harness replaces the network with an in-process `MockedTransport` that can delay messages per
direction, so the scenarios run in seconds without Docker networking. The assignment's delays (5 s / 6 s)
are scaled down by 10 (`TestTime.SCALE`).

Iteration 1 scenarios (`core/src/test/.../Iteration1ReplicationTest`):
1. delay on S1 → `append` blocks until S1 ACKs;
2. delays 5 s and 6 s → `append` takes ≈ 6 s, not 11 s (replication is parallel);
3. delays removed → `append` does not block and master, S1, S2 list `m1, m2, m3`.

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
| `sec.delay-ms` / `SEC_DELAY_MS` | secondary | Sleep before storing a replicated message (default 0) |

## Notes
- Iteration 1 assumes a perfect link: no retries, timeouts or failure handling yet.
- Each iteration lives on its own branch.
