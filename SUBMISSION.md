# Submission — Replicated Log, Iteration 1

- **Repository:** https://github.com/vovaspk/DistributedSystemsReplicatedLog
- **Branch:** `iteration-1` (the repository's default branch)
- **Full documentation:** see [README.md](README.md)

## Requirements
Only Docker (Docker Desktop, or Docker Engine + the Compose plugin). No local JDK or Gradle is needed.

## Run the tests
```bash
git clone -b iteration-1 https://github.com/vovaspk/DistributedSystemsReplicatedLog
cd DistributedSystemsReplicatedLog
docker build -f Dockerfile.test --no-cache --progress=plain .
```
The build fails if any test fails. It includes the test-harness scenario from the assignment
(`core/src/test/.../Iteration1ReplicationTest`).

## Run the system
```bash
docker compose up --build
```
The first build takes about a minute. Services: master `localhost:8080`, sec1 `localhost:8081` (2 s delay),
sec2 `localhost:8082` (5 s delay).

## Try it
```bash
# blocks ~5 s (slowest secondary); replication to both secondaries runs in parallel
curl -i -X POST -H 'Content-Type: text/plain' -d 'hello' localhost:8080/messages

curl localhost:8080/messages   # ["hello"]
curl localhost:8081/messages   # ["hello"]
curl localhost:8082/messages   # ["hello"]
```
Logs: `docker compose logs -f`. Stop with `docker compose down`.
