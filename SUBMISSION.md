# Submission — Replicated Log, Iteration 2

- **Repository:** https://github.com/vovaspk/DistributedSystemsReplicatedLog
- **Branch:** `iteration-2`
- **Full documentation:** see [README.md](README.md)

## Requirements
Only Docker (Docker Desktop, or Docker Engine + the Compose plugin). No local JDK or Gradle is needed.

## Run the tests
```bash
git clone -b iteration-2 https://github.com/vovaspk/DistributedSystemsReplicatedLog
cd DistributedSystemsReplicatedLog
docker build -f Dockerfile.test --no-cache --progress=plain .
```
The build fails if any test fails. It includes the test-harness scenario from the assignment
(`core/src/test/.../Iteration1ReplicationTest`).

## Run the system
```bash
docker compose up --build
```
The first build takes about a minute. Services: master `localhost:8080`, sec1 `localhost:8081` (no delay),
sec2 `localhost:8082` (7 s delay).

## Try it
```bash
curl -i -X POST -H 'Content-Type: text/plain' -d 'Msg1' 'localhost:8080/messages?w=1'   # immediate
curl -i -X POST -H 'Content-Type: text/plain' -d 'Msg2' 'localhost:8080/messages?w=2'   # waits for sec1
curl -i -X POST -H 'Content-Type: text/plain' -d 'Msg3' 'localhost:8080/messages?w=3'   # blocks ~7 s (sec2)
curl -i -X POST -H 'Content-Type: text/plain' -d 'Msg4' 'localhost:8080/messages?w=1'   # not blocked (run in another terminal)

curl localhost:8080/messages   # all four, in order
curl localhost:8082/messages   # a prefix of them until sec2's delay has passed
```
Logs: `docker compose logs -f`. Stop with `docker compose down`.
