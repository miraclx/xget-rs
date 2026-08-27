# xget test harness

A local rig for exercising xget against real network conditions, since a healthy localhost never
fails. Three containers behind docker compose:

- **httpbin** ([go-httpbin](https://github.com/mccutchen/go-httpbin)) — arbitrary bodies with range
  support (`/range/N`), a throttled trickle (`/drip`), and delays (`/delay/N`).
- **minio** — an S3-compatible store for the `s3://` source, seeded with a public object that carries a
  stored SHA-256 and a `.sha256sum` sidecar.
- **toxiproxy** — a fault-injecting TCP proxy in front of both, so a scenario can add bandwidth limits,
  latency, connection resets, or stalls and take them away again.

## Use

```sh
./up.sh          # start and seed everything, print the endpoints
./scenarios.sh   # build xget (release, s3) and run the suite
./down.sh        # stop everything
```

`up.sh` removes any ad-hoc `xget-minio` container, since it needs port 9000.

## Endpoints

| what | url |
| ---- | --- |
| httpbin, direct | `http://localhost:8080` (e.g. `/range/104857600`, `/drip`, `/delay/5`) |
| httpbin, via toxiproxy | `http://localhost:8666` |
| minio, direct | `http://localhost:9000` (bucket `demo`, object `obj.bin` + `obj.bin.sha256sum`) |
| minio, via toxiproxy | `http://localhost:8667` |
| toxiproxy control API | `http://localhost:8474` |

## Injecting faults by hand

Toxics are added over the control API and cleared with a reset:

```sh
# throttle httpbin to 2 MB/s downstream
curl -XPOST localhost:8474/proxies/httpbin/toxics \
  -d '{"type":"bandwidth","attributes":{"rate":2048},"stream":"downstream"}'

# reset connections 800ms in (forces retry/resume)
curl -XPOST localhost:8474/proxies/httpbin/toxics \
  -d '{"type":"reset_peer","attributes":{"timeout":800},"stream":"downstream"}'

# stall data entirely (forces --timeout)
curl -XPOST localhost:8474/proxies/httpbin/toxics \
  -d '{"type":"timeout","attributes":{"timeout":0},"stream":"downstream"}'

curl -XPOST localhost:8474/reset   # clear all toxics
```

Point xget at the toxiproxy port to feel them:

```sh
xget http://localhost:8666/range/104857600 out.bin -f -n 8
```
