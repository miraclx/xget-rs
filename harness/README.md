# Historical xget fault harness

This retained rig targeted the retired engine and command surface. Its scenario scripts and command
examples are not runnable against the modern CLI, are not current Client-path evidence, and are not a
supported suite to run from this checkout. Do not run the old scenarios against the modern executable.
No flag-compatibility mode is provided. The scripts and fixtures remain intact for historical reference.

The historical topology used three containers behind docker compose:

- **httpbin** ([go-httpbin](https://github.com/mccutchen/go-httpbin)): arbitrary bodies with range
  support (`/range/N`), a throttled trickle (`/drip`), and delays (`/delay/N`).
- **minio**: an S3-compatible store for the `s3://` source, seeded with a public object that carries a
  stored SHA-256 and a `.sha256sum` sidecar.
- **toxiproxy**: a fault-injecting TCP proxy in front of both, allowing historical scenarios to add
  bandwidth limits, latency, connection resets, or stalls.

## Retained entry points, not current instructions

The old entry points below are recorded only to explain the preserved files. They are not a current
build, execution or cleanup procedure:

```text
./up.sh          # historical startup and seeding entry point
./scenarios.sh   # historical scenario entry point; incompatible with the modern CLI
./down.sh        # historical teardown entry point
```

The retained `up.sh` would remove an ad-hoc `xget-minio` container to take port 9000. Its presence here
is not permission to remove an existing container or stop another service.

## Historical endpoints

| what | url |
| ---- | --- |
| httpbin, direct | `http://localhost:8080` (e.g. `/range/104857600`, `/drip`, `/delay/5`) |
| httpbin, via toxiproxy | `http://localhost:8666` |
| minio, direct | `http://localhost:9000` (bucket `demo`, object `obj.bin` + `obj.bin.sha256sum`) |
| minio, via toxiproxy | `http://localhost:8667` |
| toxiproxy control API | `http://localhost:8474` |

## Historical fault setup

The following preserved control requests describe how that rig injected faults. They mutate a running
proxy and are not setup or validation instructions for the modern CLI:

```text
# historical httpbin throttle to 2 MB/s downstream
curl -XPOST localhost:8474/proxies/httpbin/toxics \
  -d '{"type":"bandwidth","attributes":{"rate":2048},"stream":"downstream"}'

# historical connection reset after 800ms
curl -XPOST localhost:8474/proxies/httpbin/toxics \
  -d '{"type":"reset_peer","attributes":{"timeout":800},"stream":"downstream"}'

# historical complete data stall
curl -XPOST localhost:8474/proxies/httpbin/toxics \
  -d '{"type":"timeout","attributes":{"timeout":0},"stream":"downstream"}'

curl -XPOST localhost:8474/reset   # historical removal of all toxics
```

The old payload invocation below uses retired positional-output and flag syntax. It is preserved as
historical text only, not a modern command to copy:

```text
xget http://localhost:8666/range/104857600 out.bin -f -n 8
```
