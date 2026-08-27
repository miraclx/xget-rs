# libxget

A chunked, parallel, resumable, verified fetch engine over a pluggable byte source.

> Early. A Rust rewrite of the NodeJS [`libxget`](https://github.com/miraclx/libxget-js), built so it
> can never hand back a corrupt file with a valid-looking hash.

![xget fetching a file in parallel chunks](media/download.gif)

## What it is

libxget plans a resource into chunks, fetches them in parallel, resumes a dropped chunk from its
offset, reassembles them in order, gates on the exact length, and streams the bytes through a hash so a
reported digest is trustworthy. A source that ignores a range, or a length that does not match, is a
typed error, never a silent bad file.

The byte source is pluggable behind the `Source` trait: HTTP range GET, and S3 (behind the `s3`
feature, for any S3-compatible store via `--endpoint-url`); a theia/bifrost peer (verified download by
public key) is next. The parallel-resume-verify engine is written once and works over any of them; a
source that cannot serve ranges is fetched as a single stream.

## Status

Scaffolding. The `Source` seam and the typed error surface are here; the engine (plan, parallel fetch
with range validation, ordered reassembly, length gate, hashing) and the HTTP adapter are next. See
`DECISIONS.md` for the design and the footguns it is built to avoid.

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT) at your option.
