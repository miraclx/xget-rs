use futures::stream;

use crate::{ByteRange, ByteStream, Error, Mirrors, Probe, Source};

/// A source that either answers with an empty stream or always errors, to exercise failover.
struct Mock {
    ok: bool,
}

impl Source for Mock {
    async fn probe(&self) -> Result<Probe, Error> {
        if self.ok {
            Ok(Probe {
                length: 10,
                supports_ranges: true,
                filename: None,
                content_type: None,
                checksum: None,
            })
        } else {
            Err(boom())
        }
    }

    async fn fetch(&self, _range: Option<ByteRange>) -> Result<ByteStream, Error> {
        if self.ok {
            Ok(Box::pin(stream::empty()))
        } else {
            Err(boom())
        }
    }
}

fn boom() -> Error {
    Error::Transport(Box::new(std::io::Error::other("boom")))
}

#[tokio::test]
async fn falls_over_to_a_working_mirror() {
    let mirrors = Mirrors::new(Mock { ok: false }, [Mock { ok: true }]);
    assert!(mirrors.probe().await.is_ok(), "the dead primary is skipped");
    assert!(mirrors.fetch(None).await.is_ok(), "fetch fails over too");
}

#[tokio::test]
async fn fails_when_every_mirror_is_dead() {
    let mirrors = Mirrors::new(Mock { ok: false }, [Mock { ok: false }]);
    assert!(mirrors.probe().await.is_err(), "no mirror can answer");
}

#[tokio::test]
async fn uses_the_primary_when_it_works() {
    let mirrors = Mirrors::new(Mock { ok: true }, [Mock { ok: false }]);
    assert!(mirrors.probe().await.is_ok());
}
