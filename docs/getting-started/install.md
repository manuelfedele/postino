# Installation

Postino is a single executable. Pick whichever way of getting it suits your machine.

## Prebuilt binaries

Releases ship static binaries for Linux (x86_64 and aarch64) and macOS (Apple silicon and Intel). Download the archive for your platform from the [releases page](https://github.com/manuelfedele/postino/releases), check it against its `.sha256` file, and put `postino` on your `PATH`:

```sh
tar -xzf postino-v0.1.0-aarch64-apple-darwin.tar.gz
sudo install postino-v0.1.0-aarch64-apple-darwin/postino /usr/local/bin/
postino version
```

## From source

Building needs the latest stable Rust toolchain.

```sh
cargo install --git https://github.com/manuelfedele/postino
```

The repository pins the toolchain in `rust-toolchain.toml`, so `rustup` picks the right compiler automatically when you build a checkout.

## Container image

The image is a static binary on an empty base, running as an unprivileged user with its data in `/data`. It listens on all interfaces, so it refuses to start without a token.

```sh
docker build -t postino https://github.com/manuelfedele/postino.git
docker run -d --name postino -p 2790:2790 -v postino:/data \
  -e POSTINO_API_TOKEN=$(postino token) postino
```

## Check that it runs

```sh
postino serve
```

```text
postino 0.1.0
  api      http://127.0.0.1:2790/api
  storage  /home/you/.postino
  auth     open (loopback only)
```

In another terminal:

```sh
curl http://127.0.0.1:2790/api/health
```

```json
{"instance":"5f1c9a2b7d3e4f60","ok":true,"service":"postino","version":"0.1.0"}
```

> [!TIP]
> On a workstation you rarely start the server yourself. `postino mcp` and every CLI command start a local server on demand and find it again through `~/.postino/broker.json`. That server exits after 30 idle minutes.

Next, take the [quickstart](quickstart.md).
