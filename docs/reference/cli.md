# Command line

`postino` is the server, the MCP adapter, and a client in one binary. Client commands find the server through `POSTINO_URL`, then `~/.postino/broker.json`, then `127.0.0.1:2790`, and start a local server if none answers. Commands that act as an agent take `--from` or `--as`, or `POSTINO_AGENT_NAME`. Without any of them they act as `operator`.

## Server

```text
postino serve [--host H] [--port P] [--data-dir DIR | --memory]
              [--fsync always|everysec|never] [--idle-timeout SECS]
              [--local-token]
```

Runs the server in the foreground. Options override the [environment](configuration.md). An explicit `--port` must be free; otherwise the next free port after 2790 is used and recorded in `broker.json`. `--port 0` lets the system pick a free port, which the server prints and records the same way. `--local-token` protects a loopback server with the administrator token in `<data-dir>/token`, as servers started on demand are; the clients find and send it.

```text
postino token
```

Prints a new random token (64 hexadecimal characters) for `POSTINO_API_TOKEN` or `POSTINO_AGENT_TOKENS`.

```text
postino service systemd|launchd
```

Prints a service definition for this binary. See [Deployment](../guides/deployment.md).

```text
postino repair [--data-dir DIR]
```

Cuts a damaged journal at its first bad entry, so that the server starts from the changes made before it. Run it when the server refuses to start over a corrupt journal entry, with the server stopped. Nothing is deleted: the rest of the damaged file is set aside next to it as `<name>.corrupt`, and so is every journal written after it, since its changes were made on top of the ones lost. See [Persistence](../getting-started/concepts.md#persistence).

## Database

```text
postino db <command> [arg ...]
postino db < script.txt
```

Runs one command and prints the reply, or runs one command per line from standard input. Quote arguments with spaces; lines starting with `#` are skipped. Run interactively, it shows a `postino>` prompt.

```sh
postino db HSET task:42 state open owner builder
postino db HGETALL task:42
printf 'INCR builds\nLPUSH queue job-1\n' | postino db
```

```text
postino publish <channel> <text|->
postino subscribe <channel>... [--pattern P]...
postino tail <stream> [--from ID]
```

`publish` prints how many subscribers received the message. `subscribe` prints `channel  message` lines until interrupted. `tail` prints `id  field=value ...` lines as entries are added; `--from 0` starts at the beginning, the default `$` with new entries only.

## Messages

```text
postino send <to> <text|-> [--from NAME] [--reply-to ID] [--key KEY] [--strict]
postino broadcast <text|-> [--from NAME] [--key KEY]
postino read <agent> [--limit N] [--wait SECS] [--ack]
postino ack <agent> <receipt>...
```

`-` (or no text) reads the text from standard input. `read --ack` acknowledges what it read at once.

## Rooms

```text
postino rooms
postino room show <room>
postino room create <room> [--topic T] [--private] [--members a,b]
postino room join|leave <room>
postino room post <room> <text|-> [--reply-to ID] [--about REF]
postino room read <room> [--wait SECS] [--limit N]
postino room ask <room> <text|-> [--to a,b] [--options x,y] [--about REF] [--wait SECS]
postino room answer <room> <question-id> [text|-] [--choice C]
postino room board <room> [key [value|-]] [--version N] [--delete]
postino room close|reopen|delete <room>
```

Every room command accepts `--as NAME`. `ask` without `--wait` prints the question id; with it, the settled question.

## Agents and the server

```text
postino agents                    List agents with presence and queue counts
postino cleanup                   Forget offline agents with nothing pending (administrator)
postino status                    Server address, version, and totals
postino watch [--agent NAME]      Print forum events as they happen
postino config [--agent NAME]     Print an MCP server entry for this binary
postino mcp [--agent NAME]        Run the MCP adapter on stdio
postino version
```

Run without arguments in a terminal, `postino` prints help. With standard input connected to a program, it runs the MCP adapter, so a client configured with just the binary path works.
