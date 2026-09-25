# ClickHouse runtime labs

The `chlab_*` MCP tools use the local root service at `/run/aviary-chlab.sock`. The Aviary server does not need Docker socket access. Build the service as an ordinary user, then install the finished binary and unit as root:

```sh
build_dir=$(mktemp -d)
go build -o "$build_dir/aviary-chlab" ./cmd/aviary
sudo install -o root -g root -m 0755 "$build_dir/aviary-chlab" /usr/local/bin/aviary-chlab
sudo install -o root -g root -m 0644 deploy/aviary-chlab.service /etc/systemd/system/aviary-chlab.service
sudo systemctl daemon-reload
sudo systemctl enable --now aviary-chlab.service
```

The unit executes only the root-owned binary in `/usr/local/bin`; it never compiles or reads the user checkout at startup. The unit uses the local `ubuntu` group for socket access; set `Group=` to the Aviary server's group on another host. Rebuild and reinstall the binary before restarting the service for future updates.

The service discards labeled lab containers and networks when it starts. It limits each lab to a fixed shape, permits two labs, and expires them after 15 minutes idle or 60 minutes total. A failed image pull leaves an error status until the session calls `chlab_stop` or the lab expires.

Start with `chlab_start {version: "25.8", shape: "1x2"}` and poll `chlab_status` until it reports `ready`. The version can be any published official server image tag, including an exact version or a moving alias such as `latest`. The status reports the actual `SELECT version()` value and immutable image digest used for every node. Shapes mean one shard with one replica (`1x1`), one shard with two replicas (`1x2`), or two shards with two replicas each (`2x2`). Use `chlab_query` with `sql` and optional `node`, the `chlab_node_*` tools with a node name, and `chlab_stop` when done. A new start in the same Aviary session reuses its existing lab if the version and shape match; a Slack thread is one Aviary session.

For the ClickHouse expert agent, allow `chlab_start`, `chlab_status`, `chlab_query`, `chlab_stop`, and the three `chlab_node_*` tools. Keep `exec` restricted to the read-only `chsource` command. Agent rules should permit `chlab_start` only when a user explicitly requests runtime verification; ordinary source questions use `chsource` alone. The agent should label SQL results as observed in the reported `SELECT version()` build and image digest, and label source conclusions separately.

The service never publishes host ports or mounts host files. It creates a Docker bridge using `--internal` and `com.docker.network.bridge.gateway_mode_ipv4=isolated`, uses a read-only container filesystem with bounded tmpfs for ClickHouse data and logs, and runs all SQL through `docker exec` with fixed resource and output limits. The ClickHouse user profile prevents SQL from disabling the query time, memory, and row limits. Image pulls are the only network operation outside that private bridge. Docker explains the need for isolated gateway mode in its [network documentation](https://docs.docker.com/engine/network/port-publishing/#gateway-modes).
