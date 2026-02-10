# Blobcache

[![GoDoc](https://godoc.org/blobcache.io/blobcache?status.svg)](http://godoc.org/blobcache.io/blobcache)
[<img src="https://discord.com/assets/cb48d2a8d4991281d7a6a95d2f58195e.svg" width="80">](https://discord.gg/TWy6aVWJ7f)

Blobcache reimagines application state as Volumes holding hash-linked data structures.
Volumes can be persisted locally, encrypted, and accessed over the network.
Hash-linked data structures are efficient to sync and transfer.
Corruption is always detected, and the root is a cryptographic commitment to the whole data structure.

Blobcache is a universal backend for E2EE applications.

## Quick Docs
- [Why Blobcache?](./doc/0.2_Why_Blobcache.md)
- [Concepts](./doc/1.0_Concepts.md)
- [BCP vs. HTTP](./doc/9.01_BCP_vs_HTTP.md)
- [Git Remote](./doc/6.01_Git_Remote.md)

## Getting Started

For non-user machines (cloud, homelab) it is recommended to run the docker image.
For user machines, it is recommended to run blobcache as a systemd service when you log in.

### Installation
There is an [install script](./etc/install-systemd.sh) for systemd based systems.
You can run that with `just install-systemd`.

The install script does 2 things:
- Installs the blobcache executable to `/usr/bin/blobcache`
- Copies [blobcache.service](./etc/blobcache.service) into `$HOME/.config/systemd/user/`

Then you can manage the service using `systemctl --user` as you would normally.
The service is not enabled by default (you can do that with `systemctl --user enable blobcache`), so this install method is also appropriate for getting the binary into `/usr/bin` without launching a background process.

### Setting `BLOBCACHE_API`
On user machines, you will probably want to set the `BLOBCACHE_API` environment variable.

This would set the variable to the unix socket used by default in the systemd service described above.
```shell
export BLOBCACHE_API="unix:///run/user/$(id -u)/blobcache.sock"
```

The environment variable is used by the `blobcache` command to talk to the blobcache daemon.

You can test that the CLI can connect to the daemon with
```shell
$ blobcache endpoint
JcJvfY9tFsfkHSwoMT8IEoSq1ZfxVYBAwpBRvJ0uUJA:[::]:6025
```

### Docker
There is a docker image on the GitHub Container Registry, it can also be built with `just docker-build`.
Right now it only builds for `linux-amd64`.

After that you can test run with
```shell
docker run -it --rm ghcr.io/blobcache/blobcache:latest
```

Images are tagged with the git hash e.g. `git-a0b1c3d` and the version e.g. `v1.2.3`

The `/state` directory is where blobcache stores all of its state.
This is where you should mount a volume to persist data on the host.
You should also expose the peer port, so other instances can connect.

```shell
docker run \
    -v /host/path/to/state:/state \
    -p 6025:6025/udp \
    ghcr.io/blobcache/blobcache:latest
```

### Running a Node in Memory
This is a good option if you just want to play around with the API, and don't want to persist any data, or connect to peers.

```shell
$ blobcache daemon-ephemeral \
    --serve-unix ./blobcache.sock \
    --net 0.0.0.0:6025
```

### Running the daemon
The following command runs a daemon with state in the specified directory. 
```shell
$ blobcache daemon \
    --state $HOME/.local/blobcache \
    --serve-unix /run/blobcache/blobcache.sock \
    --net 0.0.0.0:6025
```

Once the daemon is running, you should be able to connect to it and start building your application on top of content-addressed storage.

## License
The Blobcache implementation is licensed under GPLv3.

All of the clients are licensed under MPL 2.0

What this means is: if you improve Blobcache you have to make the improvements available, but if you are just using a client you can do whatever you want with it.
