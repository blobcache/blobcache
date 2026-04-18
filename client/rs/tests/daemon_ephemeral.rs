use blobcache::{
    Client, DequeueOpts, GetOpts, HashAlgo, Message, PostOpts, QueueBackendMemory, QueueSpec,
    SchemaSpec, Service, TxParams, VolumeBackend, VolumeBackendLocal, VolumeSpec,
};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct DaemonHandle {
    child: Child,
}

impl Drop for DaemonHandle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn blobcache_bin() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..");
    root.join("build").join("out").join("blobcache")
}

fn unique_socket_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "blobcache-rs-test-{}-{nanos}.sock",
        std::process::id()
    ))
}

fn start_daemon(socket_path: &PathBuf) -> DaemonHandle {
    let child = Command::new(blobcache_bin())
        .arg("daemon-ephemeral")
        .arg("--serve-ipc")
        .arg(socket_path)
        .arg("--net")
        .arg("127.0.0.1:0")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to start blobcache daemon-ephemeral");
    DaemonHandle { child }
}

fn wait_for_service(socket_path: &PathBuf) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if socket_path.exists() {
            return;
        }
        if Instant::now() >= deadline {
            panic!("daemon did not become ready");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn local_volume_spec() -> VolumeSpec {
    VolumeBackend {
        local: Some(VolumeBackendLocal {
            schema: SchemaSpec {
                name: "".to_string(),
                params: None,
            },
            hash_algo: HashAlgo("blake3-256".to_string()),
            max_size: 1 << 20,
            salted: false,
        }),
        remote: None,
        peer: None,
        git: None,
        vault: None,
        consensus: None,
    }
}

fn wait_for_endpoint_command(api: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let output = Command::new(blobcache_bin())
            .arg("endpoint")
            .env("BLOBCACHE_API", api)
            .output()
            .expect("failed to run blobcache endpoint command");
        if output.status.success() {
            let stdout = String::from_utf8(output.stdout).expect("endpoint output should be utf8");
            let stdout = stdout.trim().to_string();
            if !stdout.is_empty() {
                return stdout;
            }
        }
        if Instant::now() >= deadline {
            panic!("endpoint command did not succeed in time");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn memory_queue_spec() -> QueueSpec {
    let mut spec = QueueSpec::default();
    spec.memory = Some(QueueBackendMemory {
        max_depth: 16,
        evict_oldest: false,
        max_bytes_per_message: 1024,
        max_handles_per_message: 8,
    });
    spec
}

#[test]
fn create_volume_and_transaction() {
    let socket_path = unique_socket_path();
    let _daemon = start_daemon(&socket_path);

    let endpoint = format!("unix://{}", socket_path.display());
    let client = Client::new(endpoint).expect("failed to create client");
    wait_for_service(&socket_path);

    let volume = client
        .create_volume(None, &local_volume_spec())
        .expect("failed to create volume");

    let tx = client
        .begin_tx(
            &volume,
            TxParams {
                modify: true,
                gc_blobs: false,
                gc_links: false,
            },
        )
        .expect("failed to begin tx");

    client
        .save(&tx, b"hello from rust")
        .expect("failed to save root");
    let loaded = client.load(&tx).expect("failed to load root");
    assert_eq!(loaded, b"hello from rust");

    let cid = client
        .post(&tx, b"blob-data", PostOpts { salt: None })
        .expect("failed to post blob");
    let mut buf = vec![0u8; 64];
    let n = client
        .get(
            &tx,
            cid,
            &mut buf,
            GetOpts {
                salt: None,
                skip_verify: false,
            },
        )
        .expect("failed to get blob");
    assert_eq!(&buf[..n], b"blob-data");

    client.commit(&tx).expect("failed to commit tx");

    let tx2 = client
        .begin_tx(
            &volume,
            TxParams {
                modify: false,
                gc_blobs: false,
                gc_links: false,
            },
        )
        .expect("failed to begin read tx");
    let loaded2 = client.load(&tx2).expect("failed to load committed root");
    assert_eq!(loaded2, b"hello from rust");

    client.abort(&tx2).expect("failed to abort read tx");

    let _ = std::fs::remove_file(socket_path);
}

#[test]
fn endpoint_command_works_against_ephemeral_daemon() {
    let socket_path = unique_socket_path();
    let _daemon = start_daemon(&socket_path);
    wait_for_service(&socket_path);

    let api = format!("unix://{}", socket_path.display());
    let endpoint = wait_for_endpoint_command(&api);
    assert!(endpoint.contains(':'));

    let _ = std::fs::remove_file(socket_path);
}

#[test]
fn enqueue_and_dequeue_messages() {
    let socket_path = unique_socket_path();
    let _daemon = start_daemon(&socket_path);

    let endpoint = format!("unix://{}", socket_path.display());
    let client = Client::new(endpoint).expect("failed to create client");
    wait_for_service(&socket_path);

    let queue = client
        .create_queue(None, &memory_queue_spec())
        .expect("failed to create queue");

    let messages = vec![
        Message {
            handles: vec![],
            bytes: b"m1".to_vec(),
        },
        Message {
            handles: vec![],
            bytes: b"m2".to_vec(),
        },
    ];

    let insert = client
        .enqueue(&queue, &messages)
        .expect("failed to enqueue messages");
    assert_eq!(insert.success, 2);

    let dequeued = client
        .dequeue(
            &queue,
            8,
            DequeueOpts {
                min: 0,
                leave_in: false,
                skip: 0,
                max_wait: Some(0),
            },
        )
        .expect("failed to dequeue messages");

    assert_eq!(dequeued.len(), 2);
    assert_eq!(dequeued[0].bytes, b"m1");
    assert_eq!(dequeued[1].bytes, b"m2");

    let _ = std::fs::remove_file(socket_path);
}
