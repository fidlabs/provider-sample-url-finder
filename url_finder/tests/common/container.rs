#![allow(dead_code)]

use std::sync::{Arc, LazyLock, Weak};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;
use tokio::sync::Mutex;

pub const POSTGRES_USER: &str = "postgres";
pub const POSTGRES_PASSWORD: &str = "postgres";

pub struct ContainerState {
    pub container: ContainerAsync<Postgres>,
    pub port: u16,
}

static CONTAINER: LazyLock<Mutex<Weak<ContainerState>>> = LazyLock::new(|| Mutex::new(Weak::new()));

pub async fn get_or_create_container() -> Arc<ContainerState> {
    let mut weak_lock = CONTAINER.lock().await;

    // Try to upgrade Weak to Arc (reuse existing container)
    if let Some(arc) = weak_lock.upgrade() {
        return arc;
    }

    // Weak failed to upgrade (Create new container)
    let container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("Failed to start Postgres container");

    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("Failed to get container port");

    let state = Arc::new(ContainerState { container, port });
    *weak_lock = Arc::downgrade(&state);

    state
}
