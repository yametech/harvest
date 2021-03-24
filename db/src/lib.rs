#[macro_use]
extern crate lazy_static;
mod database;

mod pod;
use database::Message;
use event::Listener;
pub use pod::{GetPod, Pod, PodList, PodListMarshaller, State};

pub use common::new_arc_rwlock;
pub use database::Event;
pub(crate) use database::{MemDatabase, MemDatabaseEventDispatcher};

lazy_static! {
    static ref MEM: MemDatabase = {
        let m = MemDatabase::new(new_arc_rwlock(MemDatabaseEventDispatcher::new()));
        m
    };
}

pub fn incr_offset(uuid: &str, offset: i64) {
    MEM.tx
        .send(Message {
            event: Event::IncrOffset,
            pod: Pod {
                path: uuid.to_string(),
                last_offset: offset,
                ..Default::default()
            },
        })
        .unwrap()
}

pub fn update(pod: &Pod) {
    MEM.tx
        .send(Message {
            event: Event::Update,
            pod: pod.clone(),
        })
        .unwrap();
}

pub fn insert(pod: &Pod) {
    MEM.tx
        .send(Message {
            event: Event::Insert,
            pod: pod.clone(),
        })
        .unwrap();
}

pub fn delete(uuid: &str) {
    MEM.tx
        .send(Message {
            event: Event::Delete,
            pod: Pod {
                path: uuid.to_string(),
                ..Default::default()
            },
        })
        .unwrap();
}

pub fn all_to_json() -> PodListMarshaller {
    PodListMarshaller(
        MEM.pods
            .read()
            .unwrap()
            .iter()
            .map(|(_, v)| v.clone())
            .collect::<Vec<Pod>>(),
    )
}

pub fn close() {
    MEM.tx
        .send(Message {
            event: Event::Close,
            pod: Pod {
                ..Default::default()
            },
        })
        .unwrap();
}

pub fn get(uuid: &str) -> Option<Pod> {
    match MEM.pods.read() {
        Ok(pods) => match pods.get(uuid) {
            Some(pod) => Some(pod.clone()),
            None => None,
        },
        Err(_) => None,
    }
}

pub fn get_slice_with_ns_pod(ns: &str, pod: &str) -> Vec<(String, Pod)> {
    MEM.pods
        .read()
        .unwrap()
        .iter()
        .filter(|(_, v)| v.ns == ns && v.pod_name == pod)
        .map(|(uuid, pod)| (uuid.clone(), pod.clone()))
        .collect::<Vec<(String, Pod)>>()
}

pub fn delete_with_ns_pod(pod_ns: &str, pod_name: &str) {
    MEM.tx
        .send(Message {
            event: Event::Delete,
            pod: Pod {
                ns: pod_ns.to_string(),
                pod_name: pod_name.to_string(),
                ..Default::default()
            },
        })
        .unwrap();
}

pub fn pod_upload_stop(ns: &str, pod_name: &str) {
    let res = get_slice_with_ns_pod(ns, pod_name);
    for (_, mut pod) in res {
        if pod.is_stop() {
            continue;
        }
        pod.un_upload();
        pod.set_state_stop();
        update(&pod);
    }
}

pub fn pod_upload_start(ns: &str, pod_name: &str) {
    let res = get_slice_with_ns_pod(ns, pod_name);
    for (_, mut pod) in res {
        if pod.is_upload() && pod.is_running() {
            continue;
        }
        pod.upload();
        pod.set_state_run();
        update(&pod);
    }
}

pub fn registry_open_event_listener<L>(l: L)
where
    L: Listener<Pod> + Send + Sync + 'static,
{
    match MEM.dispatchers.write() {
        Ok(mut dispatcher) => dispatcher.registry_open_event_listener(l),
        Err(e) => {
            eprintln!("{:?}", e)
        }
    }
}

pub fn registry_close_event_listener<L>(l: L)
where
    L: Listener<Pod> + Send + Sync + 'static,
{
    match MEM.dispatchers.write() {
        Ok(mut dispatcher) => dispatcher.registry_close_event_listener(l),
        Err(e) => {
            eprintln!("{:?}", e)
        }
    }
}
