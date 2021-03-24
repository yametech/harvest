use super::{new_arc_rwlock, Pod};
use async_std::task;
use crossbeam_channel::{unbounded, Sender};
use event::{Dispatch, Listener};
use std::sync::RwLock;
use std::{collections::HashMap, sync::Arc};
use strum::AsRefStr;

#[derive(AsRefStr, Debug, Clone)]
pub enum Event {
    #[strum(serialize = "insert")]
    Insert,
    #[strum(serialize = "update")]
    Update,
    #[strum(serialize = "delete")]
    Delete,
    #[strum(serialize = "offset")]
    IncrOffset,
    #[strum(serialize = "close")]
    Close,
}

unsafe impl Sync for Event {}
unsafe impl Send for Event {}

pub(crate) type UUID = String;

#[derive(Debug, Clone)]
pub struct Message {
    pub event: Event,
    pub pod: Pod,
}

#[derive(AsRefStr, Debug, Clone)]
enum ListenerEvent {
    #[strum(serialize = "open")]
    Open,
    #[strum(serialize = "close")]
    Close,
}

pub struct MemDatabaseEventDispatcher {
    dispatchers: Dispatch<Pod>,
}

impl MemDatabaseEventDispatcher {
    pub(crate) fn new() -> Self {
        Self {
            dispatchers: Dispatch::<Pod>::new(),
        }
    }

    pub(crate) fn registry_open_event_listener<L>(&mut self, l: L)
    where
        L: Listener<Pod> + Send + Sync + 'static,
    {
        self.dispatchers.registry(ListenerEvent::Open.as_ref(), l)
    }

    pub(crate) fn registry_close_event_listener<L>(&mut self, l: L)
    where
        L: Listener<Pod> + Send + Sync + 'static,
    {
        self.dispatchers.registry(ListenerEvent::Close.as_ref(), l)
    }

    pub(crate) fn dispatch_open_event(&mut self, pod: &Pod) {
        self.dispatchers.dispatch(ListenerEvent::Open.as_ref(), pod)
    }

    pub(crate) fn dispatch_close_event(&mut self, pod: &Pod) {
        self.dispatchers
            .dispatch(ListenerEvent::Close.as_ref(), pod)
    }
}

pub struct MemDatabase {
    // pod key is the pod path uuid
    pub(crate) pods: Arc<RwLock<HashMap<UUID, Pod>>>,
    // internal event send queue
    pub(crate) tx: Sender<Message>,
    // db event dispatchers
    pub(crate) dispatchers: Arc<RwLock<MemDatabaseEventDispatcher>>,
}

impl MemDatabase {
    pub fn new(dispatchers: Arc<RwLock<MemDatabaseEventDispatcher>>) -> Self {
        let (tx, rx) = unbounded::<Message>();
        let hm = new_arc_rwlock(HashMap::<UUID, Pod>::new());
        let t_hm = Arc::clone(&hm);
        let t_dispatchers = Arc::clone(&dispatchers);
        task::spawn(async move {
            while let Ok(msg) = rx.recv() {
                let evt = msg.event;
                let pod = msg.pod;

                let mut m = match t_hm.write() {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!("MemDatabase thread write hm failed, error:{:?}", e);
                        continue;
                    }
                };

                match evt {
                    Event::Update => {
                        m.entry(pod.path.to_string())
                            .or_insert(pod.clone())
                            .merge_with(&pod);
                        match pod.state {
                            crate::State::Running => {
                                match t_dispatchers.write() {
                                    Ok(mut dispatch) => dispatch.dispatch_open_event(&pod),
                                    Err(e) => {
                                        eprintln!("MemDatabase thread dispath open event failed, error:{:?}",e)
                                    }
                                }
                            }
                            crate::State::Stopped => {
                                match t_dispatchers.write() {
                                    Ok(mut dispatch) => dispatch.dispatch_close_event(&pod),
                                    Err(e) => {
                                        eprintln!("MemDatabase thread dispath close event failed, error:{:?}",e)
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    Event::Delete => {
                        if pod.ns != "" && pod.path == "" && pod.pod_name != "" {
                            m.retain(|_, inner| !inner.compare_ns_pod(&pod));
                            continue;
                        }
                        m.remove(&pod.path);
                    }
                    Event::IncrOffset => {
                        if let Some(inner) = m.get_mut(&pod.path) {
                            inner.last_offset = pod.last_offset;
                            inner.offset += pod.last_offset
                        };
                    }

                    Event::Close => {
                        break;
                    }
                    Event::Insert => {
                        m.insert(pod.path.clone(), pod);
                    }
                };
            }
        });

        Self {
            pods: hm,
            tx,
            dispatchers,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Event;
    #[test]
    fn event_it_works() {
        assert_eq!(Event::Insert.as_ref(), "insert");
        assert_eq!(Event::Delete.as_ref(), "delete");
    }
}
