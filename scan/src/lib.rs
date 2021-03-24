use common::Result;
use db::Pod;
use event::{Dispatch, Listener};
use notify::{raw_watcher, RawEvent, RecursiveMode, Watcher};
use std::collections::{hash_map::DefaultHasher, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::mpsc::channel;
use std::sync::{Arc, RwLock};
use strum::AsRefStr;
use walkdir::WalkDir;
mod config_v2;
use config_v2::JSONConfig;

#[derive(Debug, AsRefStr, Clone)]
pub enum PathEvent {
    #[strum(serialize = "create")]
    Create,
    #[strum(serialize = "remove")]
    Remove,
    #[strum(serialize = "write")]
    Write,
}

pub trait GetPathEventInfo {
    fn get(&self) -> &PathEventInfo;
}

pub trait GetDebug {
    fn get_debug(&self) -> String;
}

#[derive(Debug, Clone)]
pub struct PathEventInfo {
    pub service_name: String,
    pub ns: String,
    pub pod_name: String,
    pub container_name: String,
    pub path: String,
    pub ips: Vec<String>,
}

impl Default for PathEventInfo {
    fn default() -> Self {
        Self {
            service_name: "".to_string(),
            ns: "".to_string(),
            pod_name: "".to_string(),
            container_name: "".to_string(),
            path: "".to_string(),
            ips: vec![],
        }
    }
}

impl GetPathEventInfo for PathEventInfo {
    fn get(&self) -> &PathEventInfo {
        self
    }
}

impl GetDebug for PathEventInfo {
    fn get_debug(&self) -> String {
        format!("{:?}", self).to_owned()
    }
}

impl PathEventInfo {
    pub fn to_pod(&self) -> Pod {
        Pod {
            service_name: self.service_name.clone(),
            ns: self.ns.clone(),
            pod_name: self.pod_name.clone(),
            container: self.container_name.clone(),
            path: self.path.clone(),
            ..Default::default()
        }
    }
}

unsafe impl Sync for PathEventInfo {}
unsafe impl Send for PathEventInfo {}

enum DockerConfigFileType {
    ConfigV2,
    Log,
    Unknow,
}

fn docker_config_file_type(path: &str) -> DockerConfigFileType {
    if path.ends_with(".log") {
        return DockerConfigFileType::Log;
    } else if path.ends_with("config.v2.json") {
        return DockerConfigFileType::ConfigV2;
    }
    DockerConfigFileType::Unknow
}

type Cache = Arc<Vec<RwLock<HashMap<String, Option<JSONConfig>>>>>;

pub struct AutoScanner {
    namespace: String,
    docker_dir: String,
    event_dispatch: Dispatch<PathEventInfo>,
    cache: Cache,
}

impl AutoScanner {
    pub fn new(namespace: String, docker_dir: String) -> Self {
        let len = 2;
        let mut cache: Vec<RwLock<HashMap<String, Option<JSONConfig>>>> = Vec::with_capacity(len);
        for _i in 0..len {
            cache.push(RwLock::new(HashMap::new()))
        }
        Self {
            namespace,
            docker_dir,
            event_dispatch: Dispatch::<PathEventInfo>::new(),
            cache: Arc::new(cache),
        }
    }

    fn hash(&self, k: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        k.to_owned().hash(&mut hasher);
        hasher.finish() as usize
    }

    fn remove(&self, k: &str) {
        let cache = self.cache.clone();
        let writer = cache[(self.hash(k) % cache.len()) as usize].write();
        match writer {
            Ok(mut w) => {
                w.remove(k);
            }
            Err(e) => {
                eprintln!("cache remove failed: {:?}", e)
            }
        }
    }

    fn insert(&self, k: &str, v: JSONConfig) {
        if self.namespace != v.get_ns() {
            return;
        }
        let cache = self.cache.clone();
        let writer = cache[(self.hash(k) % cache.len()) as usize].write();
        match writer {
            Ok(mut w) => {
                w.insert(k.into(), Some(v));
            }
            Err(e) => {
                eprintln!("cache insert failed: {:?}", e)
            }
        }
    }

    fn insert_key(&self, k: &str) {
        let cache = self.cache.clone();
        let writer = cache[(self.hash(k) % cache.len()) as usize].write();
        match writer {
            Ok(mut w) => {
                w.insert(k.into(), None);
            }
            Err(e) => {
                eprintln!("cache insert failed: {:?}", e)
            }
        }
    }

    fn get(&self, k: &str) -> Option<JSONConfig> {
        let cache = self.cache.clone();
        let reader = cache[(self.hash(k) % cache.len()) as usize].write();
        match reader {
            Ok(r) => match r.get(k) {
                Some(v) => v.clone(),
                None => None,
            },
            Err(e) => {
                eprintln!("cache get key {:?} failed: {:?}", k, e);
                None
            }
        }
    }

    pub fn append_close_event_handle<L>(&mut self, l: L)
    where
        L: Listener<PathEventInfo> + Send + Sync + 'static,
    {
        self.event_dispatch.registry(PathEvent::Remove.as_ref(), l)
    }

    pub fn append_write_event_handle<L>(&mut self, l: L)
    where
        L: Listener<PathEventInfo> + Send + Sync + 'static,
    {
        self.event_dispatch.registry(PathEvent::Write.as_ref(), l)
    }

    pub fn append_create_event_handle<L>(&mut self, l: L)
    where
        L: Listener<PathEventInfo> + Send + Sync + 'static,
    {
        self.event_dispatch.registry(PathEvent::Create.as_ref(), l)
    }

    fn dispatch_create_event(&mut self, pei: &PathEventInfo) {
        self.event_dispatch
            .dispatch(PathEvent::Create.as_ref(), pei)
    }

    fn dispatch_write_event(&mut self, pei: &PathEventInfo) {
        self.event_dispatch.dispatch(PathEvent::Write.as_ref(), pei)
    }

    fn dispatch_close_event(&mut self, pei: &PathEventInfo) {
        self.event_dispatch
            .dispatch(PathEvent::Remove.as_ref(), pei)
    }

    fn config_to_pei(
        service_name: &str,
        ns: &str,
        pod_name: &str,
        container_name: &str,
        log_path: &str,
    ) -> PathEventInfo {
        PathEventInfo {
            service_name: service_name.to_string(),
            ns: ns.to_string(),
            pod_name: pod_name.to_string(),
            container_name: container_name.to_string(),
            path: log_path.to_string(),
            ..Default::default()
        }
    }

    fn insert_config_file(&self, path: &str) {
        let _j_s_o_n_config = JSONConfig::from(path);
        self.insert(&_j_s_o_n_config.log_path.clone(), _j_s_o_n_config);
    }

    pub fn prepare(&self) -> Result<Vec<PathEventInfo>> {
        let mut result = vec![];
        for entry in WalkDir::new(self.docker_dir.clone()) {
            let entry = entry?;
            let path = entry.path().to_str().unwrap();
            match docker_config_file_type(path) {
                DockerConfigFileType::ConfigV2 => self.insert_config_file(path),
                DockerConfigFileType::Log => self.insert_key(path),
                _ => {}
            }
        }

        for item in self.cache.clone().iter() {
            match item.read() {
                Ok(hm) => {
                    for (_, v) in hm.iter() {
                        if let Some(_j_s_o_n_config) = v {
                            result.push(Self::config_to_pei(
                                &_j_s_o_n_config.get_service_name(),
                                &_j_s_o_n_config.get_ns(),
                                &_j_s_o_n_config.get_pod_name(),
                                &_j_s_o_n_config.get_container_name(),
                                &_j_s_o_n_config.log_path,
                            ));
                        }
                    }
                }
                Err(e) => {
                    panic!("error occurred prepare {:?}", e);
                }
            }
        }

        Ok(result)
    }

    pub fn watch_start(&mut self) -> Result<()> {
        let (tx, rx) = channel();
        let mut watcher = raw_watcher(tx).unwrap();

        watcher.watch(&self.docker_dir, RecursiveMode::Recursive)?;
        while let Ok(RawEvent {
            path: Some(path),
            op: Ok(op),
            cookie,
        }) = rx.recv()
        {
            let path = path.to_str().unwrap();
            match op {
                notify::Op::CREATE => match docker_config_file_type(path) {
                    DockerConfigFileType::ConfigV2 => self.insert(path, JSONConfig::from(path)),
                    DockerConfigFileType::Log => match self.get(path) {
                        Some(cfg) => {
                            let pei = Self::config_to_pei(
                                &cfg.get_service_name(),
                                &cfg.get_ns(),
                                &cfg.get_pod_name(),
                                &cfg.get_container_name(),
                                &cfg.log_path,
                            );
                            self.dispatch_create_event(&pei)
                        }
                        _ => {}
                    },
                    _ => continue,
                },
                notify::Op::WRITE => match docker_config_file_type(path) {
                    DockerConfigFileType::ConfigV2 => self.insert(path, JSONConfig::from(path)),
                    DockerConfigFileType::Log => self.dispatch_write_event(&PathEventInfo {
                        path: path.to_string(),
                        ..Default::default()
                    }),
                    _ => continue,
                },
                notify::Op::REMOVE => match docker_config_file_type(path) {
                    DockerConfigFileType::ConfigV2 => self.remove(path),
                    DockerConfigFileType::Log => self.dispatch_write_event(&PathEventInfo {
                        path: path.to_string(),
                        ..Default::default()
                    }),
                    _ => continue,
                },
                _ => {
                    if op == notify::Op::CREATE | notify::Op::WRITE {
                        match docker_config_file_type(path) {
                            DockerConfigFileType::ConfigV2 => {
                                self.insert(path, JSONConfig::from(path))
                            }
                            DockerConfigFileType::Log => match self.get(path) {
                                Some(cfg) => {
                                    let pei = Self::config_to_pei(
                                        &cfg.get_service_name(),
                                        &cfg.get_ns(),
                                        &cfg.get_pod_name(),
                                        &cfg.get_container_name(),
                                        &cfg.log_path,
                                    );
                                    self.dispatch_create_event(&pei);
                                    self.dispatch_write_event(&pei)
                                }
                                _ => {}
                            },
                            _ => continue,
                        }
                        continue;
                    } else if op == notify::Op::CREATE | notify::Op::REMOVE | notify::Op::WRITE
                        || op == notify::Op::CREATE | notify::Op::REMOVE
                        || op == notify::Op::REMOVE | notify::Op::WRITE
                    {
                        match docker_config_file_type(path) {
                            DockerConfigFileType::ConfigV2 => {
                                self.remove(path);
                            }
                            DockerConfigFileType::Log => {
                                self.dispatch_close_event(&PathEventInfo {
                                    path: path.to_string(),
                                    ..Default::default()
                                })
                            }
                            _ => continue,
                        }
                    }
                    println!("unhandled event {:?} {:?} ({:?})", op, path, cookie);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{AutoScanner, GetDebug, PathEvent};
    use event::Listener;

    #[test]
    fn it_works() {
        let mut auto_scanner = AutoScanner::new("".into(), ".".into());

        struct ListenerImpl;
        impl<T> Listener<T> for ListenerImpl
        where
            T: Clone + GetDebug,
        {
            fn handle(&self, t: T) {
                let _ = t.get_debug();
            }
        }
        auto_scanner.append_close_event_handle(ListenerImpl);
    }

    #[test]
    fn event_it_works() {
        assert_eq!(PathEvent::Remove.as_ref(), "NeedClose");
        assert_eq!(PathEvent::Create.as_ref(), "NeedOpen");
        assert_eq!(PathEvent::Write.as_ref(), "NeedWrite");
    }
}
