use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub enum State {
    Ready,
    Running,
    Stopped,
}
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Pod {
    pub ns: String,
    pub service_name: String,
    pub pod_name: String,
    pub container: String,
    pub path: String, // on this the uuid path is unique identifier
    pub offset: i64,
    pub is_upload: bool,
    pub state: State,
    pub filter: String,
    pub output: String,
    pub ips: Vec<String>,
    pub last_offset: i64,
    pub node_name: String,
}

impl Pod {
    pub fn set_state_run(&mut self) -> &mut Self {
        self.state = State::Running;
        self
    }

    pub fn set_state_ready(&mut self) -> &mut Self {
        self.state = State::Ready;
        self
    }

    pub fn set_state_stop(&mut self) -> &mut Self {
        self.state = State::Stopped;
        self
    }

    pub fn is_upload(&self) -> bool {
        self.is_upload == true
    }

    pub fn upload(&mut self) -> &mut Self {
        self.is_upload = true;
        self
    }

    pub fn un_upload(&mut self) -> &mut Self {
        self.is_upload = false;
        self
    }

    pub fn merge(&mut self, other: &Pod) -> &mut Self {
        self.is_upload = other.is_upload;
        self.filter = other.filter.clone();
        self.output = other.output.clone();
        self.offset = other.offset.clone();
        self.node_name = other.node_name.clone();
        self.ips = self.ips.clone();
        self.state = other.state.clone();
        self
    }

    pub fn is_running(&self) -> bool {
        self.state == State::Running
    }

    pub fn is_ready(&self) -> bool {
        self.state == State::Ready
    }

    pub fn is_stop(&self) -> bool {
        self.state == State::Stopped
    }

    pub fn merge_with(&mut self, other: &Pod) {
        self.merge(other);
    }

    pub fn compare_ns_pod(&self, other: &Pod) -> bool {
        self.ns == other.ns && self.pod_name == other.pod_name
    }

    pub fn incr_offset(&mut self, offset: i64) -> &mut Self {
        self.offset += offset;
        self.last_offset = offset;
        self
    }
}

impl Default for Pod {
    fn default() -> Pod {
        Pod {
            service_name: "".to_string(),
            path: "".to_string(),
            offset: 0,
            ns: "".to_string(),
            pod_name: "".to_string(),
            container: "".to_string(),
            is_upload: false,
            state: State::Ready,
            filter: "".to_string(),
            output: "".to_string(),
            ips: Vec::new(),
            last_offset: 0,
            node_name: "".to_string(),
        }
    }
}

pub trait GetPod {
    fn get(&self) -> Option<&Pod>;
}

impl GetPod for Pod {
    fn get(&self) -> Option<&Pod> {
        Some(self)
    }
}

pub type PodList = Vec<Pod>;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PodListMarshaller(pub PodList);

impl PodListMarshaller {
    pub fn to_json(&self) -> String {
        match serde_json::to_string(&self.0) {
            Ok(contents) => contents,
            Err(_) => "".to_owned(),
        }
    }
}
