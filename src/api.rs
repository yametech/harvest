use super::{run_task, stop_task, tasks_json, Task};
use rocket::get;
use rocket_contrib::json::JsonValue;
use serde::{Deserialize, Serialize};
use sse_client::EventSource;

const RUN: &'static str = "run";
const STOP: &'static str = "stop";

pub(crate) fn recv_tasks(addr: &str, node_name: &str) {
    let event_sources = match EventSource::new(addr) {
        Ok(it) => it,
        Err(e) => {
            eprintln!("{:?}", e);
            return;
        }
    };

    for event in event_sources.receiver().iter() {
        println!("recv task {:?}", event);
        let request = match serde_json::from_str::<ApiServerRequest>(&event.data) {
            Ok(it) => it,
            Err(e) => {
                eprintln!(
                    "recv event parse json error or connect to api server error: {:?} \n data: {:?}",
                    e, &event.data
                );
                continue;
            }
        };
        if !request.has_node_events(&node_name) {
            continue;
        }

        output::registry_output(request.output);

        for task in request.to_pod_tasks() {
            if request.op == RUN {
                run_task(&task);
            } else if request.op == STOP {
                stop_task(&task);
            } else {
                println!("recv api server unknown event: {:?}", request)
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct RequestPod<'a> {
    pub(crate) node: &'a str,
    pub(crate) pod: &'a str,
    pub(crate) ips: Vec<&'a str>,
    pub(crate) offset: i64,
}

//{"op":"run","ns":"default","service_name":"xx_service","rules":"","output":"fake_output","pods":[{"node":"node1","pod":"xx","ips":["127.0.0.1"],"offset":0}]}
//{"op":"stop","ns":"default","service_name":"xx_service","rules":"","output":"fake_output","pods":[{"node":"node1","pod":"xx","ips":["127.0.0.1"],"offset":0}]}
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct ApiServerRequest<'a> {
    op: &'a str,
    pub(crate) ns: &'a str,
    pub(crate) output: &'a str,
    pub(crate) rules: &'a str,
    pub(crate) service_name: &'a str,
    pub(crate) pods: Vec<RequestPod<'a>>,
}

impl<'a> ApiServerRequest<'a> {
    pub fn to_pod_tasks(&self) -> Vec<Task> {
        self.pods
            .iter()
            .map(|req_pod| {
                let mut task = Task::from(req_pod.clone());
                task.pod.ns = self.ns.to_string();
                task.pod.output = self.output.to_string();
                task.pod.service_name = self.service_name.to_string();
                task.pod.filter = self.rules.to_string();
                task
            })
            .collect::<Vec<Task>>()
    }
}

impl<'a> ApiServerRequest<'a> {
    pub fn has_node_events(&self, node_name: &str) -> bool {
        for pod in self.pods.iter() {
            if pod.node == node_name {
                return true;
            }
        }
        false
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Request {
    namespace: String,
    pod: String,
    filter: String,
    output: String,
    upload: bool,
}

#[get("/tasks")]
pub(crate) fn query_tasks() -> JsonValue {
    json!(tasks_json())
}

#[get("/pods")]
pub(crate) fn query_pod() -> JsonValue {
    json!(db::all_to_json())
}

#[catch(404)]
pub(crate) fn not_found() -> JsonValue {
    json!({
        "status": "error",
        "reason": "Resource was not found."
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
