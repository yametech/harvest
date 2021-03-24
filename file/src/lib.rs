#![feature(seek_stream_len)]
extern crate crossbeam_channel;
use async_std::task;
use crossbeam_channel::{unbounded as async_channel, Sender};
use db::Pod;
use output::OTS;
use serde_json::json;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};

pub enum SendFileEvent {
    Close,
    Other,
}

pub struct FileReaderWriter {
    file_handles: HashMap<String, Sender<SendFileEvent>>,
}

impl FileReaderWriter {
    pub fn new(num_workers: usize) -> Self {
        let _ = num_workers;
        Self {
            file_handles: HashMap::new(),
        }
    }

    pub fn close_event(&mut self, pod: &Pod) {
        if let Some(tx) = self.file_handles.get(&pod.path) {
            if let Err(e) = tx.send(SendFileEvent::Close) {
                eprintln!("frw send close to {:?} handle error: {:?}", &pod.path, e);
            }
            self.file_handles.remove(&pod.path);
        }
    }

    pub fn remove_event(&mut self, pod: &Pod) {
        if let Some(tx) = self.file_handles.get(&pod.path) {
            if let Err(e) = tx.send(SendFileEvent::Close) {
                eprintln!("frw send remove to {:?} handle error: {:?}", &pod.path, e);
            }
            self.file_handles.remove(&pod.path);
        };

        db::delete(&pod.path);
    }

    pub fn open_event(&mut self, pod: &mut Pod) {
        if self.file_handles.contains_key(&pod.path) {
            return;
        }
        self.open(pod);
    }

    pub fn write_event(&mut self, pod: &mut Pod) {
        let handle = match self.file_handles.get(&pod.path) {
            Some(it) => it,
            _ => {
                return;
            }
        };
        if let Err(e) = handle.send(SendFileEvent::Other) {
            eprintln!("frw send write event error: {}, path: {}", e, &pod.path)
        }
    }

    fn open(&mut self, pod: &mut Pod) {
        let mut offset = pod.offset;
        let mut file = match File::open(&pod.path) {
            Ok(file) => file,
            Err(e) => {
                eprintln!("frw open file {:?} error: {:?}", pod.path, e);
                return;
            }
        };
        if let Err(e) = file.seek(SeekFrom::Current(offset)) {
            eprintln!("frw open event seek failed, error: {}", e);
            return;
        }

        let file_size = file.stream_len().unwrap();
        let mut br = BufReader::new(file);
        let mut bf = String::new();
        let outputs = OTS.clone();

        loop {
            let cur_size = br.read_line(&mut bf).unwrap();
            if let Ok(mut ot) = outputs.lock() {
                ot.output(&pod.output, &encode_message(&pod, bf.as_str()))
            }
            db::incr_offset(&pod.path, cur_size as i64);
            bf.clear();

            offset += cur_size as i64;
            if offset >= file_size as i64 {
                break;
            }
        }

        pod.offset = offset;
        let thread_pod = pod.clone();
        let (tx, rx) = async_channel::<SendFileEvent>();
        task::spawn(async move {
            while let Ok(evt) = rx.recv() {
                match evt {
                    SendFileEvent::Close => {
                        break;
                    }
                    _ => {
                        let incr_offset = br.read_line(&mut bf).unwrap();
                        if let Ok(mut ot) = outputs.lock() {
                            ot.output(
                                &thread_pod.output,
                                &encode_message(&thread_pod, bf.as_str()),
                            )
                        }
                        db::incr_offset(&thread_pod.path, incr_offset as i64);
                        bf.clear();
                    }
                };
            }
        });

        self.file_handles.insert(pod.path.to_string(), tx);

        db::update(&pod.set_state_run())
    }
}

fn encode_message<'a>(pod: &'a Pod, message: &'a str) -> String {
    if message.len() == 0 {
        return "".to_string();
    }
    json!({
        "custom":
            {
              "nodeId":pod.pod_name,
              "container":pod.container,
              "serviceName":pod.service_name,
              "ips":pod.ips,
              "version":"v1.0.0",
            },
        "message":message}
    )
    .to_string()
}

#[cfg(test)]
mod tests {
    use crate::FileReaderWriter;
    use db::Pod;

    #[test]
    fn it_works() {
        let mut input = FileReaderWriter::new(10);
        input.open_event(&mut Pod::default());
    }
}
