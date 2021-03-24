use crate::{get_pod_task, GetTask};
use db::GetPod;
use event::Listener;
use file::FileReaderWriter;
use scan::GetPathEventInfo;
use std::sync::{Arc, Mutex};

pub(crate) struct DBOpenEvent(pub Arc<Mutex<FileReaderWriter>>);
impl<T> Listener<T> for DBOpenEvent
where
    T: Clone + GetPod,
{
    fn handle(&self, t: T) {
        let mut pod = match t.get() {
            Some(pod) => pod.clone(),
            _ => return,
        };
        match self.0.lock() {
            Ok(mut frw) => frw.open_event(&mut pod),
            Err(e) => {
                eprintln!("{:?}", e);
            }
        }
    }
}

pub(crate) struct DBCloseEvent(pub Arc<Mutex<FileReaderWriter>>);
impl<T> Listener<T> for DBCloseEvent
where
    T: Clone + GetPod,
{
    fn handle(&self, t: T) {
        let mut pod = match t.get() {
            Some(pod) => pod.clone(),
            _ => return,
        };
        match self.0.lock() {
            Ok(mut frw) => frw.remove_event(&mut pod),
            Err(e) => {
                eprintln!("{:?}", e);
            }
        }
    }
}

pub(crate) struct ScannerCreateEvent(pub Arc<Mutex<FileReaderWriter>>);
impl<T> Listener<T> for ScannerCreateEvent
where
    T: Clone + GetPathEventInfo,
{
    fn handle(&self, t: T) {
        let mut pod = t.get().to_pod();
        db::insert(&pod);

        match get_pod_task(&pod.pod_name) {
            Some(t) => {
                if !t.pod.is_upload() {
                    return;
                }
                match self.0.lock() {
                    Ok(mut frw) => frw.write_event(&mut pod),
                    Err(e) => {
                        eprintln!("{:?}", e);
                    }
                }
            }
            None => {}
        }
    }
}

pub(crate) struct ScannerWriteEvent(pub Arc<Mutex<FileReaderWriter>>);
impl<T> Listener<T> for ScannerWriteEvent
where
    T: Clone + GetPathEventInfo,
{
    fn handle(&self, t: T) {
        match self.0.lock() {
            Ok(mut frw) => frw.write_event(&mut t.get().to_pod()),
            Err(e) => {
                eprintln!("{:?}", e);
            }
        }
    }
}

pub(crate) struct ScannerCloseEvent();
impl<T> Listener<T> for ScannerCloseEvent
where
    T: Clone + GetPathEventInfo,
{
    fn handle(&self, t: T) {
        let mut pod = t.get().to_pod();
        pod.set_state_stop();
        db::update(&pod);
    }
}

pub(crate) struct TaskRunEvent(pub Arc<Mutex<FileReaderWriter>>);
impl<T> Listener<T> for TaskRunEvent
where
    T: Clone + GetTask,
{
    fn handle(&self, t: T) {
        let task = t.get();
        let mut pod = task.pod.clone();
        match self.0.lock() {
            Ok(mut frw) => frw.open_event(&mut pod),
            Err(e) => {
                eprintln!("{:?}", e);
            }
        }
    }
}

pub(crate) struct TaskStopEvent(pub Arc<Mutex<FileReaderWriter>>);
impl<T> Listener<T> for TaskStopEvent
where
    T: Clone + GetTask,
{
    fn handle(&self, t: T) {
        let task = t.get();
        let mut pod = task.pod.clone();
        match self.0.lock() {
            Ok(mut frw) => frw.close_event(&mut pod),
            Err(e) => {
                eprintln!("{:?}", e);
            }
        }
    }
}
