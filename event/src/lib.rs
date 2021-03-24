use std::collections::HashMap;

pub trait Listener<T>
where
    T: Clone,
{
    fn handle(&self, t: T);
}

pub type BoxListener<T> = Box<dyn Listener<T> + Send + Sync>;

pub type VecBoxListener<T> = Vec<BoxListener<T>>;

pub struct Dispatch<T: Clone> {
    listeners: HashMap<String, VecBoxListener<T>>,
}

impl<T: Clone> Dispatch<T> {
    pub fn new() -> Self {
        Self {
            listeners: HashMap::new(),
        }
    }

    pub fn registry<L>(&mut self, name: &str, listener: L)
    where
        L: Listener<T> + Send + Sync + 'static,
    {
        let listener = Box::new(listener);
        if self.listeners.contains_key(name) {
            self.listeners.get_mut(name).unwrap().push(listener);
        } else {
            self.listeners.insert(name.to_string(), vec![listener]);
        }
    }

    pub fn dispatch(&mut self, name: &str, d: &T) {
        if self.listeners.contains_key(name) {
            for listener in self.listeners.get(name).unwrap().iter() {
                listener.handle(d.clone())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Dispatch, Listener};
    use std::{
        sync::mpsc::{sync_channel, SyncSender},
        thread,
    };

    struct ListenerImpl<T> {
        sender: SyncSender<T>,
    }

    impl<T> ListenerImpl<T> {
        pub fn new(sender: SyncSender<T>) -> Self {
            Self { sender }
        }
    }

    impl<T> Listener<T> for ListenerImpl<T>
    where
        T: Clone,
    {
        fn handle(&self, t: T) {
            self.sender.send(t).unwrap()
        }
    }

    #[test]
    fn it_works() {
        let mut dispatch = Dispatch::<String>::new();

        let (tx, rx) = sync_channel::<String>(1);
        let tx1 = tx.clone();
        let li1 = ListenerImpl::new(tx1);
        let tx2 = tx.clone();
        let li2 = ListenerImpl::new(tx2);

        dispatch.registry(&"pod_name_update", li1);
        dispatch.registry(&"pod_name_update", li2);

        let join_handle = thread::spawn(move || {
            dispatch.dispatch(&"pod_name_update", &"abc".to_string());
        });

        for item in rx.recv() {
            assert_eq!(item, "abc".to_owned());
        }
        join_handle.join().unwrap();
    }
}
