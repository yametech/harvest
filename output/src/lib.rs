use common::{Item, Result};
use kafka_output::KafkaOuput;
use once_cell::sync::Lazy;

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

mod kafka_output;

pub use OUTPUTS as OTS;

pub static OUTPUTS: Lazy<Arc<Mutex<Outputs>>> = Lazy::new(|| {
    let outputs = Arc::new(Mutex::new(Outputs::new()));
    if let Ok(mut ots) = outputs.lock() {
        ots.registry_output("fake_output", Output::new(FakeOutput));
        ots.registry_output("counter_output", Output::new(Counter(AtomicUsize::new(0))));
    }
    outputs
});

pub fn registry_output(channel: &str) {
    if !channel.starts_with("kafka") {
        return;
    }
    if let Ok(mut ots) = OUTPUTS.lock() {
        if ots.contains_output(channel) {
            return;
        }
        ots.registry_output(channel, Output::new(KafkaOuput::new()));
    }
}

pub struct Outputs {
    output_listener: HashMap<String, Box<dyn IOutput>>,
}

impl Outputs {
    pub fn new() -> Self {
        Self {
            output_listener: HashMap::new(),
        }
    }

    pub fn contains_output(&self, channel: &str) -> bool {
        self.output_listener.contains_key(channel)
    }

    pub fn registry_output<T>(&mut self, channel: &str, t: T)
    where
        T: IOutput + Send + Sync + 'static,
    {
        if self.output_listener.contains_key(channel) {
            return;
        }
        self.output_listener
            .insert(channel.to_string(), Box::new(t));
    }

    pub fn output(&mut self, channel: &str, line: &str) {
        if !self.output_listener.contains_key(channel) {
            if line.len() == 0 {
                return;
            }
            eprintln!("output not found `{:?}`", channel);
            eprintln!("use stdout {:?}", line);
            return;
        }

        match self.output_listener.get_mut(channel) {
            Some(o) => {
                if let Err(e) = o.write(channel, Item::from(line)) {
                    eprintln!("{:?}", e);
                }
            }
            _ => {}
        }
    }
}

pub trait IOutput: Send + Sync + 'static {
    fn write(&mut self, channel: &str, item: Item) -> Result<()>;
}

#[derive(Debug)]
pub struct Output<T: ?Sized + IOutput> {
    o: T,
}
unsafe impl<T: IOutput> Send for Output<T> {}
unsafe impl<T: IOutput> Sync for Output<T> {}

impl<T: IOutput> Output<T> {
    pub fn new(o: T) -> Self {
        Self { o }
    }
}

impl<T: IOutput> IOutput for Output<T> {
    // delegate write IOutput.write
    fn write(&mut self, channel: &str, item: Item) -> Result<()> {
        self.o.write(channel, item)
    }
}

pub fn sync_via_output(line: &str, channel: &str, output: Arc<Mutex<dyn IOutput>>) -> Result<()> {
    if let Ok(mut output) = output.lock() {
        return output.write(channel, Item::from(line));
    }
    Ok(())
}

pub fn via_output<'a, T: IOutput>(channel: &str, line: &'a str, o: &'a mut T) -> Result<()> {
    if line.len() == 0 {
        return Ok(());
    }
    o.write(channel, Item::from(line))
}

pub fn new_sync_output<T: IOutput>(t: T) -> Arc<Mutex<T>> {
    Arc::new(Mutex::new(t))
}

pub struct FakeOutput;

impl IOutput for FakeOutput {
    fn write(&mut self, _: &str, item: Item) -> Result<()> {
        println!("FakeOutput content: {:?}", item.string());
        Ok(())
    }
}

pub struct Counter(AtomicUsize);
impl IOutput for Counter {
    fn write(&mut self, _: &str, _: Item) -> Result<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        if self.0.load(Ordering::Relaxed) as i64 % 10000 == 0 {
            println!("Counter {:?}", self.0.load(Ordering::Relaxed));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn it_works() {
        let output = &mut Output::new(FakeOutput);
        if let Err(e) = via_output("fake_output", &r#"abc"#, output) {
            panic!("{}", e);
        }
    }

    #[test]
    fn it_works_with_multiple_threads() {
        let fake_output = Arc::new(Mutex::new(FakeOutput));

        let mut list = vec![];
        for _ in 0..2 {
            let output = fake_output.clone();
            list.push(thread::spawn(move || {
                if let Err(e) = sync_via_output("fake_output", &r#"abc"#, output) {
                    panic!("{}", e);
                }
            }));
        }

        for j in list.into_iter() {
            j.join().unwrap()
        }
    }

    #[test]
    fn it_works_with_outputs() {
        let mut outputs = Outputs::new();
        outputs.registry_output("fake_output", Output::new(FakeOutput));
        outputs.output("fake_output", "123")
    }

    #[test]
    fn it_static_outputs() {
        if let Ok(mut ots) = OUTPUTS.try_lock() {
            ots.output("fake_output", "1")
        }
        let mut j = vec![];
        let o1 = OUTPUTS.clone();
        j.push(thread::spawn(move || {
            if let Ok(mut ots) = o1.try_lock() {
                ots.output("fake_output", "2")
            }
        }));

        let o2 = OUTPUTS.clone();
        j.push(thread::spawn(move || {
            if let Ok(mut ots) = o2.try_lock() {
                ots.output("fake_output", "3")
            }
        }));

        let _ = j.into_iter().map(|_j| _j.join().unwrap());
    }
}
