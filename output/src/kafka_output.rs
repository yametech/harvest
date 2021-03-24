use super::{IOutput, Item, Result};
use async_std::task;
use kafka::producer::{Producer, Record, RequiredAcks};
use ringbuf::{Consumer as RConsumer, Producer as RProducer, RingBuffer};

use std::time::Instant;
use std::{collections::HashMap, thread, time::Duration};

#[derive(Clone, Debug)]
struct KafkaOutputConfig {
    broker: Vec<String>,
    topic: String,
}

pub(crate) struct KafkaOuput {
    channels: HashMap<String, RProducer<Item>>,
}

impl KafkaOuput {
    pub fn new() -> KafkaOuput {
        Self {
            channels: HashMap::new(),
        }
    }

    // channel =  kafka:topic@10.200.100.200:9092,10.200.100.201:9092
    fn parse_uri_to_producer(&self, channel: &str) -> KafkaOutputConfig {
        let type_topic_ips = channel.split("@").collect::<Vec<&str>>();
        let broker = type_topic_ips[1]
            .split(",")
            .map(|k| k.to_string())
            .collect::<Vec<String>>();
        let topic = type_topic_ips[0].split(":").collect::<Vec<&str>>()[1];
        KafkaOutputConfig {
            broker: broker,
            topic: topic.to_string(),
        }
    }

    fn write_in(&mut self, channel: &str, item: &Item) {
        let prod = self.channels.get_mut(channel).unwrap();
        loop {
            if prod.is_full() {
                thread::sleep(Duration::from_millis(1));
                continue;
            }
            prod.push(item.clone()).unwrap();
            return;
        }
    }

    async fn write_out(topic: &str, cons: &mut RConsumer<Item>, kp: &mut Producer) {
        let count = 5;
        let mut index = 0;
        let mut now = Instant::now();
        let mut write_buffer = Vec::with_capacity(count);
        loop {
            if cons.is_empty() {
                thread::sleep(Duration::from_millis(1));
                continue;
            }
            let content = cons.pop().unwrap().string();
            write_buffer.push(Record::from_key_value(
                topic,
                format!("{:?}", index),
                content,
            ));
            index += 1;

            if index >= write_buffer.capacity() || now.elapsed().as_secs() > 1 {
                match kp.send_all(&write_buffer) {
                    Ok(_) => {
                        index = 0;
                        now = Instant::now();
                        write_buffer.clear();
                    }
                    Err(e) => {
                        eprintln!("{:?}", e);
                        continue;
                    }
                }
            }
        }
    }

    fn not_exist_create(&mut self, channel: &str) -> Result<()> {
        let cfg = self.parse_uri_to_producer(channel);
        let mut kp = match Producer::from_hosts(cfg.broker)
            .with_ack_timeout(Duration::from_secs(1))
            .with_required_acks(RequiredAcks::One)
            .create()
        {
            Ok(it) => it,
            Err(e) => return Err(Box::new(e)),
        };

        let ring_buff = RingBuffer::new(10240);
        let (p, mut c) = ring_buff.split();

        let topic = cfg.topic.clone();

        task::spawn(async move {
            Self::write_out(&topic, &mut c, &mut kp).await;
        });

        self.channels.insert(channel.to_string(), p);

        Ok(())
    }

    fn write_to_channel_queue(&mut self, channel: &str, item: Item) -> Result<()> {
        self.write_in(channel, &item);
        Ok(())
    }
}

impl IOutput for KafkaOuput {
    fn write(&mut self, channel: &str, item: Item) -> Result<()> {
        if !self.channels.contains_key(channel) {
            self.not_exist_create(channel)?;
        }
        self.write_to_channel_queue(channel, item)
    }
}

// #[cfg(test)]
// mod tests {
//     use super::KafkaOuput;
//     use crate::IOutput;
//     use common::Item;
//     use std::{thread, time::Duration};

//     #[test]
//     fn kafka_working() {
//         //first docker run a kafka
//         let mut ko = KafkaOuput::new();
//         for index in 0..1000 {
//             let item = Item::from(format!("{:?} xx", index).as_str());
//             if let Err(e) = ko.write(&"kafka:test@10.200.100.200:9092", item) {
//                 panic!("{:?}", e)
//             }
//         }
//         thread::sleep(Duration::from_secs(5));
//     }
// }
