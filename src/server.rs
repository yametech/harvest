use super::*;
use async_std::task;
use common::new_arc_mutex;
use file::FileReaderWriter;
use rocket::config::{Config, Environment};
use rocket::routes;
use scan::AutoScanner;

pub struct Harvest<'a> {
    node_name: &'a str,
    namespace: &'a str,
    docker_dir: &'a str,
    api_server_addr: &'a str,
}

impl<'a> Harvest<'a> {
    pub fn new(
        namespace: &'a str,
        docker_dir: &'a str,
        api_server_addr: &'a str,
        node_name: &'a str,
    ) -> Self {
        Self {
            namespace,
            docker_dir,
            node_name,
            api_server_addr,
        }
    }

    pub fn start(&mut self) -> Result<()> {
        let scanner = new_arc_rwlock(AutoScanner::new(
            String::from(self.namespace),
            String::from(self.docker_dir),
        ));

        let frw = new_arc_mutex(FileReaderWriter::new(0));

        if let Ok(mut scan) = scanner.write() {
            // registry scanner event handle
            scan.append_create_event_handle(ScannerCreateEvent(frw.clone()));
            scan.append_write_event_handle(ScannerWriteEvent(frw.clone()));
            scan.append_close_event_handle(ScannerCloseEvent());
        }

        // registry db open/close events
        db::registry_open_event_listener(DBOpenEvent(frw.clone()));
        db::registry_close_event_listener(DBCloseEvent(frw.clone()));

        // registry task run/stop event handle
        registry_task_run_event_listener(TaskRunEvent(frw.clone()));
        registry_task_stop_event_listener(TaskStopEvent(frw.clone()));

        let mut tasks = vec![];
        // start auto scanner with a new async
        tasks.push(task::spawn(async move {
            let mut scan = match scanner.write() {
                Ok(it) => it,
                Err(e) => {
                    eprintln!("{}", e);
                    return;
                }
            };

            let res = match scan.prepare() {
                Ok(it) => it,
                Err(e) => {
                    eprintln!("{}", e);
                    return;
                }
            };

            // add to local MemDatabase
            for item in res.iter() {
                db::insert(&item.to_pod())
            }

            if let Err(e) = scan.watch_start() {
                panic!("{:?}", e);
            }
        }));

        tasks.push(task::spawn(async move {
            let cfg = Config::build(Environment::Production)
                .address("0.0.0.0")
                .port(8080)
                .secret_key("8Xui8SN4mI+7egV/9dlfYYLGQJeEx4+DwmSQLwDVXJg=")
                .unwrap();

            rocket::custom(cfg)
                .mount("/", routes![query_pod, query_tasks])
                .register(catchers![not_found])
                .launch();
        }));

        let api_server_addr = self.api_server_addr.to_string().clone();
        let node_name = self.node_name.to_string().clone();
        recv_tasks(&api_server_addr, &node_name);

        for _ in tasks {}

        task_close();
        Ok(())
    }
}
