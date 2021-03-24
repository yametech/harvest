use std::{collections::HashMap, fs::File, io::BufReader};

const SERVICE_LABEL_NAME: &'static str = "io.yametech.pod.harvest_service_lable";
const NAMESPACE_LABEL_NAME: &'static str = "io.kubernetes.pod.namespace";
const PODNAME_LABEL_NAME: &'static str = "io.kubernetes.pod.name";
const CONTAINERNAME_LABEL_NAME: &'static str = "io.kubernetes.container.name";

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JSONConfig {
    #[serde(rename = "State")]
    pub state: State,
    #[serde(rename = "ID")]
    pub id: String,
    #[serde(rename = "Path")] // container start command
    pub path: String,
    #[serde(rename = "Config")]
    pub config: Config,
    #[serde(rename = "Image")]
    pub image: String,
    #[serde(rename = "LogPath")]
    pub log_path: String,
    #[serde(rename = "Name")]
    pub name: String,
}

impl From<&str> for JSONConfig {
    fn from(path: &str) -> Self {
        serde_json::from_reader::<_, JSONConfig>(BufReader::new(File::open(path).unwrap())).unwrap()
    }
}

impl From<&[u8]> for JSONConfig {
    fn from(bytes: &[u8]) -> Self {
        serde_json::from_reader::<_, JSONConfig>(BufReader::new(bytes)).unwrap()
    }
}

impl JSONConfig {
    pub fn get_ns(&self) -> String {
        if let Some(label) = self.config.labels.get(NAMESPACE_LABEL_NAME) {
            return label.to_string();
        }
        "".to_string()
    }

    pub fn get_pod_name(&self) -> String {
        if let Some(label) = self.config.labels.get(PODNAME_LABEL_NAME) {
            return label.to_string();
        }
        "".to_string()
    }

    pub fn get_service_name(&self) -> String {
        if let Some(label) = self.config.labels.get(SERVICE_LABEL_NAME) {
            return label.to_string();
        }
        "".to_string()
    }

    pub fn get_container_name(&self) -> String {
        if let Some(label) = self.config.labels.get(CONTAINERNAME_LABEL_NAME) {
            if label == "POD" {
                return self.get_pod_name();
            }
            return label.to_string();
        }
        "".to_string()
    }
}

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct State {
    #[serde(rename = "Running")]
    pub running: bool,
    #[serde(rename = "Paused")]
    pub paused: bool,
    #[serde(rename = "Restarting")]
    pub restarting: bool,
    #[serde(rename = "OOMKilled")]
    pub oomkilled: bool,
    #[serde(rename = "RemovalInProgress")]
    pub removal_in_progress: bool,
    #[serde(rename = "Dead")]
    pub dead: bool,
    #[serde(rename = "Pid")]
    pub pid: i64,
    #[serde(rename = "StartedAt")]
    pub started_at: String,
    #[serde(rename = "FinishedAt")]
    pub finished_at: String,
}

#[derive(Default, Debug, Clone, PartialEq, serde_derive::Serialize, serde_derive::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    #[serde(rename = "Hostname")]
    pub hostname: String,
    #[serde(rename = "Image")]
    pub image: String,
    #[serde(rename = "Labels")]
    pub labels: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::JSONConfig;

    #[test]
    fn it_works() {
        const TEST_STRING: &'static str = "
        {
            \"StreamConfig\": {},
            \"State\": {
                \"Running\": true,
                \"Paused\": false,
                \"Restarting\": false,
                \"OOMKilled\": false,
                \"RemovalInProgress\": false,
                \"Dead\": false,
                \"Pid\": 19895,
                \"ExitCode\": 0,
                \"Error\": \"\",
                \"StartedAt\": \"2021-03-16T09:05:01.461813069Z\",
                \"FinishedAt\": \"0001-01-01T00:00:00Z\",
                \"Health\": null
            },
            \"ID\": \"58044a726890a4cbdd054a75cfa70b7e776d73f04925685aa827f162cc9026bd\",
            \"Created\": \"2021-03-16T09:04:58.040873904Z\",
            \"Managed\": false,
            \"Path\": \"/pause\",
            \"Args\": [],
            \"Config\": {
                \"Hostname\": \"sky-fcms-web-ui-0-b-0\",
                \"Domainname\": \"\",
                \"User\": \"\",
                \"AttachStdin\": false,
                \"AttachStdout\": false,
                \"AttachStderr\": false,
                \"Tty\": false,
                \"OpenStdin\": false,
                \"StdinOnce\": false,
                \"Env\": [\"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\"],
                \"Cmd\": null,
                \"Image\": \"registry.aliyuncs.com/google_containers/pause:3.2\",
                \"Volumes\": null,
                \"WorkingDir\": \"/\",
                \"Entrypoint\": [\"/pause\"],
                \"OnBuild\": null,
                \"Labels\": {
                    \"app\": \"sky-fcms-web-ui\",
                    \"app-uuid\": \"finance-dev-sky-fcms-web-ui\",
                    \"controller-revision-hash\": \"sky-fcms-web-ui-0-b-7ff677df9f\",
                    \"io.kubernetes.container.name\": \"POD\",
                    \"io.kubernetes.docker.type\": \"podsandbox\",
                    \"io.kubernetes.pod.name\": \"sky-fcms-web-ui-0-b-0\",
                    \"io.kubernetes.pod.namespace\": \"finance-dev\",
                    \"io.kubernetes.pod.uid\": \"c7621e69-de2b-4a5c-b439-6e3021dba432\",
                    \"statefulset.kubernetes.io/pod-name\": \"sky-fcms-web-ui-0-b-0\",
                    \"yce-cloud-extensions\": \"sky-fcms-web-ui-web\"
                }
            },
            \"Image\": \"sha256:80d28bedfe5dec59da9ebf8e6260224ac9008ab5c11dbbe16ee3ba3e4439ac2c\",
            \"NetworkSettings\": {
                \"Bridge\": \"\",
                \"SandboxID\": \"59340431598e6276afdc995ac880a3d38555b1a1ab47702796db96a0a36c80c3\",
                \"HairpinMode\": false,
                \"LinkLocalIPv6Address\": \"\",
                \"LinkLocalIPv6PrefixLen\": 0,
                \"Networks\": {
                    \"none\": {
                        \"IPAMConfig\": null,
                        \"Links\": null,
                        \"Aliases\": null,
                        \"NetworkID\": \"ed12f3414f244e47a572796ffb6aaed9e97491fa4e42cf036ccbe035bb01e4b9\",
                        \"EndpointID\": \"39df2e46027174bcdedb39aec2286290864c141a5e8e9d16125504fa5362d5d8\",
                        \"Gateway\": \"\",
                        \"IPAddress\": \"\",
                        \"IPPrefixLen\": 0,
                        \"IPv6Gateway\": \"\",
                        \"GlobalIPv6Address\": \"\",
                        \"GlobalIPv6PrefixLen\": 0,
                        \"MacAddress\": \"\",
                        \"DriverOpts\": null,
                        \"IPAMOperational\": false
                    }
                },
                \"Service\": null,
                \"Ports\": {},
                \"SandboxKey\": \"/var/run/docker/netns/59340431598e\",
                \"SecondaryIPAddresses\": null,
                \"SecondaryIPv6Addresses\": null,
                \"IsAnonymousEndpoint\": false,
                \"HasSwarmEndpoint\": false
            },
            \"LogPath\": \"/data/docker/containers/58044a726890a4cbdd054a75cfa70b7e776d73f04925685aa827f162cc9026bd/58044a726890a4cbdd054a75cfa70b7e776d73f04925685aa827f162cc9026bd-json.log\",
            \"Name\": \"/k8s_POD_sky-fcms-web-ui-0-b-0_finance-dev_c7621e69-de2b-4a5c-b439-6e3021dba432_29\",
            \"Driver\": \"overlay2\",
            \"OS\": \"linux\",
            \"MountLabel\": \"\",
            \"ProcessLabel\": \"\",
            \"RestartCount\": 0,
            \"HasBeenStartedBefore\": true,
            \"HasBeenManuallyStopped\": false,
            \"MountPoints\": {},
            \"SecretReferences\": null,
            \"ConfigReferences\": null,
            \"AppArmorProfile\": \"\",
            \"HostnamePath\": \"/data/docker/containers/58044a726890a4cbdd054a75cfa70b7e776d73f04925685aa827f162cc9026bd/hostname\",
            \"HostsPath\": \"/data/docker/containers/58044a726890a4cbdd054a75cfa70b7e776d73f04925685aa827f162cc9026bd/hosts\",
            \"ShmPath\": \"/data/docker/containers/58044a726890a4cbdd054a75cfa70b7e776d73f04925685aa827f162cc9026bd/mounts/shm\",
            \"ResolvConfPath\": \"/data/docker/containers/58044a726890a4cbdd054a75cfa70b7e776d73f04925685aa827f162cc9026bd/resolv.conf\",
            \"SeccompProfile\": \"unconfined\",
            \"NoNewPrivileges\": false
        }";

        let _j_s_o_n_config = JSONConfig::from(TEST_STRING.as_bytes());

        assert_eq!(_j_s_o_n_config.get_ns(), "finance-dev");
        assert_eq!(_j_s_o_n_config.get_pod_name(), "sky-fcms-web-ui-0-b-0");
        assert_eq!(_j_s_o_n_config.log_path, "/data/docker/containers/58044a726890a4cbdd054a75cfa70b7e776d73f04925685aa827f162cc9026bd/58044a726890a4cbdd054a75cfa70b7e776d73f04925685aa827f162cc9026bd-json.log");
    }
}
