pub struct Config {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub passwd: String,
}

// todo:同一个连接
impl Config {
    pub fn new() -> Self {
        Config {
            host: String::from("192.168.0.54"),
            port: 8848,
            user: String::from("admin"),
            passwd: String::from("123456"),
        }
    }
}
