use std::collections::HashMap;

use dolphindb::client::ClientBuilder;

#[tokio::main]
async fn main() {
    // Since the client supports multiple configurations, we provide the ClientBuilder utility class to assist in creating client objects.
    // First, you need to provide the server's IP address and port number.
    let mut builder = ClientBuilder::new("127.0.0.1:8848");
    // If your server has access control, you may need to log in before performing further operations.
    // Here, we use the default username and password.
    builder.with_auth(("admin", "123456"));
    // After the configuration is complete, execute the connect method.
    // Upon successful connection, a client object for interaction is returned.
    let mut client = builder.connect().await.unwrap();

    // One of the basic functionalities of the client is executing DolphinDB scripts.
    // Here, a pair object is created on the server.
    let res = client.run_script("a = pair(`a, `b)").await.unwrap();
    // If the DolphinDB script does not return a value, the returned `res` is None.
    if let Some(ref c) = res {
        println!("{}", c);
    }
    // If the DolphinDB script has a return value, the returned `res` is a ConstantImpl.
    let res = client.run_script("a").await.unwrap();
    if let Some(ref c) = res {
        println!("{}", c);
    }

    // The client can also upload data from Rust to the server and generate a variable.
    let mut variables = HashMap::new();
    // In this case, the HashMap's key is the variable name, and the value is the variable wrapped in ConstantImpl.
    variables.insert("a".to_string(), res.unwrap().clone());
    // The client can also upload data from Rust to the server and generate a variable.
    client.upload(&variables).await.unwrap();
}
