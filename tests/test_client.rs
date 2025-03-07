mod setup;

use dolphindb::{
    client::ClientBuilder,
    types::{ConstantImpl, Int, VectorImpl},
    BehaviorOptions,
};
use rstest::rstest;
use setup::settings::Config;

mod test_client_client_builder {
    use super::*;

    #[tokio::test]
    async fn test_client_client_builder_addr_error() {
        let builder = ClientBuilder::new("192.168.0.54:12345");
        let client = builder.connect().await;
        assert!(client.is_err())
    }

    #[tokio::test]
    async fn test_client_client_builder_user_error() {
        let conf = Config::new();
        let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        builder.with_auth(("tmp", "123456"));
        let client = builder.connect().await;
        assert!(client.is_err())
    }

    #[tokio::test]
    #[rstest]
    #[case::priority_0(0, 0)]
    #[case::priority_4(4, 4)]
    #[case::priority_9(9, 8)]
    async fn test_client_client_builder_with_option_priority(
        #[case] priority: i32,
        #[case] _expect: i32,
    ) {
        let conf = Config::new();
        let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        let mut option = BehaviorOptions::default();
        option.with_priority(priority);
        builder.with_option(option);
        let mut client = builder.connect().await.unwrap();
        let res = client
            .run_script(
                "exec priority from getConsoleJobs() where sessionId=getCurrentSessionAndUser()[0]",
            )
            .await
            .unwrap()
            .unwrap();
        if let ConstantImpl::Vector(VectorImpl::Int(res_vec)) = res {
            assert_eq!(*(res_vec.get(0).unwrap()), Int::new(_expect));
        } else {
            assert!(false, "return error")
        }
    }

    #[tokio::test]
    #[rstest]
    #[case::parallelism_10(10, 10)]
    #[case::parallelism_64(64, 64)]
    #[case::parallelism_1(1, 1)]
    async fn test_client_client_builder_with_option_parallelism(
        #[case] parallelism: i32,
        #[case] _expect: i32,
    ) {
        let conf = Config::new();
        let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        let mut option = BehaviorOptions::default();
        option.with_parallelism(parallelism);
        builder.with_option(option);
        let mut client = builder.connect().await.unwrap();
        let res = client.run_script("exec parallelism from getConsoleJobs() where sessionId=getCurrentSessionAndUser()[0]").await.unwrap().unwrap();
        if let ConstantImpl::Vector(VectorImpl::Int(res_vec)) = res {
            assert_eq!(*(res_vec.get(0).unwrap()), Int::new(_expect));
        } else {
            assert!(false, "return error")
        }
    }

    // todo:RUS-40
    #[tokio::test]
    async fn test_client_client_builder_with_fetch_size() {
        let conf = Config::new();
        let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        let mut option = BehaviorOptions::default();
        option.with_fetch_size(8192);
        builder.with_option(option);
        let _client = builder.connect().await.unwrap();
    }
}

mod test_client_client {
    use super::*;

    #[tokio::test]
    #[rstest]
    #[case::priority_0(0, 0)]
    #[case::priority_4(4, 4)]
    #[case::priority_9(9, 8)]
    async fn test_client_client_run_script_with_option_priority(
        #[case] priority: i32,
        #[case] _expect: i32,
    ) {
        let conf = Config::new();
        let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        let mut client = builder.connect().await.unwrap();
        let mut option = BehaviorOptions::default();
        option.with_priority(priority);
        let res = client
            .run_script_with_option(
                "exec priority from getConsoleJobs() where sessionId=getCurrentSessionAndUser()[0]",
                &option,
            )
            .await
            .unwrap()
            .unwrap();
        if let ConstantImpl::Vector(VectorImpl::Int(res_vec)) = res {
            assert_eq!(*(res_vec.get(0).unwrap()), Int::new(_expect));
        } else {
            assert!(false, "return error")
        }
    }

    #[tokio::test]
    async fn test_client_client_run_script_with_option_priority_default() {
        let conf = Config::new();
        let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        let mut client = builder.connect().await.unwrap();
        let option = BehaviorOptions::default();
        let res = client
            .run_script_with_option(
                "exec priority from getConsoleJobs() where sessionId=getCurrentSessionAndUser()[0]",
                &option,
            )
            .await
            .unwrap()
            .unwrap();
        if let ConstantImpl::Vector(VectorImpl::Int(res_vec)) = res {
            assert_eq!(*(res_vec.get(0).unwrap()), Int::new(4));
        } else {
            assert!(false, "return error")
        }
    }

    #[tokio::test]
    #[rstest]
    #[case::parallelism_10(10, 10)]
    #[case::parallelism_64(64, 64)]
    #[case::parallelism_1(1, 1)]
    async fn test_client_client_run_script_with_option_parallelism(
        #[case] parallelism: i32,
        #[case] _expect: i32,
    ) {
        let conf = Config::new();
        let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        let mut client = builder.connect().await.unwrap();
        let mut option = BehaviorOptions::default();
        option.with_parallelism(parallelism);
        let res = client.run_script_with_option("exec parallelism from getConsoleJobs() where sessionId=getCurrentSessionAndUser()[0]", &option).await.unwrap().unwrap();
        if let ConstantImpl::Vector(VectorImpl::Int(res_vec)) = res {
            assert_eq!(*(res_vec.get(0).unwrap()), Int::new(_expect));
        } else {
            assert!(false, "return error")
        }
    }

    #[tokio::test]
    async fn test_client_client_run_script_with_option_parallelism_default() {
        let conf = Config::new();
        let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        let mut client = builder.connect().await.unwrap();
        let option = BehaviorOptions::default();
        let res = client.run_script_with_option("exec parallelism from getConsoleJobs() where sessionId=getCurrentSessionAndUser()[0]", &option).await.unwrap().unwrap();
        if let ConstantImpl::Vector(VectorImpl::Int(res_vec)) = res {
            assert_eq!(*(res_vec.get(0).unwrap()), Int::new(64));
        } else {
            assert!(false, "return error")
        }
    }

    #[tokio::test]
    #[rstest]
    #[case::priority_0(0, 0)]
    #[case::priority_4(4, 4)]
    #[case::priority_9(9, 8)]
    async fn test_client_client_run_function_with_option_priority(
        #[case] priority: i32,
        #[case] _expect: i32,
    ) {
        let conf = Config::new();
        let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        let mut client = builder.connect().await.unwrap();
        let mut option = BehaviorOptions::default();
        option.with_priority(priority);
        let _ = client.run_script(r#"
            def get_priority(){
                return exec priority from getConsoleJobs() where sessionId=getCurrentSessionAndUser()[0]
            }
        "#).await;
        let args = Vec::<ConstantImpl>::new();
        let res = client
            .run_function_with_option("get_priority", &args, &option)
            .await
            .unwrap()
            .unwrap();
        if let ConstantImpl::Vector(VectorImpl::Int(res_vec)) = res {
            assert_eq!(*(res_vec.get(0).unwrap()), Int::new(_expect));
        } else {
            assert!(false, "return error")
        }
    }

    #[tokio::test]
    #[rstest]
    #[case::parallelism_10(10, 10)]
    #[case::parallelism_64(64, 64)]
    #[case::parallelism_1(1, 1)]
    async fn test_client_client_run_function_with_option_parallelism(
        #[case] parallelism: i32,
        #[case] _expect: i32,
    ) {
        let conf = Config::new();
        let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        let mut client = builder.connect().await.unwrap();
        let mut option = BehaviorOptions::default();
        option.with_parallelism(parallelism);
        let _ = client.run_script(r#"
            def get_parallelism(){
                return exec parallelism from getConsoleJobs() where sessionId=getCurrentSessionAndUser()[0]
            }
        "#).await;
        let args = Vec::<ConstantImpl>::new();
        let res = client
            .run_function_with_option("get_parallelism", &args, &option)
            .await
            .unwrap()
            .unwrap();
        if let ConstantImpl::Vector(VectorImpl::Int(res_vec)) = res {
            assert_eq!(*(res_vec.get(0).unwrap()), Int::new(_expect));
        } else {
            assert!(false, "return error")
        }
    }

    // #[tokio::test]
    // #[rstest]
    // #[case::fetch_size_10(8222, 10)]
    // async fn test_client_client_run_script_with_option_fetch_size(
    //     #[case] fetch_size: i32,
    //     #[case] _expect: i32,
    // ) {
    //     let conf = Config::new();
    //     let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
    //     builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
    //     let mut client = builder.connect().await.unwrap();
    //     let mut option = BehaviorOptions::default();
    //     option.with_fetch_size(fetch_size);
    //     let res = client.run_script_with_option("table(1..10000 as id)", option).await.unwrap().unwrap();
    //     println!("{}",res);
    //     // if let ConstantImpl::Vector(VectorImpl::Int(res_vec)) = res {
    //     //     assert_eq!(*(res_vec.get(0).unwrap()), Int::new(_expect));
    //     // } else {
    //     //     assert!(false, "return error")
    //     // }
    // }

    #[tokio::test]
    async fn test_client_client_local_addr() {
        let conf = Config::new();
        let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
        builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
        let mut client = builder.connect().await.unwrap();
        let socket_addr = client.local_addr();
        let res=client.run_script("exec string(remoteIp)+\":\"+string(remotePort) from getConsoleJobs() where sessionId=getCurrentSessionAndUser()[0]").await.unwrap().unwrap();
        if let ConstantImpl::Vector(VectorImpl::String(res_vec)) = res {
            assert_eq!(socket_addr.to_string(), res_vec.get(0).unwrap().to_string());
        } else {
            assert!(false, "return error")
        }
    }
}
