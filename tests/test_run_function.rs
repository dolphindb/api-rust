mod setup;

use dolphindb::types::*;
use dolphindb::{
    client::ClientBuilder,
    types::{Bool, ConstantImpl, DataForm, DataType, ScalarImpl},
};

use setup::settings::Config;

#[tokio::test]
async fn test_run_function_params_0() {
    // connect
    let conf = Config::new();
    let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
    builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
    let mut client = builder.connect().await.unwrap();
    // prapare
    let _ = client
        .run_script(
            r#"
                def test_run_function_params_0(){
                    return true
                }
            "#,
        )
        .await;
    let args = Vec::<ConstantImpl>::new();
    let res = client
        .run_function("test_run_function_params_0", &args)
        .await;
    assert!(res.is_ok());
    let res_constantimpl = res.unwrap().unwrap();
    assert_eq!(res_constantimpl.data_form(), DataForm::Scalar);
    assert_eq!(res_constantimpl.data_type(), DataType::Bool);
    if let ConstantImpl::Scalar(ScalarImpl::Bool(res_)) = res_constantimpl {
        assert_eq!(res_, Bool::new(true));
    } else {
        assert!(false, "error in constant");
    }
}

#[tokio::test]
async fn test_run_function_params_1() {
    // connect
    let conf = Config::new();
    let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
    builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
    let mut client = builder.connect().await.unwrap();
    // prapare
    let _ = client
        .run_script(
            r#"
                def test_run_function_params_1(x){
                    return true
                }
            "#,
        )
        .await;
    let args: Vec<ConstantImpl> = vec![Bool::new(true).into()];
    let res = client
        .run_function("test_run_function_params_1", &args)
        .await;
    assert!(res.is_ok());
    let res_constantimpl = res.unwrap().unwrap();
    assert_eq!(res_constantimpl.data_form(), DataForm::Scalar);
    assert_eq!(res_constantimpl.data_type(), DataType::Bool);
    if let ConstantImpl::Scalar(ScalarImpl::Bool(res_)) = res_constantimpl {
        assert_eq!(res_, Bool::new(true));
    } else {
        assert!(false, "error in constant");
    }
}

#[tokio::test]
async fn test_run_function_params_2() {
    // connect
    let conf = Config::new();
    let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
    builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
    let mut client = builder.connect().await.unwrap();
    // prapare
    let _ = client
        .run_script(
            r#"
                def test_run_function_params_2(x,y){
                    return true
                }
            "#,
        )
        .await;
    let args = vec![Bool::new(true).into(), Bool::new(false).into()];
    let res = client
        .run_function("test_run_function_params_2", &args)
        .await;
    assert!(res.is_ok());
    let res_constantimpl = res.unwrap().unwrap();
    assert_eq!(res_constantimpl.data_form(), DataForm::Scalar);
    assert_eq!(res_constantimpl.data_type(), DataType::Bool);
    if let ConstantImpl::Scalar(ScalarImpl::Bool(res_)) = res_constantimpl {
        assert_eq!(res_, Bool::new(true));
    } else {
        assert!(false, "error in constant");
    }
}

#[tokio::test]
async fn test_run_function_part_apply() {
    // connect
    let conf = Config::new();
    let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
    builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
    let mut client = builder.connect().await.unwrap();
    // prapare
    let _ = client
        .run_script(
            r#"
                def test_run_function_part_apply(x,y){
                    return true
                }
            "#,
        )
        .await;
    let args = vec![Bool::new(true).into()];
    let res = client
        .run_function("test_run_function_part_apply{0}", &args)
        .await;
    assert!(res.is_ok());
    let res_constantimpl = res.unwrap().unwrap();
    assert_eq!(res_constantimpl.data_form(), DataForm::Scalar);
    assert_eq!(res_constantimpl.data_type(), DataType::Bool);
    if let ConstantImpl::Scalar(ScalarImpl::Bool(res_)) = res_constantimpl {
        assert_eq!(res_, Bool::new(true));
    } else {
        assert!(false, "error in constant");
    }
}

#[tokio::test]
async fn test_run_function_error() {
    // connect
    let conf = Config::new();
    let mut builder = ClientBuilder::new(format!("{}:{}", conf.host, conf.port));
    builder.with_auth((conf.user.as_str(), conf.passwd.as_str()));
    let mut client = builder.connect().await.unwrap();
    // prapare
    let _ = client
        .run_script(
            r#"
                def test_run_function_error(x){
                    throw "error"
                }
            "#,
        )
        .await;
    let args = vec![Bool::new(true).into()];
    let res = client.run_function("test_run_function_error", &args).await;
    assert!(!res.is_ok());
}
