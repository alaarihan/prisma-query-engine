use query_engine::{ConstructorOptions, QueryEngine};
use std::{
  fs,
  collections::{BTreeMap},
  env,
};

#[tokio::main]
async fn main() {
  // Get prisma dir path.
  let mut config_dir = env::current_dir().unwrap();
  config_dir.push("prisma");
  
  // Get prisma schema file path.
  let mut prisma_path = config_dir.clone();
  prisma_path.push("schema.prisma");

  let datamodel = fs::read_to_string(prisma_path)
        .expect("Something went wrong reading prisma schema file");
        
  let opts = ConstructorOptions {
    datamodel,
    datasource_overrides: BTreeMap::new(),
    env: serde_json::from_str("{}").unwrap(),
    config_dir: config_dir,
    ignore_env_var_errors: true,
  };


  // Initialaize the Query Engine.
  let engine = QueryEngine::new(opts).unwrap();

  let query_body = String::from(r#"{ "query":"{ findManyPost { id title viewCount } }", "operationName":null, "variables":{} }"#);
  engine.connect().await.unwrap();
  let res = engine.query(query_body, None).await.unwrap();
  let res = serde_json::to_string(&res).unwrap();
  println!("{:#?}", res);
  engine.disconnect().await.unwrap();
}
