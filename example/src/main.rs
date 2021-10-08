use query_engine::{ConstructorOptions, QueryEngine};
use request_handlers::{GraphQlBody, SingleQuery};
use std::{
  collections::{BTreeMap, HashMap},
  env,
};

#[tokio::main]
async fn main() {
  // Get prisma schema file path.
  let mut prisma_path = env::current_dir().unwrap();
  prisma_path.push("prisma");
  prisma_path.push("schema.prisma");

  let opts = ConstructorOptions {
    datasource_overrides: BTreeMap::new(),
    env: HashMap::new(),
    datamodel_path: Some(prisma_path),
    datamodel_str: None,
  };

  // Initialaize the Query Engine.
  let engine = QueryEngine::new(opts).unwrap();

  let query = SingleQuery::from("{ findManyPost { id title viewCount }}");
  let query_body = GraphQlBody::Single(query);
  engine.connect().await.unwrap();
  let res = engine.query(query_body, None).await;
  println!("{:#?}", res);
  engine.disconnect().await.unwrap();
}
