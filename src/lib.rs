mod error;

use crate::error::ApiError;
use datamodel::{dml::Datamodel, ValidatedConfiguration};
use prisma_models::InternalDataModelBuilder;
use query_core::{
    executor, schema_builder, BuildMode, QueryExecutor, QuerySchema, QuerySchemaRenderer, TxId,
};
use request_handlers::{GraphQLSchemaRenderer, GraphQlHandler, TxInput};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::RwLock;

/// The main engine, that can be cloned between threads.
pub struct QueryEngine {
    inner: RwLock<Inner>,
}

/// The state of the engine.
pub enum Inner {
    /// Not connected, holding all data to form a connection.
    Builder(EngineBuilder),
    /// A connected engine, holding all data to disconnect and form a new
    /// connection. Allows querying when on this state.
    Connected(ConnectedEngine),
}

/// Holding the information to reconnect the engine if needed.
#[derive(Debug, Clone)]
struct EngineDatamodel {
    ast: Datamodel,
    raw: String,
}

/// Everything needed to connect to the database and have the core running.
pub struct EngineBuilder {
    datamodel: EngineDatamodel,
    config: ValidatedConfiguration,
    config_dir: PathBuf,
    env: HashMap<String, String>,
}

/// Internal structure for querying and reconnecting with the engine.
pub struct ConnectedEngine {
    datamodel: EngineDatamodel,
    query_schema: Arc<QuerySchema>,
    executor: crate::Executor,
    config_dir: PathBuf,
    env: HashMap<String, String>,
}

impl ConnectedEngine {
    /// The schema AST for Query Engine core.
    pub fn query_schema(&self) -> &Arc<QuerySchema> {
        &self.query_schema
    }

    /// The query executor.
    pub fn executor(&self) -> &(dyn QueryExecutor + Send + Sync) {
        &*self.executor
    }
}

/// Parameters defining the construction of an engine.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConstructorOptions {
    pub datamodel: String,
    #[serde(default)]
    pub datasource_overrides: BTreeMap<String, String>,
    #[serde(default)]
    pub env: serde_json::Value,
    #[serde(default)]
    pub config_dir: PathBuf,
    #[serde(default)]
    pub ignore_env_var_errors: bool,
}

impl Inner {
    /// Returns a builder if the engine is not connected
    fn as_builder(&self) -> crate::Result<&EngineBuilder> {
        match self {
            Inner::Builder(ref builder) => Ok(builder),
            Inner::Connected(_) => Err(ApiError::AlreadyConnected),
        }
    }

    /// Returns the engine if connected
    fn as_engine(&self) -> crate::Result<&ConnectedEngine> {
        match self {
            Inner::Builder(_) => Err(ApiError::NotConnected),
            Inner::Connected(ref engine) => Ok(engine),
        }
    }
}

impl QueryEngine {
    /// Parse a validated datamodel and configuration to allow connecting later on.
    pub fn new(opts: ConstructorOptions) -> crate::Result<Self> {
        let ConstructorOptions {
            datamodel,
            datasource_overrides,
            env,
            config_dir,
            ignore_env_var_errors,
        } = opts;
        let overrides: Vec<(_, _)> = datasource_overrides.into_iter().collect();
        let env = stringify_env_values(env)?;

        let config = if ignore_env_var_errors {
            datamodel::parse_configuration(&datamodel)
                .map_err(|errors| ApiError::conversion(errors, &datamodel))?
        } else {
            datamodel::parse_configuration(&datamodel)
                .and_then(|mut config| {
                    config
                        .subject
                        .resolve_datasource_urls_from_env(&overrides, |key| {
                            env.get(key).map(ToString::to_string)
                        })?;

                    Ok(config)
                })
                .map_err(|errors| ApiError::conversion(errors, &datamodel))?
        };

        config
            .subject
            .validate_that_one_datasource_is_provided()
            .map_err(|errors| ApiError::conversion(errors, &datamodel))?;

        let ast = datamodel::parse_datamodel(&datamodel)
            .map_err(|errors| ApiError::conversion(errors, &datamodel))?
            .subject;

        let datamodel = EngineDatamodel {
            ast,
            raw: datamodel,
        };

        let builder = EngineBuilder {
            datamodel,
            config,
            config_dir,
            env,
        };

        Ok(Self {
            inner: RwLock::new(Inner::Builder(builder)),
        })
    }

    /// Connect to the database, allow queries to be run.
    pub async fn connect(&self) -> crate::Result<()> {
        let mut inner = self.inner.write().await;
        let builder = inner.as_builder()?;

        let engine = async move {
            // We only support one data source & generator at the moment, so take the first one (default not exposed yet).
            let data_source = builder
                .config
                .subject
                .datasources
                .first()
                .ok_or_else(|| ApiError::configuration("No valid data source found"))?;

            let preview_features: Vec<_> =
                builder.config.subject.preview_features().iter().collect();
            let url = data_source
                .load_url_with_config_dir(&builder.config_dir, |key| {
                    builder.env.get(key).map(ToString::to_string)
                })
                .map_err(|err| {
                    crate::error::ApiError::Conversion(err, builder.datamodel.raw.clone())
                })?;

            let (db_name, executor) = executor::load(data_source, &preview_features, &url).await?;
            let connector = executor.primary_connector();
            connector.get_connection().await?;

            // Build internal data model
            let internal_data_model =
                InternalDataModelBuilder::from(&builder.datamodel.ast).build(db_name);

            let query_schema = schema_builder::build(
                internal_data_model,
                BuildMode::Modern,
                true, // enable raw queries
                data_source.capabilities(),
                preview_features,
                data_source.referential_integrity(),
            );

            Ok(ConnectedEngine {
                datamodel: builder.datamodel.clone(),
                query_schema: Arc::new(query_schema),
                executor,
                config_dir: builder.config_dir.clone(),
                env: builder.env.clone(),
            }) as crate::Result<ConnectedEngine>
        }
        .await?;

        *inner = Inner::Connected(engine);

        Ok(())
    }

    /// Disconnect and drop the core. Can be reconnected later with `#connect`.
    pub async fn disconnect(&self) -> crate::Result<()> {
        let mut inner = self.inner.write().await;
        let engine = inner.as_engine()?;

        let config = datamodel::parse_configuration(&engine.datamodel.raw)
            .map_err(|errors| ApiError::conversion(errors, &engine.datamodel.raw))?;

        let builder = EngineBuilder {
            datamodel: engine.datamodel.clone(),
            config,
            config_dir: engine.config_dir.clone(),
            env: engine.env.clone(),
        };

        *inner = Inner::Builder(builder);

        Ok(())
    }

    /// If connected, sends a query to the core and returns the response.
    pub async fn query(&self, body: String, tx_id: Option<String>) -> crate::Result<String> {
        let inner = self.inner.read().await;
        let engine = inner.as_engine()?;

        let query = serde_json::from_str(&body)?;
        async move {
            let handler = GraphQlHandler::new(engine.executor(), engine.query_schema());

            let response = handler.handle(query, tx_id.map(TxId::from), None).await;

            Ok(serde_json::to_string(&response)?)
        }
        .await
    }

    /// If connected, attempts to start a transaction in the core and returns its ID.
    pub async fn start_transaction(&self, input: String) -> crate::Result<String> {
        let inner = self.inner.read().await;
        let engine = inner.as_engine()?;

        let input: TxInput = serde_json::from_str(&input)?;

        async move {
            match engine
                .executor()
                .start_tx(engine.query_schema().clone(), input.max_wait, input.timeout)
                .await
            {
                Ok(tx_id) => Ok(json!({ "id": tx_id.to_string() }).to_string()),
                Err(err) => Ok(map_known_error(err)?),
            }
        }
        .await
    }

    /// If connected, attempts to commit a transaction with id `tx_id` in the core.
    pub async fn commit_transaction(
        &self,
        tx_id: String,
    ) -> crate::Result<String> {
        let inner = self.inner.read().await;
        let engine = inner.as_engine()?;

        async move {
            match engine.executor().commit_tx(TxId::from(tx_id)).await {
                Ok(_) => Ok("{}".to_string()),
                Err(err) => Ok(map_known_error(err)?),
            }
        }
        .await
    }

    /// If connected, attempts to roll back a transaction with id `tx_id` in the core.
    pub async fn rollback_transaction(&self, tx_id: String) -> crate::Result<String> {
        let inner = self.inner.read().await;
        let engine = inner.as_engine()?;

        async move {
            match engine.executor().rollback_tx(TxId::from(tx_id)).await {
                Ok(_) => Ok("{}".to_string()),
                Err(err) => Ok(map_known_error(err)?),
            }
        }
        .await
    }

    /// Loads the query schema. Only available when connected.
    pub async fn sdl_schema(&self) -> crate::Result<String> {
        let inner = self.inner.read().await;
        let engine = inner.as_engine()?;

        Ok(GraphQLSchemaRenderer::render(engine.query_schema().clone()))
    }
}

fn stringify_env_values(origin: serde_json::Value) -> crate::Result<HashMap<String, String>> {
    use serde_json::Value;

    let msg = match origin {
        Value::Object(map) => {
            let mut result: HashMap<String, String> = HashMap::new();

            for (key, val) in map.into_iter() {
                match val {
                    Value::Null => continue,
                    Value::String(val) => {
                        result.insert(key, val);
                    }
                    val => {
                        result.insert(key, val.to_string());
                    }
                }
            }

            return Ok(result);
        }
        Value::Null => return Ok(Default::default()),
        Value::Bool(_) => "Expected an object for the env constructor parameter, got a boolean.",
        Value::Number(_) => "Expected an object for the env constructor parameter, got a number.",
        Value::String(_) => "Expected an object for the env constructor parameter, got a string.",
        Value::Array(_) => "Expected an object for the env constructor parameter, got an array.",
    };

    Err(ApiError::JsonDecode(msg.to_string()))
}

fn map_known_error(err: query_core::CoreError) -> crate::Result<String> {
    let user_error: user_facing_errors::Error = err.into();
    let value = serde_json::to_string(&user_error)?;

    Ok(value)
}

pub(crate) type Result<T> = std::result::Result<T, error::ApiError>;
pub(crate) type Executor = Box<dyn query_core::QueryExecutor + Send + Sync>;
