mod error;

use crate::error::ApiError;
use datamodel::{diagnostics::ValidatedConfiguration, Datamodel};
use prisma_models::DatamodelConverter;
use query_core::{
    executor, schema_builder, BuildMode, QueryExecutor, QuerySchema, QuerySchemaRenderer, TxId,
};
use request_handlers::{
    GraphQLSchemaRenderer, GraphQlBody, GraphQlHandler, PrismaResponse, TxInput,
};
use serde::Deserialize;
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::RwLock;

/// The main engine, that can be cloned between threads.
#[derive(Clone)]
pub struct QueryEngine {
    inner: Arc<RwLock<Inner>>,
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
    datasource_overrides: Vec<(String, String)>,
    ast: Datamodel,
    raw: String,
}

/// Everything needed to connect to the database and have the core running.
pub struct EngineBuilder {
    datamodel: EngineDatamodel,
    config: ValidatedConfiguration,
    env: HashMap<String, String>,
}

/// Internal structure for querying and reconnecting with the engine.
pub struct ConnectedEngine {
    datamodel: EngineDatamodel,
    query_schema: Arc<QuerySchema>,
    executor: crate::Executor,
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
    pub datamodel_str: Option<String>,
    pub datamodel_path: Option<PathBuf>,
    #[serde(default)]
    pub datasource_overrides: BTreeMap<String, String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
}

impl QueryEngine {
    /// Parse a validated datamodel and configuration to allow connecting later on.
    pub fn new(opts: ConstructorOptions) -> crate::Result<Self> {
        let ConstructorOptions {
            datamodel_str,
            datamodel_path,
            datasource_overrides,
            env,
        } = opts;
        let datamodel: String = datamodel_str.unwrap_or(
            fs::read_to_string(PathBuf::from(datamodel_path.unwrap()))
                .expect("failed to read prisma datamodel file"),
        );
        let overrides: Vec<(_, _)> = datasource_overrides.into_iter().collect();

        let config = datamodel::parse_configuration(&datamodel)
            .map_err(|errors| ApiError::conversion(errors, &datamodel))?;

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
            datasource_overrides: overrides,
        };

        let builder = EngineBuilder {
            datamodel,
            config,
            env,
        };

        Ok(Self {
            inner: Arc::new(RwLock::new(Inner::Builder(builder))),
        })
    }

    /// Connect to the database, allow queries to be run.
    pub async fn connect(&self) -> crate::Result<()> {
        let mut inner = self.inner.write().await;

        match *inner {
            Inner::Builder(ref builder) => {
                let template = DatamodelConverter::convert(&builder.datamodel.ast);

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
                    .load_url(|key| builder.env.get(key).map(ToString::to_string))
                    .map_err(|err| {
                        crate::error::ApiError::Conversion(err, builder.datamodel.raw.clone())
                    })?;

                let (db_name, executor) =
                    executor::load(data_source, &preview_features, &url).await?;
                let connector = executor.primary_connector();
                connector.get_connection().await?;

                // Build internal data model
                let internal_data_model = template.build(db_name);

                let query_schema = schema_builder::build(
                    internal_data_model,
                    BuildMode::Modern,
                    true, // enable raw queries
                    data_source.capabilities(),
                    preview_features,
                );
                let engine = ConnectedEngine {
                    datamodel: builder.datamodel.clone(),
                    query_schema: Arc::new(query_schema),
                    executor,
                    env: builder.env.clone(),
                };
                *inner = Inner::Connected(engine);

                Ok(())
            }
            Inner::Connected(_) => Err(ApiError::AlreadyConnected),
        }
    }

    /// Disconnect and drop the core. Can be reconnected later with `#connect`.
    pub async fn disconnect(&self) -> crate::Result<()> {
        let mut inner = self.inner.write().await;

        match *inner {
            Inner::Connected(ref engine) => {
                let config = datamodel::parse_configuration(&engine.datamodel.raw)
                    .map_err(|errors| ApiError::conversion(errors, &engine.datamodel.raw))?;

                let builder = EngineBuilder {
                    datamodel: engine.datamodel.clone(),
                    config,
                    env: engine.env.clone(),
                };

                *inner = Inner::Builder(builder);

                Ok(())
            }
            Inner::Builder(_) => Err(ApiError::NotConnected),
        }
    }

    /// If connected, sends a query to the core and returns the response.
    pub async fn query(
        &self,
        query: GraphQlBody,
        tx_id: Option<String>,
    ) -> crate::Result<PrismaResponse> {
        match *self.inner.read().await {
            Inner::Connected(ref engine) => {
                let handler = GraphQlHandler::new(engine.executor(), engine.query_schema());
                Ok(handler.handle(query, tx_id.map(TxId::from)).await)
            }
            Inner::Builder(_) => Err(ApiError::NotConnected),
        }
    }

    /// If connected, attempts to start a transaction in the core and returns its ID.
    pub async fn start_tx(&self, input: TxInput) -> crate::Result<String> {
        match *self.inner.read().await {
            Inner::Connected(ref engine) => {
                match engine
                    .executor()
                    .start_tx(input.max_wait, input.timeout)
                    .await
                {
                    Ok(tx_id) => Ok(json!({ "id": tx_id.to_string() }).to_string()),
                    Err(err) => Ok(map_known_error(err)?),
                }
            }
            Inner::Builder(_) => Err(ApiError::NotConnected),
        }
    }

    /// If connected, attempts to commit a transaction with id `tx_id` in the core.
    pub async fn commit_tx(&self, tx_id: String) -> crate::Result<String> {
        match *self.inner.read().await {
            Inner::Connected(ref engine) => {
                match engine.executor().commit_tx(TxId::from(tx_id)).await {
                    Ok(_) => Ok("{}".to_string()),
                    Err(err) => Ok(map_known_error(err)?),
                }
            }
            Inner::Builder(_) => Err(ApiError::NotConnected),
        }
    }

    /// If connected, attempts to roll back a transaction with id `tx_id` in the core.
    pub async fn rollback_tx(&self, tx_id: String) -> crate::Result<String> {
        match *self.inner.read().await {
            Inner::Connected(ref engine) => {
                match engine.executor().rollback_tx(TxId::from(tx_id)).await {
                    Ok(_) => Ok("{}".to_string()),
                    Err(err) => Ok(map_known_error(err)?),
                }
            }
            Inner::Builder(_) => Err(ApiError::NotConnected),
        }
    }

    /// Loads the query schema. Only available when connected.
    pub async fn sdl_schema(&self) -> crate::Result<String> {
        match *self.inner.read().await {
            Inner::Connected(ref engine) => {
                Ok(GraphQLSchemaRenderer::render(engine.query_schema().clone()))
            }
            Inner::Builder(_) => Err(ApiError::NotConnected),
        }
    }
}

fn map_known_error(err: query_core::CoreError) -> crate::Result<String> {
    let user_error: user_facing_errors::Error = err.into();
    let value = serde_json::to_string(&user_error)?;

    Ok(value)
}

pub(crate) type Result<T> = std::result::Result<T, error::ApiError>;
pub(crate) type Executor = Box<dyn query_core::QueryExecutor + Send + Sync>;
