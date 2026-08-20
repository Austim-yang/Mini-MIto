use std::{pin::Pin, sync::Arc, vec};

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    datasource::TableType,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
};

use datafusion::error::Result as DataFusionResult;

use crate::{
    memtable::memtable::Region,
    query::{predicate::extract_time_range, scan::LSMScanExec},
};

#[derive(Debug)]
pub struct LSMTableProvider {
    region: Arc<Region>,
    schema: SchemaRef,
}

impl LSMTableProvider {
    pub fn new(region: Region) -> Self {
        let schema = Arc::new(region.schema().arrow_schema());
        Self {
            region: Arc::new(region),
            schema,
        }
    }
}

impl TableProvider for LSMTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        _state: &'life1 dyn Session,
        projection: Option<&'life2 Vec<usize>>,
        filters: &'life3 [Expr],
        limit: Option<usize>,
    ) -> Pin<Box<dyn Future<Output = DataFusionResult<Arc<dyn ExecutionPlan>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        let region = self.region.clone();
        let schema = self.schema.clone();
        let projection = projection.cloned();
        let mut time_range = extract_time_range(filters, region.schema().time_index_name());
        if let Some(c) = region.ttl_cutoff() {
            time_range.min = Some(time_range.min.map_or(c, |m| m.max(c)));
        }

        Box::pin(async move {
            let exec = LSMScanExec::new(region, schema, projection, limit, time_range);
            let plan: Arc<dyn ExecutionPlan> = Arc::new(exec);
            DataFusionResult::Ok(plan)
        })
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}
