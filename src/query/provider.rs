use std::{pin::Pin, sync::Arc, vec};

use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{Session, TableProvider},
    datasource::TableType,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
};

use datafusion::error::Result as DataFusionResult;

use crate::{memtable::memtable::MemtableManager, query::scan::LSMScanExec};

#[derive(Debug)]
pub struct LSMTableProvider {
    memtable_manager: Arc<MemtableManager>,
    schema: SchemaRef,
}

impl LSMTableProvider {
    pub fn new(memtable_manager: MemtableManager) -> Self {
        let schema = Arc::new(memtable_manager.schema().arrow_schema());
        Self {
            memtable_manager: Arc::new(memtable_manager),
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
        state: &'life1 dyn Session,
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
        let memtable = self.memtable_manager.clone();
        let schema = self.schema.clone();
        let projection = projection.cloned();

        Box::pin(async move {
            let exec = LSMScanExec::new(memtable, schema, projection, limit);
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
