use std::{
    fmt::{Error, Formatter},
    sync::Arc,
    vec,
};

use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
    },
};

use crate::memtable::memtable::Region;
use crate::query::stream::LSMStream;

#[derive(Debug)]
pub struct LSMScanExec {
    region: Arc<Region>,
    schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
}

impl LSMScanExec {
    pub fn new(
        region: Arc<Region>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        let projected_schema = if let Some(proj) = &projection {
            let fields: Vec<_> = proj.iter().map(|&idx| schema.field(idx).clone()).collect();
            Arc::new(Schema::new(fields))
        } else {
            schema.clone()
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            region,
            schema,
            projected_schema,
            projection,
            limit,
            properties,
        }
    }
}

impl ExecutionPlan for LSMScanExec {
    fn name(&self) -> &str {
        "LSMScan"
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Plan("Only one partition supported".into()));
        }

        let stream = LSMStream::new(
            self.region.clone(),
            self.projected_schema.clone(),
            self.projection.clone(),
            self.limit,
        )?;
        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
}

impl Clone for LSMScanExec {
    fn clone(&self) -> Self {
        Self {
            region: self.region.clone(),
            schema: self.schema.clone(),
            projected_schema: self.projected_schema.clone(),
            projection: self.projection.clone(),
            limit: self.limit,
            properties: self.properties.clone(),
        }
    }
}

impl DisplayAs for LSMScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> Result<(), Error> {
        f.write_str("LSMScanExec")
    }
}
