# colibri

This folder defines all source code for the colibri pipeline:

- `explorations/`: Ad-hoc notebooks used to explore the data processed by this pipeline.
- `transformations/`: All dataset definitions and transformations.
- `utilities/` (optional): Utility functions and Python modules used in this pipeline.
- `data_sources/` (optional): View definitions describing the source data for this pipeline.

## Getting Started

To get started, go to the `transformations` folder -- most of the relevant source code lives there:

* By convention, every dataset under `transformations` is in a separate file.
* Take a look at the sample called "sample_trips_colibri.py" to get familiar with the syntax.
  Read more about the syntax at https://docs.databricks.com/dlt/python-ref.html.
* If you're using the workspace UI, use `Run file` to run and preview a single transformation.
* If you're using the CLI, use `databricks bundle run colibri_etl --refresh sample_trips_colibri` to run a single transformation.

For more tutorials and reference material, see https://docs.databricks.com/dlt.

## Alternative design: Apache Spark Structured Streaming

This pipeline could instead be implemented directly with Apache Spark
Structured Streaming APIs. Lakeflow Declarative Pipelines (formerly DLT) builds
on Spark's streaming capabilities and adds declarative dataset definitions,
managed pipeline execution, and built-in data quality and incremental
processing features.

For transformations or processing routines that are difficult to express
within the pipeline framework, using Structured Streaming directly can provide
more control over the processing logic and runtime behavior. The trade-off is
greater development and operational effort: the application must explicitly
handle concerns such as checkpoints, recovery, schema changes, and monitoring
that the managed pipeline framework provides or simplifies.
