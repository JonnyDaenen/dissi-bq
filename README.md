# Part 2 - JSON to BigQuery

This application loads JSON data from a storage location (e.g. GCS)
into BigQuery according to a common schema. The schema is auto-inferred
based on the data, and is used to keep the target table up to date.

See `doc` folder for a diagram and an Dataflow component overview.

## Prerequisites

- Bash terminal (for example scripts)
- Python 3.8
- gcloud command installed locally
- Google Cloud project of which you have sufficient rights + Dataflow API enabled

## Quickstart

Create your own venv:
```sh
bash scripts/001_create_venv.sh
```

Run the tests:
```sh
bash scripts/002_run_tests.sh
```

Create resources on your _current active project_:
```sh
bash scripts/003_provision_resources.sh
```

> Note that currently, this scripts generates a service account
with owner permissions. Please adjust these to suit your needs.
We recommend employing a least privilege setup.

Test the pipeline locally
```sh
bash scripts/004_run_pipeline_locally.sh
```

In order to test schema updates, you can uncomment the other
pipelines in script `004` to test the `base` and `modified` sample datasets.

Run the pipeline on Dataflow
```sh
bash scripts/005_run_pipeline_gcp.sh
```

Finally, scripts `008`, `009` and `010` allow you to test with some larger datasets,
which can make the Dataflow pipeline scale up.

## Assumptions

- Pipeline can run in batch mode
- Dataflow api is enabled
- Dataset exists (not created by pipeline so we have freedom to e.g. terraform and assign IAM policies)
- Nested fields don't need to be nullable as BQ can handle that when parent is nullable
- Null fields can be ignored


## Core Features

- [x] Input pattern support
- [x] Basic Primitive JSON types
- [x] TIMESTAMP data detector
- [x] INTEGER to FLOAT coercion
- [x] REQUIRED to NULLABLE conversion
- [x] Table schema updates
- [x] Optional data loading in the same job
- [x] Ignore null fields (until another doc has a value)


## Known limitations
- JSON null fields and empty lists are only supported if they appear as non-null in at least one doc
- nested lists are not supported


## Future work

Some interesting ideas for the future:

- [ ] extended primitive types (e.g., geolocation)
- [ ] schema depth/width validation
- [ ] validation of values according to schema in step 2 
- [ ] lineage tracking
- [ ] handle invalid inputs
- [ ] list with null
- [ ] create SA with least privilege
- [ ] override/seed schema
- [ ] forbidden characters in field names
- [ ] refactor into class structure
- [ ] make data detectors modular
- [ ] auto-casting during data load
- [ ] add counters in dataflow job (e.g. for validation)
- [ ] catch BQ ingestion errors
- [ ] error tolerance and dead-lettering
- [ ] BigQuery integration tests

The source code contains some pointers to future work by using the `FUTURE` tag in the comments.
