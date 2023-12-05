<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Welcome to Apache Beam Notebooks!

[Apache Beam](https://beam.apache.org/) is an open source, unified model for defining both batch and streaming data-parallel processing pipelines.

These notebooks assume you have basic knowledge of
[notebooks](https://jupyterlab.readthedocs.io/en/stable/user/notebook.html) and
the [Python programming language](https://python.org). They are designed to help
you learn the Python SDK of Apache Beam. You can build, iteratively augment your
Apache Beam pipelines, and explore the data in PCollections while doing so.

*   [Example 1: Word Count](Examples/01-Word_Count.ipynb) demonstrates a simple
    batch pipeline that counts words from a text file.
*   [Example 2: Streaming Word Count](Examples/02-Streaming_Word_Count.ipynb)
    demonstrates a simple streaming pipeline that counts words from a stream.
*   [Example 3: Streaming NYC Taxi Ride Data](Examples/03-Streaming_NYC_Taxi_Ride_Data.ipynb)
    demonstrates a streaming pipeline that processes a taxi ride data stream.

We also have:

*   A set of [tutorials](Tutorials/0_START_HERE.md) that go through the basic
    operations of Apache Beam with exercises.
*   A [notebook](Examples/Dataflow_Word_Count.ipynb) that shows you how to
    launch a Dataflow job based on
    [Example 1: Word Count](Examples/01-Word_Count.ipynb).
*   A [notebook](Examples/Visualize_Data.ipynb) that shows you how to visualize
    collected PCollection data with native Interactive Beam visualization and
    various third party visualization libraries.
*   A [notebook](Examples/Use_GPUs_with_Apache_Beam.ipynb) that shows you how to
    write a Beam pipeline and run it with GPUs locally and on Dataflow.
    **Note**: you need to
    [create](https://pantheon.corp.google.com/dataflow/notebooks/list/instances)
    your Dataflow Notebooks instance `With 1 NVIDIA Tesla T4` and check the
    option to `Install NVIDIA GPU driver automatically for me` for this example
    to work.
*   A [notebook](Examples/Apache_Beam_SQL_in_notebooks.ipynb) that shows you how
    to write Beam pipeline with Beam SQL, use `beam_sql` magic to iterate your
    Beam SQL development quickly in notebooks, and launch Dataflow jobs with
    Beam SQL from notebooks.
*   A [notebook](Examples/Interactive_Flink_at_Scale.ipynb) that shows you how
    to run Beam pipelines with the FlinkRunner automagically hosted on a
    notebook-managed [Cloud Dataproc](https://cloud.google.com/dataproc) cluster
    that is provisioned distributedly instead of working with limited resources
    on the notebook instance itself.
*   A [notebook](Examples/RunInference.ipynb) that shows you how to use the
    `RunInference` transform to make machine learning inferences on simple data
    using PyTorch and Scikit-learn models.

If you have issues using the notebook, check these
[frequently asked questions](faq.md).

If you have any feedback on these notebooks, drop us a line at
beam-notebooks-feedback@google.com.

# Changelog

## 2023-11

- New kernel with Beam v2.52.0
- Removed deprecated version v2.43.0
- Removing support for versions older than v2.50.0, from this point onwards only
  the latest 3 Beam versions will be supported, this decision has been taken to
  improve the startup times of the container.

## 2023-10

- New kernel with Beam v2.51.0
- Removed deprecated version v2.42.0

## 2023-09

- New kernel with Beam v2.50.0
- Removed deprecated version v2.41.0

## 2023-07

- New kernel with Beam v2.49.0
- Removed deprecated versions v2.39.0 and v2.40.0

## 2023-06

-  New kernel with Beam v2.48.0

## 2023-05

-  New kernel with Beam v2.47.0
-  Remove deprecated kernel with beam v2.38.0

## 2023-02

### Added

-   New kernel with Beam v2.46.0
-   Remove the deprecated kernel with Beam v2.37.0

## 2023-02

### Added

-   New kernel with Beam v2.45.0

## 2023-01

### Added

-   New kernel with Beam v2.44.0

## 2022-11

### Added

-   New kernel with Beam v2.43.0
-   Split the Beam SQL notebook example into three notebooks
-   Patched Beam v2.43.0 with [the DataFrames fix](https://github.com/apache/beam/pull/24259)

## 2022-10 (2)

### Added

-   New kernel with Beam v2.42.0
-   When using `DataflowRunner` with the interactive mode, the system flags must
be cleared out with `flags={}` defined in `PipelineOptions`.
Some notebooks are updated with these changes.

## 2022-10

### Added

-   New kernel with Beam v2.41.0
-   Patched an upgrade of Flink on Dataproc support to kernels with Beam v2.40.0
    and v2.41.0
    - Supported high parallelism through `FlinkRunnerOptions.parallelism`
    - Supported custom container on Cloud Container Registry
-   Rewrote the interactive Flink on notebook-managed cluster example with
    additional real world examples (check [the blog](https://cloud.google.com/blog/products/data-analytics/interactive-beam-pipeline-ml-inference-at-scale-in-notebook)).
    - Demos processing tens of thousands of flight records from BigQuery.
    - Demos classifying hundreds of GBs of image files.

### Deprecated

-   Kernels with Beam v2.32.0 and v2.33.0

## 2022-07

### Added

-   Support (since Beam v2.39.0) for in-notebook
    [debugger](https://jupyterlab.readthedocs.io/en/stable/user/debugger.html).
-   Support (since Beam v2.40.0) for running Beam pipelines interactively with a
    FlinkRunner automagically hosted on notebook-managed Cloud Dataproc
    clusters with an [example](Examples/Flink_on_Dataproc_Word_Count.ipynb).
    -   This enables working with production scaled data because the workers are
        distributed to a Google Cloud internal cluster instead of a single
        notebook GCE instance.
-   Support (since Beam v2.40.0) for `RunInference` transform with an
    [example](Examples/RunInference.ipynb) for machine learning.
-   New kernel with Beam v2.40.0

### Deprecated

-   Kernel with Beam v2.31.0

## 2022-06

### Added

-   New kernel with Beam v2.39.0

### Deprecated

-   Kernels with Beam v2.28.0, v2.29.0 and v2.30.0

## 2022-05

### Added

-   New kernel with Beam v2.38.0

## 2022-03

### Added

-   New kernel with Beam v2.37.0

## 2022-02

### Added

-   New kernel with Beam v2.36.0

## 2022-01

### Added

-   New kernel with Beam v2.35.0

### Deprecated

-   Kernels with Beam v2.25.0, v2.26.0 and v2.27.0

## 2021-12

### Added

-   Beam SQL and `beam_sql` magic support (since Beam v2.34.0) with
    [example](Examples/Apache_Beam_SQL_in_notebooks.ipynb)
-   New kernel with Beam v2.34.0

### Updated

-   The [GPU example](Examples/Use_GPUs_with_Apache_Beam.ipynb) now demonstrates
    -   How to launch Dataflow jobs using GPU from notebooks
    -   How to write a Dockerfile and build a custom container with Cloud Build
        from notebooks

## 2021-11

### Added

-   [Interactive Beam](https://cloud.google.com/dataflow/docs/guides/interactive-pipeline-development#visualizing_the_data_through_the_interactive_beam_inspector)
    JupyterLab extension to interactively inspect the state of pipelines and
    data associated with each PCollection
