# GCP Variant Transforms Java

[WIP]
## Sample Run

Set the flags for your project, and run the gradle command.

- GOOGLE_CLOUD_PROJECT: This is your project ID that contains the BigQuery dataset.
- GOOGLE_CLOUD_REGION: You must choose a geographic region for Cloud Dataflow to process your data, for example: us-west1. For more info about regions please refer to [Setting Regions](https://github.com/googlegenomics/gcp-variant-transforms/blob/master/docs/setting_region.md).
- RUNNER: The PipelineRunner to use. This field allows you to determine the PipelineRunner at runtime. For Dataflow job, the runner should be ``DataflowRunner``.
- INPUT_FILE: Either a local or Google Cloud Storage path for the VCF file.
- OUTPUT: A path to output BigQuery table. The format should be `ProjectID:DatasetID.TableID`.
- TEMP_LOCATION: This can be any folder in Google Cloud Storage that your project has write access to. It's used to store temporary files and logs from the pipeline.
- USE_ONE_BASED_COORDINATE: Flag of using [one based or zero based coordinate](https://www.biostars.org/p/84686/) in BigQuery table or not. This argument has a default value **false**.
- ALLOW_MALFORMED_RECORDS: Flag of allowing malformed records or not. This argument has a default value **false**. If the flag set as true, all the malformed records will be written to an CSV file, which includes the following information: **reference name**, **start position**, **reference bases** and **error message**.
- MALFORMED_RECORDS_REPORT_PATH: A path to malformed records file. Note that this argument is required if **ALLOW_MALFORMED_RECORDS** is set true, otherwise it will raise an exception.

Sample Command:

```
GOOGLE_CLOUD_PROJECT=tural-test-runner
RUNNER=DataflowRunner
INPUT_FILE=gs://gcp-variant-transforms-testfiles/small_tests/valid-4.0.vcf
JOB_NAME=java-test-run
GOOGLE_CLOUD_REGION=us-central1
TEMP_LOCATION=gs://${GOOGLE_CLOUD_PROJECT}/javawork/temp
OUTPUT=tural-test-runner:zqsun_vcf_to_bq.sample_test
USE_ONE_BASED_COORDINATE=true
ALLOW_MALFORMED_RECORDS=true
MALFORMED_RECORDS_REPORT_PATH=gs://${GOOGLE_CLOUD_PROJECT}/javawork/temp/${JOB_NAME}/malformed_records

./gradlew vcfToBq -Prargs=" \
  --project=${GOOGLE_CLOUD_PROJECT} \
  --runner=${RUNNER} \
  --jobName=${JOB_NAME} \
  --region=${GOOGLE_CLOUD_REGION} \
  --tempLocation=${TEMP_LOCATION} \
  --inputFile=${INPUT_FILE} \
  --output=${OUTPUT} \
  --useOneBasedCoordinate=${USE_ONE_BASED_COORDINATE} \
  --allowMalformedRecords=${ALLOW_MALFORMED_RECORDS} \
  --malformedRecordsReportPath=${MALFORMED_RECORDS_REPORT_PATH}"
```

For more parameters specifying the pipeline execution, please refer to this [Google Cloud Dataflow documentation](https://cloud.google.com/dataflow/docs/guides/specifying-exec-params).