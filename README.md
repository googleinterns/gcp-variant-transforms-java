# GCP Variant Transforms Java

[WIP]
## Sample Run

Set the flags for your project, and run the gradle command.


- GOOGLE_CLOUD_PROJECT: This is your project ID that contains the BigQuery dataset.
- GOOGLE_CLOUD_REGION: You must choose a geographic region for Cloud Dataflow to process your data, for example: us-west1. For more info about regions please refer to [Setting Regions](https://github.com/googlegenomics/gcp-variant-transforms/blob/master/docs/setting_region.md).
- INPUT_FILE: Either a local or Google Cloud Storage path for the VCF file.
- OUTPUT: A path to output file. Temporarily used for Demo run.
- TEMP_LOCATION: This can be any folder in Google Cloud Storage that your project has write access to. It's used to store temporary files and logs from the pipeline.

Sample Command:
```
GOOGLE_CLOUD_PROJECT=tural-test-runner
RUNNER=DirectRunner
INPUT_FILE=gs://gcp-variant-transforms-testfiles/small_tests/valid-4.0.vcf
JOB_NAME=java-test-run
GOOGLE_CLOUD_REGION=us-central1
TEMP_LOCATION=gs://${GOOGLE_CLOUD_PROJECT}/javawork/temp
OUTPUT=../output/report

./gradlew vcfToBq -Prargs=" \
  --project=${GOOGLE_CLOUD_PROJECT} \
  --runner=${RUNNER} \
  --jobName=${JOB_NAME} \
  --region=${GOOGLE_CLOUD_REGION} \
  --tempLocation=${TEMP_LOCATION} \
  --inputFile=${INPUT_FILE} \
  --output=${OUTPUT}"
```