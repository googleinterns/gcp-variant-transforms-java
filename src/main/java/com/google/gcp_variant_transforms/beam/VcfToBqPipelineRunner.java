// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.beam;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gcp_variant_transforms.beam.helper.ConvertVariantToRowFn;
import com.google.gcp_variant_transforms.common.Constants;
import com.google.gcp_variant_transforms.entity.MalformedRecord;
import com.google.gcp_variant_transforms.library.BigQueryRowGenerator;
import com.google.gcp_variant_transforms.library.MalformedRecordCSVSink;
import com.google.inject.Inject;
import com.google.gcp_variant_transforms.beam.helper.ConvertLineToVariantFn;
import com.google.gcp_variant_transforms.library.VcfParser;
import com.google.gcp_variant_transforms.options.VcfToBqContext;
import com.google.gcp_variant_transforms.options.VcfToBqOptions;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import java.util.Arrays;
import java.util.List;

public final class VcfToBqPipelineRunner implements PipelineRunner {

  private final VcfToBqContext context;
  private final PipelineOptions options;
  private final VcfParser vcfParser;
  private final BigQueryRowGenerator bigQueryRowGenerator;

  private final TupleTag<TableRow> VALID_VARIANT_TO_BQ_RECORD_TAG = new TupleTag<>() {};
  private final TupleTag<MalformedRecord> MALFORMED_RECORD_ERROR_MESSAGE_TAG = new TupleTag<>() {};

  /** Implementation of {@link PipelineRunner} service. */
  @Inject
  public VcfToBqPipelineRunner(
      VcfToBqContext context, VcfToBqOptions options, VcfParser vcfParser,
      BigQueryRowGenerator bigQueryRowGenerator) {
    this.context = context;
    this.options = (PipelineOptions) options;
    this.vcfParser = vcfParser;
    this.bigQueryRowGenerator = bigQueryRowGenerator;
  }

  public void runPipeline() {
    Pipeline pipeline = Pipeline.create(options);
    PCollection<VariantContext> variantContextPCollection = pipeline
        .apply(TextIO.read().from(context.getInputFile()))
        .apply(Filter.by((String inputLine) -> !inputLine.startsWith("#")))
        .apply(ParDo.of(new ConvertLineToVariantFn(vcfParser, context.getHeaderLines())));
    PCollectionTuple tableRowTuple = variantContextPCollection
        .apply("VariantContextToBQRow",
            ParDo.of(new ConvertVariantToRowFn(bigQueryRowGenerator,
                context.getVCFHeader(), context.getAllowMalformedRecords(),
                    VALID_VARIANT_TO_BQ_RECORD_TAG, MALFORMED_RECORD_ERROR_MESSAGE_TAG))
                .withOutputTags(VALID_VARIANT_TO_BQ_RECORD_TAG,
                    TupleTagList.of(MALFORMED_RECORD_ERROR_MESSAGE_TAG)));

    PCollection<TableRow> validRowCollection = tableRowTuple.get(VALID_VARIANT_TO_BQ_RECORD_TAG);

    validRowCollection.apply("WriteTableRowToBigQuery",
        BigQueryIO.writeTableRows().to(context.getOutput())
            .withSchema(context.getBqSchema())
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

    PCollection<MalformedRecord> errorMessageCollection =
        tableRowTuple.get(MALFORMED_RECORD_ERROR_MESSAGE_TAG);

    errorMessageCollection.apply(MapElements
        .into(TypeDescriptors.lists(TypeDescriptors.strings()))
        .via(record -> Arrays.asList(record.getReferenceName(), record.getStart(),
            record.getReferenceBases(), record.getErrorMessage())))
        .apply(FileIO.<List<String>>write()
            .via(new MalformedRecordCSVSink(Arrays.asList(Constants.ColumnKeyNames.REFERENCE_NAME,
                Constants.ColumnKeyNames.START_POSITION, Constants.ColumnKeyNames.REFERENCE_BASES
                , "error_message")))
            .to(context.getMalformedRecordsMessagePath())
            .withPrefix("malformed_record")
            .withSuffix(".csv"));

    pipeline.run().waitUntilFinish();
  }
}
