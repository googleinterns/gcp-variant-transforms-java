// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.beam;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gcp_variant_transforms.beam.helper.ConvertVariantToRowFn;
import com.google.gcp_variant_transforms.library.BigQueryRowGenerator;
import com.google.inject.Inject;
import com.google.gcp_variant_transforms.beam.helper.ConvertLineToVariantFn;
import com.google.gcp_variant_transforms.library.VcfParser;
import com.google.gcp_variant_transforms.options.VcfToBqContext;
import com.google.gcp_variant_transforms.options.VcfToBqOptions;
import htsjdk.variant.variantcontext.VariantContext;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public final class VcfToBqPipelineRunner implements PipelineRunner {

  private final VcfToBqContext context;
  private final PipelineOptions options;
  private final VcfParser vcfParser;
  private final BigQueryRowGenerator bigQueryRowGenerator;

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
    // Demo code.
    Pipeline pipeline = Pipeline.create(options);
    PCollection<VariantContext> variantContextPCollection =
            pipeline.apply(TextIO.read().from(context.getInputFile()))
      .apply(Filter.by((String inputLine) -> !inputLine.startsWith("#")))
      .apply(ParDo.of(new ConvertLineToVariantFn(vcfParser, context.getHeaderLines())));

    PCollection<TableRow> tableRowPCollection = variantContextPCollection
            .apply("VariantContextToBQRow",
                    ParDo.of(new ConvertVariantToRowFn(bigQueryRowGenerator,
                            context.getVCFHeader())));

    variantContextPCollection.apply(
          MapElements
              .into(TypeDescriptors.strings())
              .via(
                  (VariantContext variant) ->
                  String.format("Contig: %s; Start: %d; End: %d",
                      variant.getContig(), variant.getStart(), variant.getEnd())))
      .apply(TextIO.write().to(context.getOutput()).withNoSpilling());
    pipeline.run().waitUntilFinish();
  }
}
