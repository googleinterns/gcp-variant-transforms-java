// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.task;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.gcp_variant_transforms.beam.PipelineRunner;
import com.google.gcp_variant_transforms.library.HeaderReader;
import com.google.gcp_variant_transforms.options.VcfToBqContext;
import com.google.gcp_variant_transforms.options.VcfToBqOptions;

import java.io.IOException;

/**
 * Read VCF file and import it into BigQuery.
 */
public class VcfToBqTask implements Task {

  static String INPUT_PATH = "../test.vcf";
  static String OUTPUT_PATH = "../output/result";

  private final HeaderReader headerReader;
  private final PipelineRunner pipelineRunner;
  private final VcfToBqContext context;
  private final PipelineOptions options;

  @Inject
  public VcfToBqTask(
        PipelineRunner pipelineRunner,
        HeaderReader headerReader,
        VcfToBqContext context,
        VcfToBqOptions options) throws IOException {
    this.pipelineRunner = pipelineRunner;
    this.headerReader = headerReader;
    this.context = context;
    this.options = (PipelineOptions) options;
  }

  @Override
  public void run() throws IOException {
    set_pipeline_options(this.options);
    context.setHeaderLines(headerReader.getHeaderLines());
    pipelineRunner.runPipeline();
  }

  /** configures filesystem to the corresponding options, regardless of task. */
  /** @VisibleForTests */
  protected void set_pipeline_options(PipelineOptions options) {
    FileSystems.setDefaultPipelineOptions(options);
  }

  public static void main(String[] args) throws IOException {
    Injector injector = Guice.createInjector(new VcfToBqModule(args));
    injector.getInstance(Task.class).run();
  }
}
