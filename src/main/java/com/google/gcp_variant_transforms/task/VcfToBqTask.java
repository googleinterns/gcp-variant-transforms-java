// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.task;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.gcp_variant_transforms.beam.PipelineRunner;
import com.google.gcp_variant_transforms.library.HeaderReader;
import com.google.gcp_variant_transforms.options.VcfToBqContext;

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

  @Inject
  public VcfToBqTask(
        PipelineRunner pipelineRunner,
        HeaderReader headerReader,
        VcfToBqContext context) throws IOException {
    this.pipelineRunner = pipelineRunner;
    this.headerReader = headerReader;
    this.context = context;
  }

  @Override
  public void run() throws IOException {
    context.setHeaderLines(headerReader.getHeaderLines());
    pipelineRunner.runPipeline();
  }

  public static void main(String[] args) throws IOException {
    Injector injector = Guice.createInjector(new VcfToBqModule(args));
    injector.getInstance(Task.class).run();
  }
}
