// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.task;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.gcp_variant_transforms.beam.PipelineRunner;
import com.google.gcp_variant_transforms.library.HeaderReader;
import com.google.gcp_variant_transforms.library.VcfParser;
import com.google.gcp_variant_transforms.options.VcfToBqContext;

import java.io.IOException;

/**
 * Read VCF file and import it into BigQuery.
 */
public class VcfToBqTask implements Task {

  private final HeaderReader headerReader;
  private final PipelineRunner pipelineRunner;
  private final VcfToBqContext context;
  private final VcfParser parser;

  @Inject
  public VcfToBqTask(
        PipelineRunner pipelineRunner,
        HeaderReader headerReader,
        VcfToBqContext context,
        VcfParser parser) throws IOException {
    this.pipelineRunner = pipelineRunner;
    this.headerReader = headerReader;
    this.context = context;
    this.parser = parser;
  }

  @Override
  public void run() throws IOException {
    context.setHeaderLines(headerReader.getHeaderLines());
    context.setVCFHeader(parser.generateVCFHeader(context.getHeaderLines()));
    pipelineRunner.runPipeline();
  }

  public static void main(String[] args) throws IOException {
    Injector injector = Guice.createInjector(new VcfToBqModule(args));
    injector.getInstance(Task.class).run();
  }
}
