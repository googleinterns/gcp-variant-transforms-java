// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.options;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import java.io.IOException;

/** Abstract Singleton context class, used to retain state of implementation. */
public abstract class AbstractContext {

  public AbstractContext(PipelineOptions options) {
    configureFilesystems(options);
  }

  /**
   * Validates flags at the start of the pipeline run.
   *
   * @throws IOException
   */
  protected abstract void validateFlags() throws IOException;

  /** configures filesystem to the corresponding options, regardless of task. */
  private void configureFilesystems(PipelineOptions options) {
    FileSystems.setDefaultPipelineOptions(options);
  }
}
