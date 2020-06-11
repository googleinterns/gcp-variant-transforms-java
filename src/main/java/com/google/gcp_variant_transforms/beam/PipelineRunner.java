// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.beam;
/**
 * Service for running main pipeline.
 */
public interface PipelineRunner {
  /**
   * Runs default pipeline for the corresponding task.
   */
  public void runPipeline();
}
