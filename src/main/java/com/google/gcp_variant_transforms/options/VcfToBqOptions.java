// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.options;

import com.google.gcp_variant_transforms.task.VcfToBqTask;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * PipelineOptions interface for {@link VcfToBqTask} task.
 *
 * In order to parse supplied arguments, we use Apache Beam infrastructure, by extending their
 * options and adding any required flags for our task. These flags, after getting parsed, are
 * supplied into {@link VcfToBqContext} to be validated and populated.
 */
public interface VcfToBqOptions extends PipelineOptions {

  @Description("Path of the file to read from")
  @Required
  String getInputFile();
  void setInputFile(String value);

  /** Used only for demo, to be deleted. */
  @Description("Path of the file to write to")
  String getOutput();
  void setOutput(String value);
}
