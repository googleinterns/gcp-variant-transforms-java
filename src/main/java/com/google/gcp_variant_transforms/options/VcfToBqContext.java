// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.options;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.gcp_variant_transforms.task.VcfToBqTask;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.beam.sdk.options.PipelineOptions;
import java.io.IOException;

/** Context for {@link VcfToBqTask} taks. */
@Singleton
public class VcfToBqContext extends AbstractContext {

  private final String inputFile;
  private final String output;
  private ImmutableList<String> headerLines = null;
  private VCFHeader vcfHeader = null;
  private TableSchema bqSchema = null;

  @Inject
  public VcfToBqContext(VcfToBqOptions options) throws IOException {
    super((PipelineOptions) options);
    this.inputFile = options.getInputFile();
    this.output = options.getOutput();
    validateFlags();
  }

  @Override
  protected void validateFlags() throws IOException {
    // Mock validation.
    if (this.output == null) {
      throw new IOException("No value for --output flag provided.");
    }
  }

  public String getInputFile() {
    return this.inputFile;
  }

  public String getOutput() {
    return this.output;
  }

  public void setHeaderLines(ImmutableList<String> headerLines) {
    this.headerLines = headerLines;
  }

  public ImmutableList<String> getHeaderLines() {
    return headerLines;
  }

  public VCFHeader getVCFHeader(){
    return vcfHeader;
  }

  public void setVCFHeader(VCFHeader vcfHeader){
    this.vcfHeader = vcfHeader;
  }

  public TableSchema getBqSchema(){
    return bqSchema;
  }

  public void setBqSchema(TableSchema schema){
    this.bqSchema = schema; 
  }
}
