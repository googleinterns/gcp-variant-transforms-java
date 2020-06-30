// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import htsjdk.tribble.TribbleException;
import htsjdk.variant.vcf.VCFCodec;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Units tests for VcfParserImpl.java
 */
@RunWith(MockitoJUnitRunner.class)
public class VcfParserImplTest {

  @InjectMocks
  VcfParserImpl vcfParserImpl;

  ImmutableList<String> headerLines;

  @Before
  public void buildHeaderLines() {
    ImmutableList.Builder<String> headerLinesBuilder = new ImmutableList.Builder<>();
    headerLinesBuilder.add("##fileformat=VCFv4.3");
    headerLinesBuilder.add("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tNA00001\tNA00002\tNA00003");
    headerLines = headerLinesBuilder.build();
  }

  @Test
  public void testGenerateCodecFromHeaderLines_whenCheckFunctionCall_thenTrue() {
    VCFCodec vcfCodec = vcfParserImpl.generateCodecFromHeaderLines(headerLines);

    assertThat(vcfCodec).isNotNull();
  }

  @Test
  public void testGenerateCodecFromHeaderLines_withOutVcfVersion_thenThrowException() {
    ImmutableList.Builder<String> headerLinesBuilder = new ImmutableList.Builder<>();
    headerLinesBuilder.add("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tNA00001\tNA00002\tNA00003");

    // without specifying VCF version will throw TribbleException.InvalidHeader exception
    Exception invalidHeaderException = assertThrows(TribbleException.class, () ->
        vcfParserImpl.generateCodecFromHeaderLines(headerLinesBuilder.build()));

    assertThat(invalidHeaderException).hasMessageThat().contains("We never saw a header line specifying VCF version");
  }

  @Test
  public void testGenerateCodecFromHeaderLines_withOutHeaderLine_thenThrowException() {
    ImmutableList.Builder<String> headerLinesBuilder = new ImmutableList.Builder<>();
    headerLinesBuilder.add("##fileformat=VCFv4.3");
    headerLinesBuilder.add("##fileDate=20090805");

    // without VCF header line will throw TribbleException.InvalidHeader exception
    Exception invalidHeaderException = assertThrows(TribbleException.class, () ->
            vcfParserImpl.generateCodecFromHeaderLines(headerLinesBuilder.build()));

    assertThat(invalidHeaderException).hasMessageThat().contains("We never saw the required CHROM header line");
  }

}
