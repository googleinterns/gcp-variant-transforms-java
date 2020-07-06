// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import htsjdk.tribble.TribbleException;
import htsjdk.variant.vcf.VCFCodec;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Units tests for VcfParserImpl.java
 */
@RunWith(MockitoJUnitRunner.class)
public class VcfParserImplTest {
  private static final String FILE_FORMAT = "##fileformat=VCFv4.3";
  private static final String FILE_DATE = "##fileDate=20090805";
  private static final String VALID_HEADER = "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO" +
          "\tFORMAT\tNA00001\tNA00002\tNA00003";
  private static final String INVALID_HEADER = "#CHROM  POS  ID  REF  ALT  QUAL  FILTER  INFO  " +
          "FORMAT  NA00001 NA00002  NA00003";

  @InjectMocks
  VcfParserImpl vcfParserImpl;

  ImmutableList.Builder<String> headerLinesBuilder = new ImmutableList.Builder<>();

  @Test
  public void testGenerateCodecFromHeaderLines_whenCheckFunctionCall_thenTrue() {
    headerLinesBuilder.add(FILE_FORMAT);
    headerLinesBuilder.add(VALID_HEADER);
    VCFCodec vcfCodec = vcfParserImpl.generateCodecFromHeaderLines(headerLinesBuilder.build());

    assertThat(vcfCodec).isNotNull();
  }

  @Test
  public void testGenerateCodecFromHeaderLines_withoutVcfVersion_thenThrowException() {
    headerLinesBuilder.add(FILE_DATE);
    headerLinesBuilder.add(VALID_HEADER);

    // without specifying VCF version will throw TribbleException.InvalidHeader exception
    Exception invalidHeaderException = assertThrows(TribbleException.class, () ->
        vcfParserImpl.generateCodecFromHeaderLines(headerLinesBuilder.build()));

    assertThat(invalidHeaderException).hasMessageThat()
               .contains("We never saw a header line specifying VCF version");
  }

  @Test
  public void testGenerateCodecFromHeaderLines_withoutHeaderLine_thenThrowException() {
    headerLinesBuilder.add(FILE_FORMAT);
    headerLinesBuilder.add(FILE_DATE);

    // without VCF header line will throw TribbleException.InvalidHeader exception
    Exception invalidHeaderException = assertThrows(TribbleException.class, () ->
        vcfParserImpl.generateCodecFromHeaderLines(headerLinesBuilder.build()));

    assertThat(invalidHeaderException).hasMessageThat()
               .contains("We never saw the required CHROM header line");
  }

  @Test
  public void testGenerateCodecFromHeaderLines_wrongHeaderFormat_thenThrowException() {
    headerLinesBuilder.add(FILE_FORMAT);
    headerLinesBuilder.add(INVALID_HEADER);

    // Invalid VCF header line format will throw TribbleException.InvalidHeader exception
    Exception invalidHeaderException = assertThrows(TribbleException.class, () ->
            vcfParserImpl.generateCodecFromHeaderLines(headerLinesBuilder.build()));

    assertThat(invalidHeaderException).hasMessageThat()
               .contains("there are not enough columns present in the header line");
  }
}
