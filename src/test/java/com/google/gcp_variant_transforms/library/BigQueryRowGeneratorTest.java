// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;
import com.google.gcp_variant_transforms.TestEnv;
import com.google.guiceberry.junit4.GuiceBerryRule;
import com.google.inject.Inject;
import htsjdk.samtools.util.IOUtil;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Integration test for generating BigQuery row from VCF record
 * This test cover the following methods in the Pipeline Runner:
 * 'generateCodecFromHeaderLines' method in ConvertLineToVariantFn.java
 * 'getRows' method in ConvertVariantRowFn.java and BigQueryRowGeneratorImpl.java
 */
public class BigQueryRowGeneratorTest {
  private static final String TEST_FILE = "src/test/resources/vcf_files_small_valid-4.0.vcf";
  private VCFCodec vcfCodec;
  private VCFHeader vcfHeader;
  private ImmutableList<String> vcfRecords;
  private List<TableRow> rows;

  @Rule
  public final GuiceBerryRule guiceBerry = new GuiceBerryRule(TestEnv.class);

  @Inject
  public VcfParser vcfParser;

  @Inject
  public BigQueryRowGenerator bigQueryRowGenerator;

  @Before
  public void buildBigQueryRowsFromVCFFile() throws FileNotFoundException {
    File smallFile = new File(TEST_FILE);
    List<String> fileLines = IOUtil.slurpLines(smallFile);
    final int firstNonComment = IntStream.range(0, fileLines.size()).
            filter(i -> !fileLines.get(i).startsWith("#")).findFirst().getAsInt();
    ImmutableList<String> headerLines = ImmutableList.copyOf(fileLines.subList(0,
            firstNonComment));
    vcfCodec = vcfParser.generateCodecFromHeaderLines(headerLines);
    vcfHeader = vcfCodec.getHeader();
    // all records in VCF file
    vcfRecords = ImmutableList.copyOf(
            fileLines.subList(firstNonComment, fileLines.size()));

    // build BQ rows
    rows = new ArrayList<>();
    for (int i = 0; i < vcfRecords.size(); ++i) {
      rows.add(bigQueryRowGenerator.getRows(vcfCodec.decode(vcfRecords.get(i)), vcfHeader));
    }
  }

  /**
   * Records in the test VCF file:
   * #CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	NA00001	NA00002	NA00003
   * 20	14370	rs6054257	G	A	29	PASS	NS=3;DP=14;AF=0.5;DB;H2	GT:GQ:DP:HQ	0|0:48:1:51,51
   * 1|0:48:8:51,51	1/1:43:5:.,.
   * 20	17330	.	T	A	3	q10	NS=3;DP=11;AF=0.017	GT:GQ:DP:HQ	0|0:49:3:58,50	0|1:3:5:65,3	0/0:41:3
   * 20	1110696	rs6040355	A	G,T	67	PASS	NS=2;DP=10;AF=0.333,0.667;AA=T;DB	GT:GQ:DP:HQ
   * 1|2:21:6:23,27	2|1:2:0:18,2	2/2:35:4
   * 20	1230237	.	T	.	47	PASS	NS=3;DP=13;AA=T	GT:GQ:DP:HQ	0|0:54:7:56,60	0|0:48:4:51,51	0/0:61:2
   * 19	1234567	microsat1	GTCT	G,GTACT	50	PASS	NS=3;DP=9;AA=G	GT:GQ:DP	0/1:35:4	0/2:17:2
   * 1/1:40:3
   * There are 5 records in the file, thus there will be 5 BQ rows
   * The test should cover all kinds of elements in those records, including corner cases.
   */
  @Test
  public void testBigQueryRowGenerator_whenCheckingRowElements_thenTrue() {
    assertThat(rows.size()).isEqualTo(5);
    // The first row covers all base fields without missing field value("."),
    // and this row also contains record HQ=".,.", which should be [null,null] in the BQ row
    TableRow firstRow = rows.get(0);
    assertThat(firstRow.get(VariantToBqUtils.ColumnKeyConstants.REFERENCE_NAME))
            .isEqualTo("20");
    assertThat(firstRow.get(VariantToBqUtils.ColumnKeyConstants.START_POSITION))
            .isEqualTo(14370);
    assertThat(firstRow.get(VariantToBqUtils.ColumnKeyConstants.END_POSITION))
            .isEqualTo(14370);
    assertThat(firstRow.get(VariantToBqUtils.ColumnKeyConstants.NAMES))
            .isEqualTo("rs6054257");
    assertThat(firstRow.get(VariantToBqUtils.ColumnKeyConstants.REFERENCE_BASES)).isEqualTo("G");

    // AF field has type=`A` in VCFHeader, which should be moved from Info field to ALT field
    List<TableRow> altMetadata = (List<TableRow>) firstRow.get(
            VariantToBqUtils.ColumnKeyConstants.ALTERNATE_BASES);
    TableRow altBase = altMetadata.get(0);
    assertThat(altBase.get(VariantToBqUtils.ColumnKeyConstants.ALTERNATE_BASES_ALT)).isEqualTo("A");
    assertThat(altBase.get("AF")).isEqualTo(0.5);

    assertThat(firstRow.get(VariantToBqUtils.ColumnKeyConstants.QUALITY)).isEqualTo(29);
    Set<String> expectedFilter = new HashSet<>(Collections.singletonList("PASS"));
    assertThat(firstRow.get(VariantToBqUtils.ColumnKeyConstants.FILTER)).
            isEqualTo(expectedFilter);

    assertThat(firstRow.get("NS")).isEqualTo(3);
    assertThat(firstRow.get("DP")).isEqualTo(14);
    assertThat(firstRow.get("DB")).isEqualTo(true);
    assertThat(firstRow.get("H2")).isEqualTo(true);

    List<TableRow> calls = (List<TableRow>) firstRow.get(VariantToBqUtils.ColumnKeyConstants.CALLS);
    assertThat(calls.size()).isEqualTo(3);
    // The third genotype contains HQ=".,.", test if the field value is [null, null]
    TableRow rowWithMissingHQ = calls.get(2);
    assertThat(rowWithMissingHQ.get(VariantToBqUtils.ColumnKeyConstants.CALLS_NAME))
            .isEqualTo("NA00003");
    assertThat(rowWithMissingHQ.get(VariantToBqUtils.ColumnKeyConstants.CALLS_PHASESET))
            .isEqualTo("*");
    assertThat(rowWithMissingHQ.get(VariantToBqUtils.ColumnKeyConstants.CALLS_GENOTYPE))
            .isEqualTo(Arrays.asList(1, 1));
    assertThat(rowWithMissingHQ.get("HQ")).isEqualTo(Arrays.asList(null, null));

    // Test the fourth record in the test comment, there are some empty field in the record
    TableRow fourthRow = rows.get(3);
    assertThat(fourthRow.get(VariantToBqUtils.ColumnKeyConstants.NAMES)).isEqualTo(null);
    // ALT field is ".", which should be null in the BQ row
    List<TableRow> emptyAltMetadata = (List<TableRow>) fourthRow.get(
            VariantToBqUtils.ColumnKeyConstants.ALTERNATE_BASES);
    TableRow emptyAltBase = emptyAltMetadata.get(0);
    assertThat(emptyAltBase.get(VariantToBqUtils.ColumnKeyConstants.ALTERNATE_BASES_ALT))
            .isEqualTo(null);
  }
}
