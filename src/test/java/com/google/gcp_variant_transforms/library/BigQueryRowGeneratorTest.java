// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;
import com.google.gcp_variant_transforms.TestEnv;
import com.google.gcp_variant_transforms.common.Constants;
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
  private static final String TEST_REFERENCE_NAME = "20";
  private static final String TEST_ID= "rs6054257";
  private static final String TEST_REFERENCE_BASES = "G";
  private static final String TEST_ALTERNATE_BASES = "A";
  private static final String TEST_CALLS_NAME = "NA00003";
  private static final String DEFAULT_PHASE_SET = "*";
  private static final boolean TEST_DB = true;
  private static final boolean TEST_H2 = true;
  private static final double TEST_AF = 0.5;
  private static final double UNKNOWN_QUALITY = -10;
  private static final int TEST_START = 14370;
  private static final int TEST_END = 14370;
  private static final int TEST_NS = 3;
  private static final int TEST_DP = 14;
  private static final int TEST_GENOTYPE= 1;
  private static final int DEFAULT_GENOTYPE = -1;
  private static final Set<String> FILTER_PASS = new HashSet<>(Collections.singletonList("PASS"));

  private VCFCodec vcfCodec;
  private VCFHeader vcfHeader;
  private ImmutableList<String> vcfRecords;
  private List<TableRow> rows;
  private TableRow rowWithFieldValues;
  private TableRow rowWithEmptyFields;

  @Rule
  public final GuiceBerryRule guiceBerry = new GuiceBerryRule(TestEnv.class);

  @Inject public VcfParser vcfParser;

  @Inject public BigQueryRowGenerator bigQueryRowGenerator;

  /**
   * Records             in the test VCF file:
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
  @Before
  public void buildBigQueryRowsFromVCFFile() throws FileNotFoundException {
    File smallFile = new File(TEST_FILE);
    List<String> fileLines = IOUtil.slurpLines(smallFile);
    final int firstNonComment = IntStream.range(0, fileLines.size())
        .filter(i -> !fileLines.get(i).startsWith("#")).findFirst().getAsInt();
    ImmutableList<String> headerLines = ImmutableList.copyOf(fileLines
        .subList(0, firstNonComment));
    vcfCodec = vcfParser.generateCodecFromHeaderLines(headerLines);
    vcfHeader = vcfCodec.getHeader();
    // All records in VCF file.
    vcfRecords = ImmutableList.copyOf(fileLines.subList(firstNonComment, fileLines.size()));

    // Build BQ rows.
    rows = new ArrayList<>();
    for (int i = 0; i < vcfRecords.size(); ++i) {
      rows.add(bigQueryRowGenerator.convertToBQRow(vcfCodec.decode(vcfRecords.get(i)), vcfHeader));
    }

    // The first row covers all base fields without missing field value(".").
    // The second row is a row with many empty fields for testing corner cases.
    rowWithFieldValues = rows.get(0);
    rowWithEmptyFields = rows.get(1);
  }

  @Test
  public void testBigQueryRowBaseElements_whenCheckingRowElements_thenTrue() {
    assertThat(rowWithFieldValues.get(Constants.ColumnKeyConstants.REFERENCE_NAME))
        .isEqualTo(TEST_REFERENCE_NAME);
    assertThat(rowWithFieldValues.get(Constants.ColumnKeyConstants.START_POSITION))
        .isEqualTo(TEST_START);
    assertThat(rowWithFieldValues.get(Constants.ColumnKeyConstants.END_POSITION))
        .isEqualTo(TEST_END);
    assertThat(rowWithFieldValues.get(Constants.ColumnKeyConstants.NAMES))
        .isEqualTo(TEST_ID);
    assertThat(rowWithFieldValues.get(Constants.ColumnKeyConstants.REFERENCE_BASES))
        .isEqualTo(TEST_REFERENCE_BASES);
    assertThat(rowWithFieldValues.get(Constants.ColumnKeyConstants.QUALITY)).isEqualTo(29);
    assertThat(rowWithFieldValues.get(Constants.ColumnKeyConstants.FILTER))
        .isEqualTo(FILTER_PASS);
  }

  @Test
  public void testBigQueryRowAlternatesAndInfo_whenCheckingRowElements_thenTrue() {
    // AF field has type=`A` in VCFHeader, which should be moved from Info field to ALT field
    List<TableRow> altMetadata = (List<TableRow>) rowWithFieldValues.get(
        Constants.ColumnKeyConstants.ALTERNATE_BASES);
    TableRow altBase = altMetadata.get(0);
    assertThat(altBase.get(Constants.ColumnKeyConstants.ALTERNATE_BASES_ALT))
        .isEqualTo(TEST_ALTERNATE_BASES);
    assertThat(altBase.get("AF")).isEqualTo(TEST_AF);

    assertThat(rowWithFieldValues.get("NS")).isEqualTo(TEST_NS);
    assertThat(rowWithFieldValues.get("DP")).isEqualTo(TEST_DP);
    assertThat(rowWithFieldValues.get("DB")).isEqualTo(TEST_DB);
    assertThat(rowWithFieldValues.get("H2")).isEqualTo(TEST_H2);
  }

  @Test
  public void testBigQueryRowCalls_whenCheckingRowElements_thenTrue() {
    List<TableRow> calls = (List<TableRow>) rowWithFieldValues
        .get(Constants.ColumnKeyConstants.CALLS);
    assertThat(calls.size()).isEqualTo(3);
    // The rowWithFieldValues also contains record HQ=".,.", which should be [null,null] in the row.
    TableRow rowWithMissingHQ = calls.get(2);
    assertThat(rowWithMissingHQ.get(Constants.ColumnKeyConstants.CALLS_NAME))
        .isEqualTo(TEST_CALLS_NAME);
    assertThat(rowWithMissingHQ.get(Constants.ColumnKeyConstants.CALLS_PHASESET))
        .isEqualTo(DEFAULT_PHASE_SET);
    assertThat(rowWithMissingHQ.get(Constants.ColumnKeyConstants.CALLS_GENOTYPE))
        .isEqualTo(Arrays.asList(TEST_GENOTYPE, TEST_GENOTYPE));
    assertThat(rowWithMissingHQ.get("HQ")).isEqualTo(Arrays.asList(null, null));
  }

  @Test
  public void testBigQueryRowWithEmptyFields_whenCheckingRowElements_thenTrue() {
    // ID field is ".", which should be null in the BQ row.
    assertThat(rowWithEmptyFields.get(Constants.ColumnKeyConstants.NAMES)).isNull();

    // Quality field is ".", which should be -10 in the BQ row.
    assertThat(rowWithEmptyFields.get(Constants.ColumnKeyConstants.QUALITY))
        .isEqualTo(UNKNOWN_QUALITY);

    assertThat(rowWithEmptyFields.get(Constants.ColumnKeyConstants.FILTER)).isNull();

    // ALT field is ".", which should be null in the BQ row.
    List<TableRow> emptyAltMetadata = (List<TableRow>) rowWithEmptyFields
        .get(Constants.ColumnKeyConstants.ALTERNATE_BASES);
    TableRow emptyAltBase = emptyAltMetadata.get(0);
    assertThat(emptyAltBase.get(Constants.ColumnKeyConstants.ALTERNATE_BASES_ALT)).isNull();

    // GT field is ".", which should be default genotype(-1) in the BQ row.
    List<TableRow> calls = (List<TableRow>) rowWithEmptyFields
        .get(Constants.ColumnKeyConstants.CALLS);
    TableRow rowWithMissingGenotype = calls.get(0);
    assertThat(rowWithMissingGenotype.get(Constants.ColumnKeyConstants.CALLS_GENOTYPE))
        .isEqualTo(Arrays.asList(DEFAULT_GENOTYPE, DEFAULT_GENOTYPE));
  }
}
