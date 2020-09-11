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
import htsjdk.variant.vcf.VCFConstants;
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
  private static final boolean TEST_FLAG_PRESENT = true;
  private static final boolean TEST_FLAG_NOT_PRESENT = false;
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
    assertThat(rowWithFieldValues.get(Constants.ColumnKeyNames.REFERENCE_NAME))
        .isEqualTo(TEST_REFERENCE_NAME);
    assertThat(rowWithFieldValues.get(Constants.ColumnKeyNames.START_POSITION))
        .isEqualTo(TEST_START);
    assertThat(rowWithFieldValues.get(Constants.ColumnKeyNames.END_POSITION))
        .isEqualTo(TEST_END);
    assertThat(rowWithFieldValues.get(Constants.ColumnKeyNames.NAMES))
        .isEqualTo(Collections.singletonList(TEST_ID));
    assertThat(rowWithFieldValues.get(Constants.ColumnKeyNames.REFERENCE_BASES))
        .isEqualTo(TEST_REFERENCE_BASES);
    assertThat(rowWithFieldValues.get(Constants.ColumnKeyNames.QUALITY)).isEqualTo(29);
    assertThat(rowWithFieldValues.get(Constants.ColumnKeyNames.FILTER))
        .isEqualTo(FILTER_PASS);
  }

  @Test
  public void testBigQueryRowAlternatesAndInfo_whenCheckingRowElements_thenTrue() {
    // AF field has type=`A` in VCFHeader, which should be moved from Info field to ALT field
    List<TableRow> altMetadata = (List<TableRow>) rowWithFieldValues.get(
        Constants.ColumnKeyNames.ALTERNATE_BASES);
    TableRow altBase = altMetadata.get(0);
    assertThat(altBase.get(Constants.ColumnKeyNames.ALTERNATE_BASES_ALT))
        .isEqualTo(TEST_ALTERNATE_BASES);
    assertThat(altBase.get(VCFConstants.ALLELE_FREQUENCY_KEY)).isEqualTo(TEST_AF);

    assertThat(rowWithFieldValues.get(VCFConstants.SAMPLE_NUMBER_KEY)).isEqualTo(TEST_NS);
    assertThat(rowWithFieldValues.get(VCFConstants.DEPTH_KEY)).isEqualTo(TEST_DP);
    assertThat(rowWithFieldValues.get(VCFConstants.DBSNP_KEY)).isEqualTo(TEST_FLAG_PRESENT);

    // For fields that type is "Flag", if it is not presented, it should be set as false.
    assertThat(rowWithEmptyFields.get(VCFConstants.DBSNP_KEY)).isEqualTo(TEST_FLAG_NOT_PRESENT);
  }

  @Test
  public void testBigQueryRowCalls_whenCheckingRowElements_thenTrue() {
    List<TableRow> calls = (List<TableRow>) rowWithFieldValues
        .get(Constants.ColumnKeyNames.CALLS);
    assertThat(calls.size()).isEqualTo(3);
    // The rowWithFieldValues also contains record HQ=".,.", which should be [null,null] in the row.
    TableRow rowWithMissingHQ = calls.get(2);
    assertThat(rowWithMissingHQ.get(Constants.ColumnKeyNames.CALLS_SAMPLE_NAME))
        .isEqualTo(TEST_CALLS_NAME);
    assertThat(rowWithMissingHQ.get(Constants.ColumnKeyNames.CALLS_PHASESET))
        .isEqualTo(DEFAULT_PHASE_SET);
    assertThat(rowWithMissingHQ.get(Constants.ColumnKeyNames.CALLS_GENOTYPE))
        .isEqualTo(Arrays.asList(TEST_GENOTYPE, TEST_GENOTYPE));
    assertThat(rowWithMissingHQ.get(VCFConstants.HAPLOTYPE_QUALITY_KEY))
        .isEqualTo(Arrays.asList(null, null));
  }

  @Test
  public void testBigQueryRowWithEmptyFields_whenCheckingRowElements_thenTrue() {
    // ID field is ".", which should be null in the BQ row.
    assertThat(rowWithEmptyFields.get(Constants.ColumnKeyNames.NAMES))
        .isEqualTo(Collections.singletonList(null));

    // Quality field is ".", which should be -10 in the BQ row.
    assertThat(rowWithEmptyFields.get(Constants.ColumnKeyNames.QUALITY))
        .isEqualTo(UNKNOWN_QUALITY);

    assertThat(rowWithEmptyFields.get(Constants.ColumnKeyNames.FILTER)).isNull();

    // ALT field is ".", which should be null in the BQ row.
    List<TableRow> emptyAltMetadata = (List<TableRow>) rowWithEmptyFields
        .get(Constants.ColumnKeyNames.ALTERNATE_BASES);
    TableRow emptyAltBase = emptyAltMetadata.get(0);
    assertThat(emptyAltBase.get(Constants.ColumnKeyNames.ALTERNATE_BASES_ALT)).isNull();

    // GT field is ".", which should be default genotype(-1) in the BQ row.
    List<TableRow> calls = (List<TableRow>) rowWithEmptyFields
        .get(Constants.ColumnKeyNames.CALLS);
    TableRow rowWithMissingGenotype = calls.get(0);
    assertThat(rowWithMissingGenotype.get(Constants.ColumnKeyNames.CALLS_GENOTYPE))
        .isEqualTo(Arrays.asList(DEFAULT_GENOTYPE, DEFAULT_GENOTYPE));
  }
}
