// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.entity;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.gcp_variant_transforms.library.VcfParser;
import com.google.gcp_variant_transforms.TestEnv;
import com.google.guiceberry.junit4.GuiceBerryRule;
import com.google.inject.Inject;
import htsjdk.samtools.util.IOUtil;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFHeader;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Unit tests for Variant.java and value type validation.
 */
public class VariantTest {
  private static final String SMALL_FILE = "src/test/resources/vcf_files_small_valid-4.0.vcf";
  private static final String LARGE_FILE = "src/test/resources/vcf_files_small_valid-4.1-large.vcf";

  private static final String PHASESET_FORMAT_KEY = "PS";
  private static final String DEFAULT_PHASESET_VALUE = "*";
  private static final String CONTIG = "20";
  private static final String TEST_PHASE_SET = "1";
  private static final String REFERENCE_BASE = "A";
  private static final List<String> ALTERNATE_BASES = Arrays.asList("G", "T");
  private static final List<String> MISSING_VALUE_LIST = Collections.singletonList(null);
  private static final List<String> NAMES = Collections.singletonList("rs6040355");
  private static final Set<String> FILTERS = new HashSet<>(Collections.singletonList("q10"));
  private static final int START = 1110696;
  private static final int END = 1110696;
  private static final double QUALITY = 67.0;

  private Variant testVariant;    // The variant with full record values
  private Variant testVariantWithUnknownHQ; // HQ field will be ".,."
  private Variant testVariantWithUnknownFields;

  private Map<String, Object> info = new HashMap<>();

  @Rule
  public final GuiceBerryRule guiceBerry = new GuiceBerryRule(TestEnv.class);

  @Inject
  public VcfParser vcfParser;

  @Before
  // The test Info Map should contain different types, including String, List, int, float, boolean
  public void buildTestInfoMap() {
    info.put("AA", "T");
    info.put("NS", 2);
    info.put("DP", 10);
    info.put("AF", Arrays.asList(0.333, 0.667));
    info.put("DB", true);
  }

  @Before
  public void buildVariantFromSmallFile() throws FileNotFoundException {
    File smallFile = new File(SMALL_FILE);
    List<String> smallFileLines = IOUtil.slurpLines(smallFile);
    final int firstNonComment = IntStream.range(0, smallFileLines.size()).
            filter(i -> !smallFileLines.get(i).startsWith("#")).findFirst().getAsInt();
    ImmutableList<String> smallFileHeaderLines = ImmutableList.copyOf(smallFileLines.subList(0,
            firstNonComment));
    VCFCodec smallFileVcfCodec = vcfParser.generateCodecFromHeaderLines(smallFileHeaderLines);
    VCFHeader smallFileVcfHeader = smallFileVcfCodec.getHeader();
    // all records in small VCF file
    ImmutableList<String> smallFileRecords = ImmutableList.copyOf(
            smallFileLines.subList(firstNonComment, smallFileLines.size()));

    // the 3rd record in the small VCF file
    // 20	1110696	rs6040355	A	G,T	67	PASS	NS=2;DP=10;AF=0.333,0.667;AA=T;DB	GT:GQ:DP:HQ
    // 1|2:21:6:23,27	2|1:2:0:18,2	2/2:35:4
    // contains full record without '.'
    testVariant = new Variant(smallFileVcfCodec.decode(smallFileRecords.get(2)),
            smallFileVcfHeader);

    // the 1st record in the small VCF file
    // 20	14370	rs6054257	G	A	29	PASS	NS=3;DP=14;AF=0.5;DB;H2	GT:GQ:DP:HQ	0|0:48:1:51,51
    // 1|0:48:8:51,51	1/1:43:5:.,.
    // contains unknown HQ field ".,."
    testVariantWithUnknownHQ = new Variant(smallFileVcfCodec.decode(smallFileRecords.get(0)),
            smallFileVcfHeader);
  }

  @Before
  public void buildVariantFromLargeFile() throws FileNotFoundException {
    File largeFile = new File(LARGE_FILE);
    List<String> largeFileLines = IOUtil.slurpLines(largeFile);
    final int firstNonComment = IntStream.range(0, largeFileLines.size()).
            filter(i -> !largeFileLines.get(i).startsWith("#")).findFirst().getAsInt();
    ImmutableList<String> largeFileHeaderLines = ImmutableList.copyOf(largeFileLines.subList(0,
            firstNonComment));
    VCFCodec largeFileVcfCodec = vcfParser.generateCodecFromHeaderLines(largeFileHeaderLines);
    VCFHeader largeFileVcfHeader = largeFileVcfCodec.getHeader();
    // all records in large VCF file
    ImmutableList<String> largeFileRecords = ImmutableList.copyOf(
            largeFileLines.subList(firstNonComment, largeFileLines.size()));

    // the 3rd record in the large VCF file
    // 1	10001	.	T	<CGA_NOCALL>	.	.	END=11038;NS=1;AN=0	GT:PS	./.:1
    // contains many unknown fields and unknown phase set and genotype
    testVariantWithUnknownFields = new Variant(largeFileVcfCodec.decode(largeFileRecords.get(2)),
            largeFileVcfHeader);
  }

  @Test
  public void testGetContig_whenComparingString_thenTrue() {
    assertThat(testVariant.getContig().equals(CONTIG)).isTrue();
  }

  @Test
  public void testGetStart_whenComparingVALUE_thenTrue() {
    assertThat(testVariant.getStart() == START).isTrue();
  }

  @Test
  public void testGetEnd_whenComparingVALUE_thenTrue() {
    assertThat(testVariant.getEnd() == END).isTrue();
  }

  @Test
  public void testGetReferenceBase_whenComparingString_thenTrue() {
    assertThat(testVariant.getReferenceBase().equals(REFERENCE_BASE)).isTrue();
  }

  @Test
  public void testGetQuality_whenComparingVALUE_thenTrue() {
    assertThat(testVariant.getQuality() == QUALITY).isTrue();
  }

  @Test
  public void testGetAlternateBases_whenComparingListElement_thenTrue() {
    assertThat(testVariant.getAlternateBases().equals(ALTERNATE_BASES)).isTrue();

    // As for empty ALT alleles, which field is '.', the alternateBases will be null
    assertThat(testVariantWithUnknownFields.getAlternateBases().equals(MISSING_VALUE_LIST)).isTrue();
  }

  @Test
  public void testGetNames_whenComparingListElement_thenTrue() {
    assertThat(testVariant.getNames().equals(NAMES)).isTrue();

    // As for empty ID field, which field is '.', the names will be null
    assertThat(testVariantWithUnknownFields.getNames().equals(MISSING_VALUE_LIST)).isTrue();
  }

  @Test
  public void testGetFilters_whenComparingSetElement_thenTrue() {
    // PASS should represent an empty filter
    assertThat(testVariant.getFilters().isEmpty()).isTrue();
  }

  @Test
  public void testGetInfo_whenComparingMapElement_thenTrue() {
    assertThat(testVariant.getInfo().equals(info)).isTrue();
  }

  @Test
  public void testGetCalls_withGenotypePresent_whenComparingVaraintCallElements_thenTrue() {
    List<VariantCall> calls = testVariant.getCalls();
    assertThat(calls.size() == 3).isTrue();

    VariantCall call0 = calls.get(0);
    assertThat(call0.getSampleName().equals("NA00001")).isTrue();
    assertThat(call0.getPhaseSet().equals(DEFAULT_PHASESET_VALUE)).isTrue();
    assertThat(call0.getGenotype().equals(Arrays.asList(1, 2)));
    Map<String, Object> info0 = call0.getInfo();
    assertThat(info0.containsKey("HQ")).isTrue();
    assertThat(info0.get("HQ").equals(Arrays.asList(23, 27))).isTrue();
    assertThat(info0.containsKey("GQ")).isTrue();
    assertThat(info0.get("GQ").equals(21)).isTrue();
    assertThat(info0.containsKey("DP")).isTrue();
    assertThat(info0.get("DP").equals(6)).isTrue();

    VariantCall call1 = calls.get(1);
    assertThat(call1.getSampleName().equals("NA00002")).isTrue();
    assertThat(call1.getPhaseSet().equals(DEFAULT_PHASESET_VALUE)).isTrue();
    assertThat(call1.getGenotype().equals(Arrays.asList(2, 1)));
    Map<String, Object> info1 = call1.getInfo();
    assertThat(info1.containsKey("HQ")).isTrue();
    assertThat(info1.get("HQ").equals(Arrays.asList(18, 2))).isTrue();
    assertThat(info1.containsKey("GQ")).isTrue();
    assertThat(info1.get("GQ").equals(2)).isTrue();
    assertThat(info1.containsKey("DP")).isTrue();
    assertThat(info1.get("DP").equals(0)).isTrue();

    VariantCall call2 = calls.get(2);
    assertThat(call2.getSampleName().equals("NA00003")).isTrue();
    assertThat(call2.getPhaseSet().equals(DEFAULT_PHASESET_VALUE)).isTrue();
    assertThat(call2.getGenotype().equals(Arrays.asList(2, 2)));
    Map<String, Object> info2 = call2.getInfo();
    assertThat(!info2.containsKey("HQ")).isTrue();    // HQ is empty field in call3
    assertThat(info2.containsKey("GQ") && info2.get("GQ").equals(35)).isTrue();
    assertThat(info2.containsKey("DP") && info2.get("DP").equals(4)).isTrue();
  }

  @Test
  public void testGetCalls_withUnknownHQ_whenCheckingHQElement_thenTrue() {
    List<VariantCall> calls = testVariantWithUnknownHQ.getCalls();
    assertThat(calls.size() == 3).isTrue();

    // in the third VariantCall, HQ is ".,.", which should be an list of two nulls
    VariantCall call2 = calls.get(2);
    assertThat(call2.getInfo().containsKey("HQ")).isTrue();
    assertThat(call2.getInfo().get("HQ").equals(
            Arrays.asList(null, null))).isTrue();
  }

  @Test
  public void testGetCalls_withUnknownGenotypes_whenCheckingGenotypesValue_thenTrue() {
    List<VariantCall> calls = testVariantWithUnknownFields.getCalls();
    VariantCall call = calls.get(0);
    assertThat(call.getGenotype().equals(Collections.singletonList(-1)));
  }

  @Test
  public void testGetCalls_checkPhaseSet_whenCheckingPhaseSetStatus_thenTrue() {
    List<VariantCall> smallFileRecordVCs = testVariant.getCalls();
    VariantCall smallFileRecordVC = smallFileRecordVCs.get(0);
    // This call does not present PS, phase set should be "*"
    assertThat(smallFileRecordVC.getPhaseSet().equals(DEFAULT_PHASESET_VALUE)).isTrue();

    List<VariantCall> largeFileRecordVCs = testVariantWithUnknownFields.getCalls();
    VariantCall largeFileRecordVC = largeFileRecordVCs.get(0);
    assertThat(largeFileRecordVC.getPhaseSet().equals(TEST_PHASE_SET)).isTrue();
    // info map should skip the Phase Set field
    assertThat(!largeFileRecordVC.getInfo().containsKey(PHASESET_FORMAT_KEY)).isTrue();
  }
}