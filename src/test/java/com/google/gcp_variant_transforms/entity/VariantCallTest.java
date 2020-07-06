// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.entity;

import static com.google.common.truth.Truth.assertThat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class VariantCallTest {
  private static final List<Integer> GENOTYPE = Arrays.asList(0, 71, 84);
  private static final String SAMPLE_NAME = "NA00001";
  private static final String PHASE_SET = "*";

  private VariantCall testVariantCall;
  private Map<String, Object> info = new HashMap<>();

  @Before
  public void buildTestInfoMap() {
    info.put("GQ", 21);
    info.put("DP", 6);
    info.put("HQ", "23,27");
  }

  @Before
  public void buildTestVariantCall() {
    testVariantCall = new VariantCall(SAMPLE_NAME, GENOTYPE, PHASE_SET, info);
  }

  @Test
  public void testGetSampleName_whenComparingString_thenTrue() {
    assertThat(testVariantCall.getSampleName().equals(SAMPLE_NAME)).isTrue();
  }

  @Test
  public void testGetPhaseSet_whenComparingString_thenTrue() {
    assertThat(testVariantCall.getPhaseSet().equals(PHASE_SET)).isTrue();
  }

  @Test
  public void testGetGenotype_whenComparingListElement_thenTrue() {
    assertThat(testVariantCall.getGenotype().equals(GENOTYPE)).isTrue();
  }

  @Test
  public void testGetInfo_whenComparingMapElement_thenTrue() {
    assertThat(testVariantCall.getInfo().equals(info)).isTrue();
  }
}
