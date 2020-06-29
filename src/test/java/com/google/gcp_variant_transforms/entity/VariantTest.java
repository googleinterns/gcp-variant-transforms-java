// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.entity;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import htsjdk.variant.variantcontext.VariantContext;
import org.junit.Test;

/**
 * Unit tests for Variant.java
 */
public class VariantTest {

  @Test
  public void testGetContig_whenComparingString_thenTrue() {
    String contig = "one";
    Variant testVariant = new Variant(contig, 1, 5);

    assertThat(testVariant.getContig().equals(contig)).isTrue();
  }

  @Test
  public void testGetStart_whenComparingString_thenTrue() {
    int start = 14370;
    Variant testVariant = new Variant("one", start, 14374);

    assertThat(testVariant.getStart() == start).isTrue();
  }

  @Test
  public void testGetEnd_whenComparingString_thenTrue() {
    int end = 14375;
    Variant testVariant = new Variant("one", 14371, end);

    assertThat(testVariant.getEnd() == end).isTrue();
  }

  @Test
  public void testVariantConstructor_whenCompareFields_thenTrue() {
    VariantContext mockVariantContext = mock(VariantContext.class);

    // mock VariantContext fields
    int start = 14370;
    int end = 14375;
    String contig = "one";
    when(mockVariantContext.getStart()).thenReturn(start);
    when(mockVariantContext.getEnd()).thenReturn(end);
    when(mockVariantContext.getContig()).thenReturn(contig);

    Variant testVariant = new Variant(mockVariantContext);

    assertThat(testVariant.getContig().equals(contig)).isTrue();
    assertThat(testVariant.getStart() == start).isTrue();
    assertThat(testVariant.getEnd() == end).isTrue();
  }
}
