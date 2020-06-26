// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.entity;

import htsjdk.variant.variantcontext.VariantContext;
import static com.google.common.truth.Truth.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;

public class VariantTest {


  @Test
  public void testGetContig_whenComparingString_thenTrue() {
    String contig = "one";
    Variant testVariant = new Variant(contig, 1, 5);
    assertEquals(testVariant.getContig(), contig);
  }

  @Test
  public void testGetStart_whenComparingString_thenTrue() {
    int start = 14370;
    Variant testVariant = new Variant("one", start, 14374);
    assertEquals(testVariant.getStart(), start);
  }

  @Test
  public void testGetEnd_whenComparingString_thenTrue() {
    int end = 14375;
    Variant testVariant = new Variant("one", 14371, end);
    assertEquals(testVariant.getEnd(), end);
  }

  @Test
  public void testVariantConstructor_whenCompareFields_thenTrue() {
    VariantContext mockVariantContext = mock(VariantContext.class);
    assertTrue(mockVariantContext instanceof VariantContext);

    // mock VariantContext fields
    int start = 14370;
    int end = 14375;
    String contig = "one";
    when(mockVariantContext.getStart()).thenReturn(start);
    when(mockVariantContext.getEnd()).thenReturn(end);
    when(mockVariantContext.getContig()).thenReturn(contig);

    Variant testVariant = new Variant(mockVariantContext);

    assertTrue(testVariant.getContig().equals(contig) &&
               testVariant.getStart() == start &&
               testVariant.getEnd() == end);
  }

}
