// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import static com.google.common.truth.Truth.*;
import static org.junit.jupiter.api.Assertions.*;

import com.google.common.collect.ImmutableList;
import com.google.gcp_variant_transforms.library.HeaderIterator;
import org.junit.*;

/**
 * Units tests for HeaderIterator.java
 */
public class HeaderIteratorTest {

  public HeaderIterator createHeaderIterator(int numHeaderLines){
    ImmutableList.Builder<String> headerLinesBuilder = new ImmutableList.Builder<String>();
    HeaderIterator headerIterator;
    for (int i = 0; i < numHeaderLines; i++) {
      headerLinesBuilder.add("##sampleHeaderLine=" + i);
    }
    return new HeaderIterator(headerLinesBuilder.build());
  }

  @Test
  public void testHeaderLinesHasNext_whenCheckingBoolean_thenTrue() {
    int numHeaderLines = 5;
    HeaderIterator headerIterator = createHeaderIterator(numHeaderLines);

    assertThat(headerIterator.hasNext()).isTrue();
  }

  @Test
  public void testHeaderLinesHasNext_whenCheckingBoolean_thenFalse() {
    int numHeaderLines = 5;
    HeaderIterator headerIterator = createHeaderIterator(numHeaderLines);

    for (int i = 0; i < numHeaderLines; i++) {
      headerIterator.next();
    }

    assertThat(headerIterator.hasNext()).isFalse();
  }

  @Test
  public void testHeaderLinesHasNext_whenEmptyCheckingBoolean_thenFalse() {
    HeaderIterator headerIterator = new HeaderIterator(ImmutableList.of()); // empty header

    assertThat(headerIterator.hasNext()).isFalse();
  }

  @Test
  public void testHeaderLinesNext_whenCheckingString_thenContains() {
    int numHeaderLines = 5;
    String expectedHeaderLine1 = "##sampleHeaderLine=0";
    String expectedHeaderLine2 = "##sampleHeaderLine=1"; 
    HeaderIterator headerIterator = createHeaderIterator(numHeaderLines);

    assertThat(headerIterator.next()).matches(expectedHeaderLine1);
    // next should move the pointer
    assertThat(headerIterator.next()).matches(expectedHeaderLine2);
  }

  @Test
  public void testHeaderLinesNext_whenCheckingString_thenException() {
    HeaderIterator headerIterator = new HeaderIterator(ImmutableList.of()); // empty header

    assertThrows(IndexOutOfBoundsException.class, () ->
        headerIterator.next());
  }

  @Test
  public void testHeaderLinesPeek_whenCheckingString_thenContains() {
    int numHeaderLines = 5;
    String expectedHeaderLine = "##sampleHeaderLine=0"; 
    HeaderIterator headerIterator = createHeaderIterator(numHeaderLines);

    assertThat(headerIterator.peek()).matches(expectedHeaderLine);
    // peek shouldn't move the pointer
    assertThat(headerIterator.peek()).matches(expectedHeaderLine); 

  }

  @Test
  public void testHeaderLinesPeek_whenCheckingString_thenException() {
    HeaderIterator headerIterator = new HeaderIterator(ImmutableList.of()); // empty header

    assertThrows(IndexOutOfBoundsException.class, () -> 
        headerIterator.peek());
  }
}
