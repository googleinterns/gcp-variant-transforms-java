package com.google.gcp_variant_transforms.library;

import com.google.gcp_variant_transforms.library.HeaderIterator;
import static com.google.common.truth.Truth.*;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.*;

/**
 * Units tests for HeaderIterator.java
 */
public class HeaderIteratorTest {

  @Test
  public void testHeaderLinesHasNext_whenCheckingBoolean_thenTrue() {
    // Arrange
    int listSize = 5;
    ImmutableList.Builder<String> headerLinesBuilder = new ImmutableList.Builder<String>();
    ImmutableList<String> headerLines;
    for (int i = 0; i < listSize; i++) {
      headerLinesBuilder.add("##sampleHeaderLine=" + i);
    }
    headerLines = headerLinesBuilder.build();
    HeaderIterator headerIterator = new HeaderIterator(headerLines);
    // Act

    // Assert
    assertThat(headerIterator.hasNext()).isTrue();

  }
}