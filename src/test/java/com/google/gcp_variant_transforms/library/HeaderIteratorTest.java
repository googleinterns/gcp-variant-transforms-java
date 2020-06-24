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
    headerIterator = new HeaderIterator(headerLinesBuilder.build());
    return headerIterator;
  }

  @Test
  public void testHeaderLinesHasNext_whenCheckingBoolean_thenTrue() {
    // Arrange
    int numHeaderLines = 5;
    HeaderIterator headerIterator = createHeaderIterator(numHeaderLines);

    // Act

    // Assert
    assertThat(headerIterator.hasNext()).isTrue();
  }

  @Test
  public void testHeaderLinesHasNext_whenCheckingBoolean_thenFalse() {
    // Arrange
    int numHeaderLines = 5;
    HeaderIterator headerIterator = createHeaderIterator(numHeaderLines);

    // Act
    for (int i = 0; i < numHeaderLines; i++) {
      headerIterator.next();
    }

    // Assert
    assertThat(headerIterator.hasNext()).isFalse();
  }

  @Test
  public void testHeaderLinesHasNext_whenEmptyCheckingBoolean_thenFalse() {
    // Arrange
    HeaderIterator headerIterator = new HeaderIterator(ImmutableList.of()); // empty header

    // Act

    // Assert
    assertThat(headerIterator.hasNext()).isFalse();
  }

  @Test
  public void testHeaderLinesNext_whenCheckingString_thenContains() {
    // Arrange
    int numHeaderLines = 5;
    CharSequence expectedHeaderLine1 = "##sampleHeaderLine=0";
    CharSequence expectedHeaderLine2 = "##sampleHeaderLine=1"; 
    HeaderIterator headerIterator = createHeaderIterator(numHeaderLines);

    // Act

    // Assert
    assertThat(headerIterator.next()).contains(expectedHeaderLine1);
    // next should move the pointer
    assertThat(headerIterator.next()).contains(expectedHeaderLine2);
  }

  @Test
  public void testHeaderLinesNext_whenCheckingString_thenException() {
    // Arrange
    HeaderIterator headerIterator = new HeaderIterator(ImmutableList.of()); // empty header
    Class expectedException = IndexOutOfBoundsException.class;

    // Act

    // Assert
    assertThrows(expectedException, () ->
        headerIterator.next());
  }

  @Test
  public void testHeaderLinesPeek_whenCheckingString_thenContains() {
    // Arrange
    int numHeaderLines = 5;
    CharSequence expectedHeaderLine = "##sampleHeaderLine=0"; 
    HeaderIterator headerIterator = createHeaderIterator(numHeaderLines);

    // Act

    // Assert
    assertThat(headerIterator.peek()).contains(expectedHeaderLine);
    // peek shouldn't move the pointer
    assertThat(headerIterator.peek()).contains(expectedHeaderLine); 

  }

  @Test
  public void testHeaderLinesPeek_whenCheckingString_thenException() {
    // Arrange
    HeaderIterator headerIterator = new HeaderIterator(ImmutableList.of()); // empty header
    Class expectedException = IndexOutOfBoundsException.class;

    // Act

    // Assert
    assertThrows(expectedException, () -> 
        headerIterator.peek());
  }
}
