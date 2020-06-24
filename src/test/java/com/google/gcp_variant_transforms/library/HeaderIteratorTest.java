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
    int numHeaderLines = 0;
    HeaderIterator headerIterator = createHeaderIterator(numHeaderLines);

    // Act

    // Assert
    assertThat(headerIterator.hasNext()).isFalse();
  }

  @Test
  public void testHeaderLinesNext_whenCheckingString_thenContains() {
    // Arrange
    int numHeaderLines = 5;
    CharSequence expectedHeaderLine = "##sampleHeaderLine=0"; 
    HeaderIterator headerIterator = createHeaderIterator(numHeaderLines);

    // Act

    // Assert
    assertThat(headerIterator.next()).contains(expectedHeaderLine);
  }

  @Test
  public void testHeaderLinesNext_whenCheckingString_thenException() {
    // Arrange
    int numHeaderLines = 0;
    HeaderIterator headerIterator = createHeaderIterator(numHeaderLines);
    Exception actualException;
    Class expectedException = IndexOutOfBoundsException.class;

    // Act
    actualException = assertThrows(IndexOutOfBoundsException.class, () ->
        headerIterator.next());

    // Assert
    assertThat(actualException).isInstanceOf(expectedException);
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
  }

  @Test
  public void testHeaderLinesPeek_whenCheckingString_thenException() {
    // Arrange
    int numHeaderLines = 0;
    HeaderIterator headerIterator = createHeaderIterator(numHeaderLines);
    Exception actualException;
    Class expectedException = IndexOutOfBoundsException.class;

    // Act
    actualException = assertThrows(IndexOutOfBoundsException.class, () ->
        headerIterator.peek());

    // Assert
    assertThat(actualException).isInstanceOf(expectedException);
  }
}
