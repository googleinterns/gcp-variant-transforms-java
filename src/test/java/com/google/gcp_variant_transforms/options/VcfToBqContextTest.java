// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.options;

import static com.google.common.truth.Truth.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableList;
import com.google.gcp_variant_transforms.options.VcfToBqContext;
import org.junit.*;

/**
 * Units tests for VcfToBqContext.java
 */
public class VcfToBqContextTest {

  public VcfToBqOptions create_mockedVcfToBqOptions(String inputFile, String output){
    VcfToBqOptions mockedVcfToBqOptions = mock(VcfToBqOptions.class);
    when(mockedVcfToBqOptions.getInputFile()).thenReturn(inputFile);
    when(mockedVcfToBqOptions.getOutput()).thenReturn(output);
    return mockedVcfToBqOptions;
  }

  public ImmutableList<String> createHeaderLines(){
    ImmutableList.Builder<String> headerLinesBuilder = new ImmutableList.Builder<String>();
    for (int i = 0; i < 10; i++) {
      headerLinesBuilder.add("##sampleHeaderLine=" + i);
    }
    return headerLinesBuilder.build();
  }

  @Test
  public void testVcfConstructor_whenCompareFields_thenMatches(){
    String inputFile = "sampleInputFile.txt";
    String output = "sampleOutputFile.txt";
    VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(inputFile, output);
    System.out.println(mockedVcfToBqOptions.getOutput());
    try{
      VcfToBqContext vcfToBqContext = new VcfToBqContext(mockedVcfToBqOptions);
    } catch (Exception e){
      System.out.println("caught an error!");
    }

    assertThat(vcfToBqContext.getInputFile()).matches(inputFile);
    assertThat(vcfToBqContext.getOutput()).matches(output);
    assertThat(vcfToBqContext.getHeaderLines()).isNull();
  }

  @Test
  public void testVcfContext_whenValidateFlags_thenException(){

  }

  @Test
  public void testVcfContext_whenGetInputFile_thenMatches(){
    String inputFile = "sampleInputFile.txt";
    String output = "sampleOutputFile.txt";
    VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(inputFile, output);
    try{
      VcfToBqContext vcfToBqContext = new VcfToBqContext(mockedVcfToBqOptions);
    } catch (Exception e){
      System.out.println("caught an error!");
    }
    
    assertThat(vcfToBqContext.getInputFile()).matches(inputFile);
  }

  @Test
  public void testVcfContext_whenGetInputFile_thenNull(){
    String output = "sampleOutputFile.txt";
    VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(null, output);
    try{
      VcfToBqContext vcfToBqContext = new VcfToBqContext(mockedVcfToBqOptions);
    } catch (Exception e){
      System.out.println("caught an error!");
    }
    
    assertThat(vcfToBqContext.getInputFile()).isNull();
  }

  @Test
  public void testVcfContext_whenGetOutput_thenMatches(){
    String inputFile = "sampleInputFile.txt";
    String output = "sampleOutputFile.txt";
    VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(inputFile, output);
    try{
      VcfToBqContext vcfToBqContext = new VcfToBqContext(mockedVcfToBqOptions);
    } catch (Exception e){
      System.out.println("caught an error!");
    }

    assertThat(vcfToBqContext.getOutput()).matches(output);
  }

  @Test
  public void testVcfContext_whenGetOutput_thenNull(){
    String inputFile = "sampleInputFile.txt";
    VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(inputFile, null);
    try{
      VcfToBqContext vcfToBqContext = new VcfToBqContext(mockedVcfToBqOptions);
    } catch (Exception e){
      System.out.println("caught an error!");
    }

    assertThat(vcfToBqContext.getOutput()).isNull();
  }

  @Test
  public void testVcfContext_whenSetHeaderLines_thenMatches(){
    String inputFile = "sampleInputFile.txt";
    String output = "sampleOutputFile.txt";
    VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(inputFile, output);
    try{
      VcfToBqContext vcfToBqContext = new VcfToBqContext(mockedVcfToBqOptions);
    } catch (Exception e){
      System.out.println("caught an error!");
    }
    ImmutableList<String> headerLines = createHeaderLines();
    vcfToBqContext.setHeaderLines(headerLines);

    assertThat(vcfToBqContext.getHeaderLines()).containsExactlyElementsIn(headerLines);
  }

  @Test
  public void testVcfContext_whenSetHeaderLines_thenNull(){
    String inputFile = "sampleInputFile.txt";
    String output = "sampleOutputFile.txt";
    VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(inputFile, output);
    try{
      VcfToBqContext vcfToBqContext = new VcfToBqContext(mockedVcfToBqOptions);
    } catch (Exception e){
      System.out.println("caught an error!");
    }
    vcfToBqContext.setHeaderLines(null);

    assertThat(vcfToBqContext.getHeaderLines()).isNull();
  }

  @Test
  public void testVcfContext_whenGetHeaderLines_thenMatches(){
    String inputFile = "sampleInputFile.txt";
    String output = "sampleOutputFile.txt";
    VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(inputFile, output);
    try{
      VcfToBqContext vcfToBqContext = new VcfToBqContext(mockedVcfToBqOptions);
    } catch (Exception e){
      System.out.println("caught an error!");
    }
    ImmutableList<String> headerLines = createHeaderLines();
    vcfToBqContext.setHeaderLines(headerLines);

    assertThat(vcfToBqContext.getHeaderLines()).containsExactlyElementsIn(headerLines);
  }

  @Test
  public void testVcfContext_whenGetHeaderLines_thenNull(){
    String inputFile = "sampleInputFile.txt";
    String output = "sampleOutputFile.txt";
    VcfToBqOptions mockedVcfToBqOptions = create_mockedVcfToBqOptions(inputFile, output);
    try{
      VcfToBqContext vcfToBqContext = new VcfToBqContext(mockedVcfToBqOptions);
    } catch (Exception e){
      System.out.println("caught an error!");
    }

    assertThat(vcfToBqContext.getHeaderLines()).isNull();
  }

}
