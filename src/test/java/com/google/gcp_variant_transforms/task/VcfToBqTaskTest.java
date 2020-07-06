// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.task;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.gcp_variant_transforms.beam.PipelineRunner;
import com.google.gcp_variant_transforms.library.HeaderReader;
import com.google.gcp_variant_transforms.options.VcfToBqContext;
import com.google.gcp_variant_transforms.options.VcfToBqOptions;
import com.google.inject.Injector;
import java.io.IOException;
import org.apache.beam.sdk.options.PipelineOptions;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.mockito.Mock;

/**
 * Units tests for VcfToBqTask.java
 */
 public class VcfToBqTaskTest {
  Injector spyInjector;
  VcfToBqTask vcfToBqTask;
  VcfToBqTask spyVcfToBqTask;

  @Mock
  PipelineRunner mockedPipelineRunner;

  @Mock
  VcfToBqOptions mockedVcfToBqOptions;

  @Mock 
  VcfToBqContext mockedVcfToBqContext;

  @Mock
  HeaderReader mockedHeaderReader;

  @Captor
  ArgumentCaptor<PipelineOptions> argCaptorPipelineOptions;

  @Captor
  ArgumentCaptor<ImmutableList<String>> argCaptorImmutableList;

  @Before
  public void initializeSpyAndMocks() throws IOException{
    MockitoAnnotations.initMocks(this);
    vcfToBqTask = new VcfToBqTask(
        mockedPipelineRunner, 
        mockedHeaderReader, 
        mockedVcfToBqContext, 
        mockedVcfToBqOptions);
    spyVcfToBqTask = spy(vcfToBqTask);
  }

  public void setUpRunBehaviors() throws IOException {
    ImmutableList.Builder<String> headerLinesBuilder = new ImmutableList.Builder<String>();
    for (int i = 0; i < 5; i++) {
      headerLinesBuilder.add("##sampleHeaderLine=" + i);
    }
    ImmutableList<String> headerLines = headerLinesBuilder.build();
    doReturn(headerLines).when(mockedHeaderReader).getHeaderLines();
    doNothing().when(spyVcfToBqTask).setPipelineOptions(argCaptorPipelineOptions.capture());
    doNothing().when(mockedVcfToBqContext).setHeaderLines(argCaptorImmutableList.capture());
    doNothing().when(mockedPipelineRunner).runPipeline();
  }

  @Test
  public void testVcfTaskConstructor_whenInitialize_thenNotNull() throws IOException {
    assertThat(vcfToBqTask).isNotNull();
    assertThat(spyVcfToBqTask).isNotNull();
  }

  @Test
  public void testVcfTask_whenRun_thenVerifySetPipelineOptions() throws IOException {
    setUpRunBehaviors();
    spyVcfToBqTask.run();

    verify(spyVcfToBqTask).setPipelineOptions(mockedVcfToBqOptions);    
  }

  @Test
  public void testVcfTask_whenRun_thenVerifySetHeaderLines() throws IOException {
    setUpRunBehaviors();
    spyVcfToBqTask.run();

    verify(mockedVcfToBqContext).setHeaderLines(argCaptorImmutableList.capture());  
  }

  @Test
  public void testVcfTask_whenRun_thenVerifyRunPipeline() throws IOException {
    setUpRunBehaviors();
    spyVcfToBqTask.run();

    verify(mockedPipelineRunner).runPipeline();  
  }

  @Test
  public void testVcfTask_whenSetPipelineOptions_thenVerify() throws Exception {
    doNothing().when(spyVcfToBqTask).setPipelineOptions(argCaptorPipelineOptions.capture());;
    spyVcfToBqTask.setPipelineOptions(mockedVcfToBqOptions);

    verify(spyVcfToBqTask).setPipelineOptions(argCaptorPipelineOptions.capture());
  }
}
