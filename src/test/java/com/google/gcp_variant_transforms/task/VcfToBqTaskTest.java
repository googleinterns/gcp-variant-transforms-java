// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.task;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.gcp_variant_transforms.beam.PipelineRunner;
import com.google.gcp_variant_transforms.library.HeaderReader;
import com.google.gcp_variant_transforms.library.VcfParser;
import com.google.gcp_variant_transforms.library.SchemaGenerator;
import com.google.gcp_variant_transforms.options.VcfToBqContext;
import com.google.gcp_variant_transforms.options.VcfToBqOptions;
import com.google.inject.Injector;
import htsjdk.variant.vcf.VCFHeader;
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

  @Mock private PipelineRunner mockedPipelineRunner;
  @Mock private VcfToBqOptions mockedVcfToBqOptions;
  @Mock private VcfToBqContext mockedVcfToBqContext;
  @Mock private VcfParser mockedVcfParser;
  @Mock private HeaderReader mockedHeaderReader;
  @Mock private SchemaGenerator mockedSchemaGenerator;
  @Mock private VCFHeader mockedVcfHeader;
  @Mock private TableSchema mockedTableSchema;

  @Captor private ArgumentCaptor<PipelineOptions> argCaptorPipelineOptions;
  @Captor private ArgumentCaptor<ImmutableList<String>> argCaptorImmutableList;

  @Before
  public void initializeSpyAndMocks() throws IOException{
    MockitoAnnotations.initMocks(this);
    vcfToBqTask = new VcfToBqTask(
        mockedPipelineRunner, 
        mockedHeaderReader, 
        mockedVcfToBqContext,
        mockedVcfParser,
        mockedVcfToBqOptions,
        mockedSchemaGenerator);
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
  public void testVcfTask_whenRun_thenVerifyCalls() throws IOException {
    ImmutableList.Builder<String> headerLines = new ImmutableList.Builder<String>();
    for (int i = 0; i < 5; i++) {
      headerLines.add("##sampleHeaderLine=" + i);
    }
    setUpRunBehaviors();
    spyVcfToBqTask.run();

    verify(spyVcfToBqTask).setPipelineOptions(mockedVcfToBqOptions);
    verify(mockedVcfToBqContext).setHeaderLines(headerLines.build());
    verify(mockedVcfToBqContext).setVCFHeader(mockedVcfHeader);
    verify(mockedVcfToBqContext).setBqSchema(mockedTableSchema);
    verify(mockedPipelineRunner).runPipeline();
  }
}
