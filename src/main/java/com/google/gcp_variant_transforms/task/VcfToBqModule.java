// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.task;

import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.BindingAnnotation;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.gcp_variant_transforms.beam.PipelineRunner;
import com.google.gcp_variant_transforms.beam.VcfToBqPipelineRunner;
import com.google.gcp_variant_transforms.library.LibraryModule;
import com.google.gcp_variant_transforms.options.VcfToBqOptions;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/** Guice Module for {@link VcfToBqTask}. */
public class VcfToBqModule extends AbstractModule {

  private final ImmutableList<String> args;

  public VcfToBqModule(String[] args) {
    this.args = ImmutableList.copyOf(args);
  }

  /** Annotation for command line arguments. */
  @Retention(RetentionPolicy.RUNTIME)
  @BindingAnnotation
  public @interface Args {}

  /**
   * Provides {@link VcfToBqOptions} from the command line arguments.
   *
   * Command line arguments are parsed through Apache Beam implementation directly.
   * @param args Command line arguments.
   * @return parsed {@link VcfToBqOptions}
   */
  @Provides
  @Singleton
  public VcfToBqOptions provideVcfToBqOptions(@Args ImmutableList<String> args) {
    // Guice will automatically fetch Args, which are populated in configure() function below, runs
    // them through Apache Beam Factory (after registering our own flags as well) and returns
    // implementation of our Options interface.
    PipelineOptionsFactory.register(VcfToBqOptions.class);
    return PipelineOptionsFactory
        .fromArgs(args.toArray(new String[args.size()]))
        .withValidation()
        .as(VcfToBqOptions.class);
  }

  @Override
  protected void configure() {
    install(new LibraryModule());
    bind(Task.class).to(VcfToBqTask.class);
    bind(PipelineRunner.class).to(VcfToBqPipelineRunner.class);
    bind(new TypeLiteral<ImmutableList<String>>() {})
        .annotatedWith(Args.class)
        .toInstance(this.args);
  }
}
