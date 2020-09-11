// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms.library;

import com.google.inject.AbstractModule;

/** Binds Library level test related classes. */
public class LibraryTestModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(VcfParser.class).to(VcfParserImpl.class);
    bind(BigQueryRowGenerator.class).to(BigQueryRowGeneratorImpl.class);
    bind(VariantToBqUtils.class).to(VariantToBqUtilsImpl.class);
    bind(SchemaGenerator.class).to(SchemaGeneratorImpl.class);
  }
}
