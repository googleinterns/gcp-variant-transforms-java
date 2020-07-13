// Copyright 2020 Google LLC

package com.google.gcp_variant_transforms;

import com.google.gcp_variant_transforms.library.VcfParser;
import com.google.gcp_variant_transforms.library.VcfParserImpl;
import com.google.guiceberry.GuiceBerryModule;
import com.google.inject.AbstractModule;

public class TestEnv extends AbstractModule {

  @Override
  protected void configure() {
    install(new GuiceBerryModule());

    bind(VcfParser.class).to(VcfParserImpl.class);
  }
}
