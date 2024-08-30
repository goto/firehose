package com.gotocompany.firehose.config;

import org.aeonbits.owner.Config;

public interface BlobConsumerConfig extends Config {
  @Config.Key("BLOB_SOURCE_TYPE")
  String getSourceBlobBucketName();
}
