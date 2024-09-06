package com.gotocompany.firehose.config;

import org.aeonbits.owner.Config;

public interface GcsBlobConsumerConfig extends Config {
  @Config.Key("GCS_BLOB_SOURCE_BUCKET_NAME")
  String getSourceBlobBucketName();

  @Config.Key("GCS_BLOB_SOURCE_SERVICE_ACCOUNT")
  String getSourceBlobServiceAccount();

  @Config.Key("GCS_BLOB_SOURCE_PROJECT_ID")
  String getSourceBlobProjectId();

  @Config.Key("GCS_BLOB_SOURCE_PATH_PREFIX")
  String getSourcePathPrefix();

  @Config.Key("GCS_BLOB_ARCHIVE_PATH")
  String getSourceArchivePath();
}
