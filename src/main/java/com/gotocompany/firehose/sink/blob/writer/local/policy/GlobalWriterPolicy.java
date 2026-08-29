package com.gotocompany.firehose.sink.blob.writer.local.policy;

import com.gotocompany.firehose.sink.blob.writer.local.LocalFileMetadata;

import java.util.List;

public interface GlobalWriterPolicy {

    boolean shouldRotate(List<LocalFileMetadata> metadata);
}
