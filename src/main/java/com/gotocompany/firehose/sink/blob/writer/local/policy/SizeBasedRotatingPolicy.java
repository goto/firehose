package com.gotocompany.firehose.sink.blob.writer.local.policy;

import com.gotocompany.firehose.sink.blob.writer.local.LocalFileMetadata;

import java.util.List;

public class SizeBasedRotatingPolicy implements WriterPolicy, GlobalWriterPolicy {

    private final long maxSize;

    public SizeBasedRotatingPolicy(long maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("The max size should be a positive integer");
        }
        this.maxSize = maxSize;
    }

    @Override
    public boolean shouldRotate(LocalFileMetadata metadata) {
        return metadata.getSize() >= maxSize;
    }

    @Override
    public boolean shouldRotate(List<LocalFileMetadata> metadataList) {
        long totalSize = metadataList.stream().map(LocalFileMetadata::getSize).reduce(0L, Long::sum);
        return totalSize >= maxSize;
    }
}
