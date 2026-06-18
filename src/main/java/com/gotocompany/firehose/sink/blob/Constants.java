package com.gotocompany.firehose.sink.blob;

/**
 * Container for the enumerations shared by the blob sink.
 */
public class Constants {
    /**
     * Supported on-disk file formats for locally staged blob sink data.
     */
    public enum WriterType {
        /** Apache Parquet columnar format, the only format currently supported. */
        PARQUET,
    }

    /**
     * Time-based partitioning granularity for blob storage output paths.
     */
    public enum FilePartitionType {
        /** No time partitioning; all records share a single path prefix. */
        NONE,
        /** Partition output by calendar day. */
        DAY,
        /** Partition output by hour. */
        HOUR
    }
}
