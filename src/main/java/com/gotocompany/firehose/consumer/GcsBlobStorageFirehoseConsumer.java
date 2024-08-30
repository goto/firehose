package com.gotocompany.firehose.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gotocompany.firehose.config.GcsBlobConsumerConfig;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.sink.Sink;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.tracer.SinkTracer;
import io.opentracing.Span;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class GcsBlobStorageFirehoseConsumer implements FirehoseConsumer {

  private final BlobStorage blobStorage;
  private final Sink sink;
  private final SinkTracer sinkTracer;
  private final GcsBlobConsumerConfig gcsBlobConsumerConfig;
  private final ObjectMapper objectMapper;

  public GcsBlobStorageFirehoseConsumer(BlobStorage blobStorage,
                                        Sink sink,
                                        SinkTracer sinkTracer,
                                        GcsBlobConsumerConfig gcsBlobConsumerConfig) {
    this.blobStorage = blobStorage;
    this.sink = sink;
    this.gcsBlobConsumerConfig = gcsBlobConsumerConfig;
    this.sinkTracer = sinkTracer;
    this.objectMapper = new ObjectMapper();
  }

  @Override
  public void process() throws IOException {
    List<String> fileNames = blobStorage.list(gcsBlobConsumerConfig.getSourcePathPrefix());

    for (String fileName : fileNames) {
      byte[] content = blobStorage.get(fileName);
      List<Message> messages = parseBlobToMessages(content);
      List<Span> spans = sinkTracer.startTrace(messages);
      sink.pushMessage(messages);
      sinkTracer.finishTrace(spans);
    }

  }

  private List<Message> parseBlobToMessages(byte[] content) throws IOException {
    InputStream inputStream = new ByteArrayInputStream(content);
    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
    String line = bufferedReader.readLine();
    List<Message> result = new ArrayList<>();
    while (line != null) {
      JsonNode node = objectMapper.readTree(line);
      String logKey = node.get("key").asText();
      String logMessage = node.get("value").asText();
      String topic = node.get("topic").asText();
      int partition = node.get("partition").asInt();
      long offset = node.get("offset").asLong();
      Message message = new Message(Base64.getDecoder().decode(logKey), Base64.getDecoder().decode(logMessage), topic, partition, offset);
      result.add(message);
      line = bufferedReader.readLine();
    }
    return result;
  }

  @Override
  public void close() throws IOException {
    sink.close();
  }
}
