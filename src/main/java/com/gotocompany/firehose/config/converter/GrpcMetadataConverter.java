package com.gotocompany.firehose.config.converter;

import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Arrays;
import io.grpc.Metadata;

public class GrpcMetadataConverter implements Converter<Metadata> {

    @Override
    public Metadata convert(Method method, String input) {

        Metadata metadata = new Metadata();

        Arrays.stream(input.split(",")).filter(metadataKeyValue -> !metadataKeyValue.trim().isEmpty()).forEach(metadataKeyValue -> {
            if (metadataKeyValue.contains(":")) {
                metadata.put(Metadata.Key.of(metadataKeyValue.split(":")[0], Metadata.ASCII_STRING_MARSHALLER), metadataKeyValue.split(":")[1]);
            }
        });
        return metadata;
    }
}
