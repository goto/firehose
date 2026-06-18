package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.serializer.constant.TypecastTarget;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.aeonbits.owner.Converter;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Owner {@link Converter} that parses the HTTP sink JSON serializer type-cast configuration into a
 * map of JSONPath expressions to type-casting functions.
 *
 * <p>The input is a JSON array of objects, each holding a {@code jsonPath} and a {@code type}. Every
 * entry becomes a map keyed by its JSONPath, whose value is a function that casts a string to the
 * configured target type. A blank input yields an empty map.
 */
public class HttpSinkSerializerJsonTypecastConfigConverter implements Converter<Map<String, Function<String, Object>>> {

    /**
     * Jackson mapper used to deserialize the JSON type-cast configuration.
     */
    private final ObjectMapper objectMapper;

    /**
     * Creates a converter with its own Jackson {@code ObjectMapper} for reading the JSON
     * configuration.
     */
    public HttpSinkSerializerJsonTypecastConfigConverter() {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Parses the JSON type-cast configuration into a map of JSONPath to casting function.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the JSON array describing the type casts; a blank value yields an empty map
     * @return a map keyed by JSONPath whose values cast a string to the configured target type
     * @throws IllegalArgumentException if the JSON cannot be parsed or an entry is missing its
     *     {@code jsonPath} or {@code type}
     */
    @Override
    public Map<String, Function<String, Object>> convert(Method method, String input) {
        if (StringUtils.isBlank(input)) {
            return Collections.emptyMap();
        }
        try {
            List<JsonTypecast> jsonTypecasts = objectMapper.readValue(input, new TypeReference<List<JsonTypecast>>() {
                    });
            validate(jsonTypecasts);
            return jsonTypecasts.stream()
                    .collect(Collectors.toMap(JsonTypecast::getJsonPath, jsonTypecast -> jsonTypecast.getType()::cast));
        } catch (IOException e) {
            throw new IllegalArgumentException("Error when parsing serializer json config: " + e.getMessage(), e);
        }
    }

    /**
     * Validates that every parsed entry declares both a JSONPath and a target type.
     *
     * @param jsonTypecasts the parsed type-cast entries to validate
     * @throws IllegalArgumentException if any entry has a {@code null} {@code jsonPath} or
     *     {@code type}
     */
    private void validate(List<JsonTypecast> jsonTypecasts) {
        boolean invalidConfigurationExist = jsonTypecasts.stream()
                .anyMatch(jt -> Objects.isNull(jt.getJsonPath()) || Objects.isNull(jt.getType()));
        if (invalidConfigurationExist) {
            throw new IllegalArgumentException("Invalid configuration: jsonPath or type should not be null");
        }
    }

    /**
     * Immutable representation of a single JSON type-cast rule, pairing a JSONPath expression with
     * the target type its matched value should be cast to.
     */
    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    @Builder
    private static class JsonTypecast {
        /**
         * JSONPath expression selecting the value to cast.
         */
        private String jsonPath;
        /**
         * Target type the selected value is cast to.
         */
        private TypecastTarget type;
    }

}
