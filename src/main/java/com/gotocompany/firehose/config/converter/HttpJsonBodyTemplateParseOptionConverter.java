package com.gotocompany.firehose.config.converter;

import com.jayway.jsonpath.Option;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class HttpJsonBodyTemplateParseOptionConverter implements Converter<Option> {
    @Override
    public Option convert(Method method, String input) {
        return Option.valueOf(input);
    }
}

