package com.gotocompany.firehose.serializer.constant;

import lombok.Getter;

@Getter
public enum NumericType {
    INTEGER {
        @Override
        public Number getValue(String input) {
            return Integer.valueOf(input);
        }
    }, LONG {
        @Override
        public Number getValue(String input) {
            return Long.valueOf(input);
        }
    };

    public abstract Number getValue(String input);
}
