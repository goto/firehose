package com.gotocompany.firehose.serializer.constant;

import lombok.Getter;

@Getter
public enum TypecastTarget {
    INTEGER {
        @Override
        public Object getValue(String input) {
            return Integer.valueOf(input);
        }
    }, LONG {
        @Override
        public Object getValue(String input) {
            return Long.valueOf(input);
        }
    }, DOUBLE {
        @Override
        public Object getValue(String input) {
            return Double.valueOf(input);
        }
    };

    public abstract Object getValue(String input);
}
