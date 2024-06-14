package com.gotocompany.firehose.serializer.constant;

public enum TypecastTarget {
    INTEGER {
        @Override
        public Object cast(String input) {
            return Integer.valueOf(input);
        }
    }, LONG {
        @Override
        public Object cast(String input) {
            return Long.valueOf(input);
        }
    }, DOUBLE {
        @Override
        public Object cast(String input) {
            return Double.valueOf(input);
        }
    };

    public abstract Object cast(String input);
}
