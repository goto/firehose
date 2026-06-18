package com.gotocompany.firehose.serializer.constant;

/**
 * Enumerates the target types a string value can be cast to during JSON serialization.
 *
 * <p>Each constant implements {@link #cast(String)} to convert a string into the corresponding Java
 * type, and is used by {@link com.gotocompany.firehose.serializer.TypecastedJsonSerializer} to coerce
 * selected JSON fields to a configured type. The {@code INTEGER}, {@code LONG}, and {@code DOUBLE}
 * constants parse the input as that numeric type, while {@code STRING} returns the input unchanged.
 */
public enum TypecastTarget {
    /** Casts the input string to an {@link Integer}. */
    INTEGER {
        @Override
        public Object cast(String input) {
            try {
                return Integer.valueOf(input);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid input for INTEGER: " + input, e);
            }
        }
    }, LONG {
        @Override
        public Object cast(String input) {
            try {
                return Long.valueOf(input);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid input for LONG: " + input, e);
            }
        }
    }, DOUBLE {
        @Override
        public Object cast(String input) {
            try {
                return Double.valueOf(input);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid input for DOUBLE: " + input, e);
            }
        }
    }, STRING {
        @Override
        public Object cast(String input) {
            return String.valueOf(input);
        }
    };

    /**
     * Casts the given string to this constant's target type.
     *
     * @param input the string value to convert
     * @return the converted value as an {@link Object} of the target type
     * @throws IllegalArgumentException if the input cannot be parsed into the numeric target type
     */
    public abstract Object cast(String input);
}
