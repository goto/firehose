package com.gotocompany.firehose.config.converter;

import java.lang.reflect.Method;

import org.aeonbits.owner.Converter;

import com.gotocompany.stencil.cache.SchemaRefreshStrategy;

/**
 * Owner {@link Converter} that resolves the Stencil schema-registry refresh configuration string
 * into a {@link com.gotocompany.stencil.cache.SchemaRefreshStrategy}.
 *
 * <p>The strategy controls how the Stencil client keeps its cached protobuf descriptors current.
 * The value {@code VERSION_BASED_REFRESH} (matched case-insensitively) selects the version-based
 * strategy; any other value, including an unset one, falls back to the long-polling strategy.
 */
public class SchemaRegistryRefreshConverter implements Converter<SchemaRefreshStrategy> {

    /**
     * Converts the raw configuration value into a
     * {@link com.gotocompany.stencil.cache.SchemaRefreshStrategy}.
     *
     * @param method the owner accessor being resolved; not used by this converter
     * @param input the raw value; {@code VERSION_BASED_REFRESH} (any case) selects version-based
     *     refresh, everything else selects long polling
     * @return the version-based refresh strategy when the input names it, otherwise the long-polling
     *     strategy
     */
    @Override
    public SchemaRefreshStrategy convert(Method method, String input) {
        if ("VERSION_BASED_REFRESH".equalsIgnoreCase(input)) {
            return SchemaRefreshStrategy.versionBasedRefresh();
        }
        return SchemaRefreshStrategy.longPollingStrategy();
    }
}
