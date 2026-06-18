package com.gotocompany.firehose.utils;

import com.gotocompany.firehose.config.AppConfig;
import com.timgroup.statsd.StatsDClient;
import com.gotocompany.stencil.SchemaUpdateListener;
import com.gotocompany.stencil.config.StencilConfig;

/**
 * Builds a Stencil {@link StencilConfig} from Firehose application configuration.
 *
 * <p>Stencil is the schema-registry client Firehose uses to fetch protobuf descriptors. This helper
 * maps the schema-registry settings (cache TTL and auto-refresh, fetch headers, retries, timeouts,
 * back-off, and refresh strategy) onto a {@link StencilConfig} and wires in the StatsD client and an
 * optional schema-update listener.
 */
public class StencilUtils {
    /**
     * Builds a Stencil configuration, including a schema-update listener.
     *
     * @param appconfig            the application config holding schema-registry settings
     * @param statsDClient         the StatsD client used by Stencil for metrics
     * @param schemaUpdateListener the listener notified when schemas refresh, may be {@code null}
     * @return the assembled Stencil configuration
     */
    public static StencilConfig getStencilConfig(
            AppConfig appconfig,
            StatsDClient statsDClient,
            SchemaUpdateListener schemaUpdateListener) {
        return StencilConfig.builder()
                .cacheAutoRefresh(appconfig.getSchemaRegistryStencilCacheAutoRefresh())
                .cacheTtlMs(appconfig.getSchemaRegistryStencilCacheTtlMs())
                .statsDClient(statsDClient)
                .fetchHeaders(appconfig.getSchemaRegistryFetchHeaders())
                .fetchBackoffMinMs(appconfig.getSchemaRegistryStencilFetchBackoffMinMs())
                .fetchRetries(appconfig.getSchemaRegistryStencilFetchRetries())
                .fetchTimeoutMs(appconfig.getSchemaRegistryStencilFetchTimeoutMs())
                .refreshStrategy(appconfig.getSchemaRegistryStencilRefreshStrategy())
                .updateListener(schemaUpdateListener)
                .build();
    }

    /**
     * Builds a Stencil configuration without a schema-update listener.
     *
     * @param appconfig    the application config holding schema-registry settings
     * @param statsDClient the StatsD client used by Stencil for metrics
     * @return the assembled Stencil configuration
     */
    public static StencilConfig getStencilConfig(AppConfig appconfig, StatsDClient statsDClient) {
        return getStencilConfig(appconfig, statsDClient, null);
    }
}
