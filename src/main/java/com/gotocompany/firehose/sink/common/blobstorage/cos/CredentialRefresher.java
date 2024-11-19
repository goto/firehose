package com.gotocompany.firehose.sink.common.blobstorage.cos;

import com.gotocompany.firehose.config.COSConfig;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.auth.COSSessionCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CredentialRefresher {
    private static final Logger LOGGER = LoggerFactory.getLogger(CredentialRefresher.class);
    private static final long MILLIS_IN_A_SECOND = 1000L; // milliseconds conversion factor

    private final COSConfig cosConfig;
    private final COSClient cosClient;
    private final CredentialProvider credentialProvider;
    private final RefreshTask refreshTask;
    private volatile long lastRefreshTimestamp;

    public CredentialRefresher(COSConfig cosConfig, COSClient cosClient, CredentialProvider credentialProvider) {
        this.cosConfig = cosConfig;
        this.cosClient = cosClient;
        this.refreshTask = new RefreshTask();
        this.credentialProvider = credentialProvider;
        this.lastRefreshTimestamp = System.currentTimeMillis();
    }

    /**
     * Performs a soft refresh of the credentials.
     * If the time elapsed since the last refresh is greater than the credential's validity period,
     * a new credential is fetched. Otherwise, the current credential is reused.
     */
    public void softRefreshCredential() {
        if (checkSoftRefreshTimestamp()) {
            synchronized (refreshTask) {
                if (checkSoftRefreshTimestamp()) {
                    refreshTask.isRunning = true;
                    Thread refreshThread = new Thread(refreshTask);
                    refreshThread.setName("sts-refresh");
                    refreshThread.setDaemon(true);
                    refreshThread.start();
                }
            }
        }
    }

    private boolean checkSoftRefreshTimestamp() {
        return System.currentTimeMillis() - lastRefreshTimestamp
                > cosConfig.getCOSTempCredentialValiditySeconds() * MILLIS_IN_A_SECOND && !refreshTask.isRunning;
    }

    private class RefreshTask implements Runnable {
        private volatile boolean isRunning;

        @Override
        public void run() {
            doRefreshTask();
        }

        public void doRefreshTask() {
            try {
                lastRefreshTimestamp = System.currentTimeMillis();
                COSSessionCredentials cred = credentialProvider.getCOSCredentials();
                cosClient.setCOSCredentials(cred);
            } catch (Exception e) {
                LOGGER.error("refresh credential failed.", e);
            } finally {
                isRunning = false;
            }
        }
    }
}
