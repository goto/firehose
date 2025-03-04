package com.gotocompany.firehose.sink.common.blobstorage.cos.auth;

import com.gotocompany.firehose.config.CloudObjectStorageConfig;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.auth.COSSessionCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenLifecycleManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(TokenLifecycleManager.class);
    private static final long MILLIS_IN_A_SECOND = 1000L;

    private final CloudObjectStorageConfig cloudObjectStorageConfig;
    private final COSClient cosClient;
    private final SessionTokenGenerator sessionTokenGenerator;
    private final RefreshTask refreshTask;
    private volatile long lastRefreshTimestamp;

    public TokenLifecycleManager(CloudObjectStorageConfig cloudObjectStorageConfig, COSClient cosClient, SessionTokenGenerator sessionTokenGenerator) {
        this.cloudObjectStorageConfig = cloudObjectStorageConfig;
        this.cosClient = cosClient;
        this.refreshTask = new RefreshTask();
        this.sessionTokenGenerator = sessionTokenGenerator;
        this.lastRefreshTimestamp = System.currentTimeMillis();
    }

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
                > cloudObjectStorageConfig.getCOSTempCredentialValiditySeconds() * MILLIS_IN_A_SECOND && !refreshTask.isRunning;
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
                COSSessionCredentials cred = sessionTokenGenerator.getCOSCredentials();
                cosClient.setCOSCredentials(cred);
            } catch (Exception e) {
                LOGGER.error("refresh credential failed.", e);
            } finally {
                isRunning = false;
            }
        }
    }
}
