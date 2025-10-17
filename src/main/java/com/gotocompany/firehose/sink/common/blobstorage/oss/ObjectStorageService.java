package com.gotocompany.firehose.sink.common.blobstorage.oss;

import com.aliyun.oss.ClientBuilderConfiguration;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.auth.DefaultCredentialProvider;
import com.aliyun.oss.common.comm.NoRetryStrategy;
import com.aliyun.oss.common.comm.SignVersion;
import com.aliyun.oss.model.BucketList;
import com.aliyun.oss.model.ListBucketsRequest;
import com.aliyun.oss.model.PutObjectRequest;
import com.gotocompany.firehose.config.ObjectStorageServiceConfig;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Optional;

@Slf4j
public class ObjectStorageService implements BlobStorage {

    private final OSS oss;
    private final String ossBucketName;
    private final String ossDirectoryPrefix;

    public ObjectStorageService(ObjectStorageServiceConfig objectStorageServiceConfig) {
        this(objectStorageServiceConfig, initializeOss(objectStorageServiceConfig));
    }

    public ObjectStorageService(ObjectStorageServiceConfig objectStorageServiceConfig, OSS oss) {
        this.oss = oss;
        this.ossBucketName = objectStorageServiceConfig.getOssBucketName();
        this.ossDirectoryPrefix = objectStorageServiceConfig.getOssDirectoryPrefix();
        
        log.info("Initializing OSS client - endpoint: {}, bucket: {}, directoryPrefix: {}", 
            objectStorageServiceConfig.getOssEndpoint(), 
            ossBucketName, 
            ossDirectoryPrefix);
        log.debug("OSS retry config - enabled: {}, maxAttempts: {}", 
            objectStorageServiceConfig.isRetryEnabled(), 
            objectStorageServiceConfig.getOssMaxRetryAttempts());
        
        logOssConfiguration(objectStorageServiceConfig);
        checkBucket();
    }

    protected static OSS initializeOss(ObjectStorageServiceConfig objectStorageServiceConfig) {
        ClientBuilderConfiguration clientBuilderConfiguration = new ClientBuilderConfiguration();
        clientBuilderConfiguration.setSignatureVersion(SignVersion.V4);
        clientBuilderConfiguration.setSocketTimeout(objectStorageServiceConfig.getOssSocketTimeoutMs());
        clientBuilderConfiguration.setConnectionTimeout(objectStorageServiceConfig.getOssConnectionTimeoutMs());
        clientBuilderConfiguration.setConnectionRequestTimeout(objectStorageServiceConfig.getOssConnectionRequestTimeoutMs());
        clientBuilderConfiguration.setRequestTimeout(objectStorageServiceConfig.getOssRequestTimeoutMs());
        if (objectStorageServiceConfig.isRetryEnabled()) {
            clientBuilderConfiguration.setMaxErrorRetry(objectStorageServiceConfig.getOssMaxRetryAttempts());
        } else {
            clientBuilderConfiguration.setRetryStrategy(new NoRetryStrategy());
        }
        return OSSClientBuilder.create()
                .endpoint(objectStorageServiceConfig.getOssEndpoint())
                .region(objectStorageServiceConfig.getOssRegion())
                .credentialsProvider(new DefaultCredentialProvider(objectStorageServiceConfig.getOssAccessId(),
                        objectStorageServiceConfig.getOssAccessKey()))
                .clientConfiguration(clientBuilderConfiguration)
                .build();
    }

    @Override
    public void store(String objectName, String filePath) throws BlobStorageException {
        File file = new File(filePath);
        long fileSize = file.exists() ? file.length() : 0;
        String builtPath = buildObjectPath(objectName);
        
        log.info("Starting OSS store operation - object: {}, filePath: {}, size: {} bytes", objectName, filePath, fileSize);
        log.debug("Built OSS object path: {}", builtPath);
        
        PutObjectRequest putObjectRequest = new PutObjectRequest(
                ossBucketName,
                builtPath,
                file
        );
        putObject(putObjectRequest, objectName, fileSize);
    }

    @Override
    public void store(String objectName, byte[] content) throws BlobStorageException {
        String builtPath = buildObjectPath(objectName);
        
        log.debug("Starting OSS store operation - object: {}, size: {} bytes", objectName, content.length);
        log.debug("Built OSS object path: {}", builtPath);
        
        PutObjectRequest putObjectRequest = new PutObjectRequest(
                ossBucketName,
                builtPath,
                new ByteArrayInputStream(content)
        );
        putObject(putObjectRequest, objectName, content.length);
    }

    private void putObject(PutObjectRequest putObjectRequest, String objectName, long contentSize) throws BlobStorageException {
        String builtPath = putObjectRequest.getKey();
        long startTime = System.currentTimeMillis();
        try {
            oss.putObject(putObjectRequest);
            long duration = System.currentTimeMillis() - startTime;
            String ossUrl = String.format("oss://%s/%s", ossBucketName, builtPath);
            log.info("Successfully uploaded to OSS - url: {}, size: {} bytes, duration: {}ms", 
                ossUrl, contentSize, duration);
            
            if (log.isDebugEnabled()) {
                boolean exists = oss.doesObjectExist(ossBucketName, builtPath);
                log.debug("OSS object existence verification - bucket: {}, key: {}, exists: {}", 
                    ossBucketName, builtPath, exists);
                if (!exists) {
                    log.warn("ALERT: Object reported as uploaded but verification failed - bucket: {}, key: {}", 
                        ossBucketName, builtPath);
                }
            }
        } catch (ClientException e) {
            String failureType = classifyClientException(e);
            long elapsedTime = System.currentTimeMillis() - startTime;
            log.error("Failed to put object to OSS (ClientException/{}) - bucket: {}, key: {}, object: {}, size: {} bytes, elapsedTime: {}ms, error: {}", 
                failureType, ossBucketName, builtPath, objectName, contentSize, elapsedTime, e.getMessage(), e);
            throw new BlobStorageException("client_error", e.getMessage(), e);
        } catch (OSSException e) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            log.error("Failed to put object to OSS (OSSException) - bucket: {}, key: {}, object: {}, size: {} bytes, errorCode: {}, errorMessage: {}, requestID: {}, hostID: {}, elapsedTime: {}ms", 
                ossBucketName, builtPath, objectName, contentSize, e.getErrorCode(), e.getErrorMessage(), e.getRequestId(), e.getHostId(), elapsedTime, e);
            throw new BlobStorageException(e.getErrorCode(), e.getErrorMessage(), e);
        }
    }

    private String buildObjectPath(String objectName) {
        return Optional.ofNullable(ossDirectoryPrefix)
                .map(prefix -> prefix + "/" + objectName)
                .orElse(objectName);
    }

    private String classifyClientException(ClientException e) {
        String msg = e.getMessage().toLowerCase();
        
        if (msg.contains("timeout") || msg.contains("timed out") || msg.contains("read timed out")) {
            return "TIMEOUT";
        }
        
        if (msg.contains("connection refused") || msg.contains("connect timed out") || msg.contains("connection reset")) {
            return "CONNECTION_ERROR";
        }
        
        if (msg.contains("socket") || msg.contains("broken pipe") || msg.contains("connection aborted")) {
            return "SOCKET_ERROR";
        }
        
        if (msg.contains("ssl") || msg.contains("certificate") || msg.contains("handshake")) {
            return "SSL_ERROR";
        }
        
        if (msg.contains("unknown host") || msg.contains("nodename nor servname provided") || msg.contains("name resolution")) {
            return "DNS_ERROR";
        }
        
        return "UNKNOWN";
    }

    private void checkBucket() {
        BucketList bucketList = oss.listBuckets(new ListBucketsRequest(ossBucketName,
                null, 1));
        if (bucketList.getBucketList().isEmpty()) {
            log.error("Bucket does not exist: {}", ossBucketName);
            log.error("Please create OSS bucket before running firehose: {}", ossBucketName);
            throw new IllegalArgumentException("Bucket does not exist");
        }
        log.info("Successfully validated OSS bucket: {}", ossBucketName);
    }
    
    private void logOssConfiguration(ObjectStorageServiceConfig config) {
        log.debug("OSS timeouts - socket: {}ms, connection: {}ms, connectionRequest: {}ms, request: {}ms", 
            config.getOssSocketTimeoutMs(), 
            config.getOssConnectionTimeoutMs(), 
            config.getOssConnectionRequestTimeoutMs(), 
            config.getOssRequestTimeoutMs());
        
        if (config.isRetryEnabled()) {
            log.debug("OSS retry strategy: ENABLED with maxRetryAttempts: {}", config.getOssMaxRetryAttempts());
        } else {
            log.debug("OSS retry strategy: DISABLED");
        }
    }

}
