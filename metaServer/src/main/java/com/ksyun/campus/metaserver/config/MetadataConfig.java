package com.ksyun.campus.metaserver.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "metadata")
public class MetadataConfig {
    
    private StorageConfig storage;
    
    @Data
    public static class StorageConfig {
        private String type;
        private String path;
        private BackupConfig backup;
    }
    
    @Data
    public static class BackupConfig {
        private boolean enabled;
        private String path;
        private long interval;
    }
}
