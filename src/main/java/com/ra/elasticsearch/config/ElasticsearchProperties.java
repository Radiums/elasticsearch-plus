package com.ra.elasticsearch.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "es")
public class ElasticsearchProperties {
    @Value("${hosts:127.0.0.1:9200}")
    private String hosts;
    @Value("${schema:http}")
    private String schema;
    @Value("${concurrencyLevel:20}")
    private int concurrencyLevel;
    @Value("${refreshInterval:1s}")
    private String refreshInterval;
    @Value("${refreshInterval:true}")
    private String refreshPolicy;

    public String getHosts() {
        return hosts;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public int getConcurrencyLevel() {
        return concurrencyLevel;
    }

    public void setConcurrencyLevel(int concurrencyLevel) {
        this.concurrencyLevel = concurrencyLevel;
    }

    public String getRefreshInterval() {
        return refreshInterval;
    }

    public void setRefreshInterval(String refreshInterval) {
        this.refreshInterval = refreshInterval;
    }

    public String getRefreshPolicy() {
        return refreshPolicy;
    }

    public void setRefreshPolicy(String refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
    }
}
