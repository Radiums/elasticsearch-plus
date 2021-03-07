package com.ra.elasticsearch.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Configuration
@EnableConfigurationProperties({ElasticsearchProperties.class})
public class ElasticsearchClientConfiguration {
    public static final String DEFAULT_SEPARATOR = ",";
    @Autowired
    private ElasticsearchProperties elasticsearchProperties;

    @Bean
    public RestHighLevelClient restHighLevelClient() {
        String hosts = elasticsearchProperties.getHosts();
        String[] hostArray = hosts.split(DEFAULT_SEPARATOR);
        String schema = elasticsearchProperties.getSchema();
        List<HttpHost> httpHostList = Arrays.stream(hostArray).map(host -> {
            String[] split = host.split(":");
            if (split.length == 2) {
                if (StringUtils.isEmpty(schema)) {
                    return new HttpHost(split[0], Integer.parseInt(split[1]));
                }
                return new HttpHost(split[0], Integer.parseInt(split[1]), schema);
            }
            return new HttpHost(split[0], 9200);
        }).collect(Collectors.toList());
        HttpHost[] httpHosts = new HttpHost[httpHostList.size()];
        RestClientBuilder builder = RestClient.builder(httpHostList.toArray(httpHosts));
        return new RestHighLevelClient(builder);
    }

}
