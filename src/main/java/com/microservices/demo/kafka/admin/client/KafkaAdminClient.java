package com.microservices.demo.kafka.admin.client;

import com.microservices.demo.config.KafkaConfigData;
import com.microservices.demo.config.RetryConfigData;
import com.microservices.demo.kafka.admin.exceptions.KafkaAdminClientException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
public class KafkaAdminClient {
    private final static Logger LOGGER = LoggerFactory.getLogger(KafkaAdminClient.class);

    private final KafkaConfigData kafkaConfigData;
    private final RetryConfigData retryConfigData;
    private final AdminClient adminClient;
    private final RetryTemplate retryTemplate;
    private final WebClient webClient;

    public KafkaAdminClient(KafkaConfigData kafkaConfigData, RetryConfigData retryConfigData, AdminClient adminClient,
                            RetryTemplate retryTemplate, WebClient webClient) {
        this.kafkaConfigData = kafkaConfigData;
        this.retryConfigData = retryConfigData;
        this.adminClient = adminClient;
        this.retryTemplate = retryTemplate;
        this.webClient = webClient;
    }

    public void createTopic() {
        CreateTopicsResult result;
        try {
            result = retryTemplate.execute(this::doCreateTopic);
        } catch (Throwable e) {
            throw new KafkaAdminClientException("Error while creating the topics, or reached max retries while creating one", e);
        }
        checkTopicCreated();
    }

    public void checkTopicCreated() {
        Collection<TopicListing> topics = getTopics();
        int retryCount = 1;
        long sleepTimeMs = retryConfigData.getSleepTimeMs();
        int maxRetry = retryConfigData.getMaxAttempts();
        int multiplier = retryConfigData.getMultiplier().intValue();

        for (String topic: kafkaConfigData.getTopicNamesToCreate()) {
            while (!isTopiCreated(topics, topic)) {
                checkMaxRetry(retryCount++, maxRetry);
                sleep(sleepTimeMs);
                sleepTimeMs *= multiplier;
                topics = getTopics();
            }
        }
    }

    private void sleep(long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new KafkaAdminClientException("Caught an exception while sleeping",e);
        }
    }

    private void checkMaxRetry(int retryCount, int maxRetry) {
        if (retryCount > maxRetry) {
            throw new KafkaAdminClientException("Maximum Retry reached while trying to create topics");
        }
    }

    private boolean isTopiCreated(Collection<TopicListing> topics, String topic) {
        if (topics == null) {
            return false;
        }
        return topics.stream().anyMatch(topicListing -> topicListing.name().equals(topic));
    }

    private CreateTopicsResult doCreateTopic(RetryContext retryContext) {
        List<String> topicNames = kafkaConfigData.getTopicNamesToCreate();
        LOGGER.info("Creating {} topic(s), attempt {}", topicNames.size(), retryContext.getRetryCount());
        List<NewTopic> newTopics = topicNames.stream().map(topic -> new NewTopic(
                topic.trim(),
                kafkaConfigData.getNumOfPartitions(),
                kafkaConfigData.getReplicationFactor()
        )).toList();
        return adminClient.createTopics(newTopics);
    }

    private Collection<TopicListing> getTopics() {
        Collection<TopicListing> topicListings;
        try {
            topicListings = retryTemplate.execute(this::doGetTopics);
        } catch (Throwable e) {
            throw new KafkaAdminClientException("Error getting the topic or reached maximum attempts while fetching them", e);
        }
        return topicListings;
    }

    private Collection<TopicListing> doGetTopics(RetryContext retryContext)
            throws ExecutionException, InterruptedException {
        LOGGER.info("Reading kafka topics {}, attempt {}", kafkaConfigData.getTopicName(), retryContext.getRetryCount());
        Collection<TopicListing> topics = adminClient.listTopics().listings().get();
        for (TopicListing topic: topics) {
            LOGGER.info("Found Topic with name {}", topic.name());
        }
        return topics;
    }

    public void checkSchemaRegistry() {
        int retryCount = 1;
        int multiplier = retryConfigData.getMultiplier().intValue();
        int maxRetry = retryConfigData.getMaxAttempts();
        long sleepTimeMs = retryConfigData.getSleepTimeMs();
        while(!getSchemaRegistryStatus().is2xxSuccessful()) {
            checkMaxRetry(retryCount++, maxRetry);
            sleep(sleepTimeMs);
            sleepTimeMs *=  multiplier;
        }
    }

    private HttpStatusCode getSchemaRegistryStatus() {
            return webClient
                    .method(HttpMethod.GET)
                    .uri(kafkaConfigData.getSchemaRegistryUrl())
                    .exchangeToMono(Mono::just)
                    .map(ClientResponse::statusCode)
                    .block();
    }
}
