package com.github.dzlog.kafka;

import com.gitee.bee.core.conf.BeeConfigClient;
import com.github.dzlog.kafka.consumer.KafkaReceiver;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static com.github.dzlog.DzlogConf.DZLOG_DATA_CENTER_KAFKA_AUTO_OFFSET_RESET;
import static com.github.dzlog.DzlogConf.DZLOG_DATA_CENTER_KAFKA_BROKERS;

/**
 * KafkaBootConfig
 */
@Configuration
public class KafkaBootConfig implements InitializingBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaBootConfig.class);

	public static final int THREAD_NUM = Runtime.getRuntime().availableProcessors() * 2;

	private static final String KAFKA_AUTO_OFFSET_RESET = "latest";

	private static final Pattern PATTERN_TOPIC = Pattern.compile("dzlog-.*");

	@Autowired
	private BeeConfigClient configClient;

	@Value("${dzlog.datacenter}")
    private String dataCenter;

	@Value("${dzlog.kafka.cluster.name}")
	private String kafkaClusterCode;

	private int threadCount = THREAD_NUM;

	private String kafkaAutoOffsetReset = KAFKA_AUTO_OFFSET_RESET;

	@Override
	public void afterPropertiesSet() throws Exception {
		Map<String, String> map = configClient.getMapString(DZLOG_DATA_CENTER_KAFKA_AUTO_OFFSET_RESET);
		kafkaAutoOffsetReset = map.getOrDefault(dataCenter, "latest");

		if (!("earliest".equals(kafkaAutoOffsetReset) || "latest".equals(kafkaAutoOffsetReset))) {
			kafkaAutoOffsetReset = KAFKA_AUTO_OFFSET_RESET;
		}
		LOGGER.info("kafkaCluster: {}, kafkaAutoOffsetReset: {},  kafka 消费线程数：{}",
				kafkaClusterCode, kafkaAutoOffsetReset, threadCount);
	}

	private ConsumerFactory<String, ByteBuffer> buildConsumerFactory(String brokerList, String groupId) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaAutoOffsetReset);
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
		props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 1000 * 60);

		return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new ByteBufferDeserializer());
	}

	private ConcurrentKafkaListenerContainerFactory<String, ByteBuffer> buildContainerFactory(KafkaReceiver receiver) {

		DzConcurrentKafkaListenerContainerFactory<String, ByteBuffer> factory = new DzConcurrentKafkaListenerContainerFactory();

		Map<String, String> map = configClient.getMapString(DZLOG_DATA_CENTER_KAFKA_BROKERS);
		String brokers = map.get(dataCenter);

		ConsumerFactory<String, ByteBuffer> consumerFactory = buildConsumerFactory(brokers, "dzlog-" + dataCenter);
		factory.setConsumerFactory(consumerFactory);
		factory.setConcurrency(threadCount);
		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
		factory.getContainerProperties().setIdleEventInterval(30000L);
		factory.getContainerProperties().setMessageListener(receiver);
		factory.setBatchListener(true);
		LOGGER.info("ContainerFactory of {} has initialed", dataCenter);

		return factory;
	}

	@Bean("kafkaListenerContainerFactory")
	public ConcurrentKafkaListenerContainerFactory<String, ByteBuffer> kafkaListenerContainerFactory(KafkaReceiver receiver) {
		return buildContainerFactory(receiver);
	}

	private static class DzConcurrentKafkaListenerContainerFactory<K, V> extends ConcurrentKafkaListenerContainerFactory {

		@Override
		protected ConcurrentMessageListenerContainer<K, V> createContainerInstance(KafkaListenerEndpoint endpoint) {
			ContainerProperties properties = new ContainerProperties(PATTERN_TOPIC);
			return new ConcurrentMessageListenerContainer<K, V>(getConsumerFactory(), properties);
		}

	}
}
