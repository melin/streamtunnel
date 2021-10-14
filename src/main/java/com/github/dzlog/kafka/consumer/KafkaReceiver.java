package com.github.dzlog.kafka.consumer;

import com.gitee.bee.core.conf.BeeConfigClient;
import com.github.dzlog.entity.LogCollectConfig;
import com.github.dzlog.kafka.LogEvent;
import com.github.dzlog.kafka.TopicConsumerInfo;
import com.github.dzlog.service.HivePartitionService;
import com.github.dzlog.service.LogCollectConfigService;
import com.github.dzlog.util.ThreadUtils;
import com.github.dzlog.util.TimeUtils;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.BatchConsumerAwareMessageListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.github.dzlog.DzlogConf.DZLOG_DATA_CENTER_CONSUMER_RATE_LIMITER;

@Service
public class KafkaReceiver implements ConsumerSeekAware, ApplicationContextAware,
		BatchConsumerAwareMessageListener<Integer, ByteBuffer>, InitializingBean, DisposableBean {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReceiver.class);

	private static final Logger TROUBLE_LOGGER = LoggerFactory.getLogger("troubleLogger");

	@Autowired
	private LogCollectConfigService dcLogCollectService;

    @Autowired
	private BeeConfigClient configClient;

	@Autowired
	private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

	@Autowired
	private HivePartitionService hivePartitionService;

	private ApplicationContext applicationContext;

	private volatile RateLimiter rateLimiter = null;

	private int lastPermitsPerSecond = 0;

	@Value("${dzlog.datacenter}")
	private String dataCenter;

	private ThreadLocal<KafkaReceiveHandler> receiverHandler = new ThreadLocal<>();

	protected Map<Long, KafkaReceiveHandler> threadToHandlerMap = new ConcurrentHashMap<>();

	/**
	 * topic 分区对应上一个 hive 分区值
	 */
	protected Map<String, String> topicPartitionLastHivePartition = new ConcurrentHashMap<>();

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (StringUtils.isBlank(dataCenter)) {
			throw new IllegalArgumentException("dataCenter can not empty");
		}

		ScheduledExecutorService executorService = ThreadUtils.newDaemonSingleThreadScheduledExecutor("update-rateLimiter");
		executorService.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				Map<String, Integer> map = configClient.getMapInteger(DZLOG_DATA_CENTER_CONSUMER_RATE_LIMITER);
				int permitsPerSecond = map.getOrDefault(dataCenter, -1);
				if (permitsPerSecond > 0) {
					if (rateLimiter != null) {
						if (lastPermitsPerSecond != permitsPerSecond) {
							rateLimiter.setRate(permitsPerSecond);
							TROUBLE_LOGGER.info("[1]更新 RateLimiter：{}", permitsPerSecond);
						}
					} else {
						rateLimiter = RateLimiter.create(permitsPerSecond);
						TROUBLE_LOGGER.info("[2]更新 RateLimiter：{}", permitsPerSecond);
					}
				} else {
					rateLimiter = null;
				}

				lastPermitsPerSecond = permitsPerSecond;
			}
		}, 5, 5, TimeUnit.SECONDS);
	}

	@Override
    @KafkaListener(containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(List<ConsumerRecord<Integer, ByteBuffer>> list, Consumer<?, ?> consumer) {
        try {
            receiveMessage(list, consumer);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
	}

    @Override
    public void destroy() throws Exception {
		TROUBLE_LOGGER.info("pause consume kafka message");
	    for (MessageListenerContainer container : kafkaListenerEndpointRegistry.getListenerContainers()) {
		    container.pause();
	    }

	    TimeUnit.SECONDS.sleep(5);

		TROUBLE_LOGGER.info("prepare to clear remain topic file");
        for (KafkaReceiveHandler handler: threadToHandlerMap.values()) {
            handler.clearRemainTopic();
        }
    }

	private LogEvent createLogEvent(ConsumerRecord<Integer, ByteBuffer> record) {
		if (rateLimiter != null) {
			rateLimiter.acquire();
		}

		try {
			String topic = record.topic();
			if (StringUtils.isBlank(topic)) {
				return null;
			}

			String code = dcLogCollectService.getCodeByTopic(topic);
			if (StringUtils.isBlank(code)) {
				return null;
			}

			LogEvent logEvent = new LogEvent();

			logEvent.setReceivedTime(record.timestamp());

			ByteBuffer byteBuffer = record.value();
			byteBuffer.position(0);
			logEvent.setMsgByteBuffer(byteBuffer);

			Integer partition = record.partition();
			Long offset = record.offset();

			logEvent.setTopic(topic);
			logEvent.setPartition(partition);
			logEvent.setOffset(offset);
			logEvent.setCode(code);

			return logEvent;
		} catch (Exception e) {
			return null;
		}
	}

	protected void receiveMessage(List<ConsumerRecord<Integer, ByteBuffer>> list, Consumer<?, ?> consumer) {
		KafkaReceiveHandler handler = receiverHandler.get();
		if (handler.getConsumer() == null) {
			handler.setConsumer(consumer);
		}

		String currentHivePartition = TimeUtils.getCurrentHivePartition();
        //到达新的分区
		if (!StringUtils.equals(currentHivePartition, handler.getCurrentPartition())) {
			for (String partitionName : handler.getPartitionToTopicConsumerInfoMap().keySet()) {
				handler.flushTopic("new", partitionName);
			}

			handler.setCurrentPartition(currentHivePartition);
		}

		for (ConsumerRecord<Integer, ByteBuffer> record : list) {
			final String topicPartition = record.topic() + "-" + record.partition();

			LogEvent logEvent = createLogEvent(record);
			if (logEvent == null) {
				continue;
			}

			LogCollectConfig collectConfig = dcLogCollectService.getDcLogByCode(logEvent.getCode());
			if (collectConfig == null) {
				LOGGER.error("采集不存在: {}", logEvent.getCode());
				continue;
			}

			changeHivePartition(collectConfig, topicPartition, currentHivePartition);

			if (handler.appendEvent(logEvent, currentHivePartition)) {
				handler.updateTopicPartition(logEvent);
				if (handler.checkTopicForCommit(topicPartition)) {
					handler.flushTopic("commit", topicPartition);
				}
			} else {
				handler.rollBackTopicPartition(topicPartition);
			}
		}
	}

	/**
	 * kafka 客户端partition 平衡重新分配操作，当前线程已经消费数据没有提交，需要清理掉。消费新分配partition数据。
	 * @param assignments
	 * @param consumerSeekCallback
	 */
	@Override
	public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback consumerSeekCallback) {
		KafkaReceiveHandler handler = receiverHandler.get();
		long threadId = Thread.currentThread().getId();
		if (handler != null) {
			IOUtils.closeQuietly(handler);
			receiverHandler.remove();
			threadToHandlerMap.remove(threadId);
		}

		if (!assignments.isEmpty()) {
			TROUBLE_LOGGER.info("thread {} assigned for topic partition {}",
					threadId, StringUtils.join(assignments.keySet(), ","));

			handler = new KafkaReceiveHandler(consumerSeekCallback, applicationContext);
			String currentHivePartition = TimeUtils.getCurrentHivePartition();
			handler.setCurrentPartition(currentHivePartition);

			handler.initAssignment(assignments);
			receiverHandler.set(handler);
			threadToHandlerMap.putIfAbsent(threadId, handler);

			assignments.entrySet().forEach(topicPartition ->
					topicPartitionLastHivePartition.put(topicPartition.toString(), currentHivePartition));
		}
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		if (!partitions.isEmpty()) {
			long threadId = Thread.currentThread().getId();
			TROUBLE_LOGGER.info("thread {} revoked assigned for topic partition {}",
					threadId, StringUtils.join(partitions, ","));
		}
	}

	/**
	 * kafka client 长时间没有消息接收，需要提交已经接收文件，以及kafka offset
	 * @param map
	 * @param consumerSeekCallback
	 */
	@Override
	public void onIdleContainer(Map<TopicPartition, Long> map, ConsumerSeekCallback consumerSeekCallback) {
		if (receiverHandler.get() == null) {
			return;
		}

		for (Map.Entry<String, TopicConsumerInfo> entry : receiverHandler.get().getPartitionToTopicConsumerInfoMap().entrySet()) {
			String topicPartition = entry.getKey();
			String currentHivePartition = TimeUtils.getCurrentHivePartition();

			String collectCode = entry.getValue().getCollectCode();
			LogCollectConfig collectConfig = dcLogCollectService.getDcLogByCode(collectCode);
			if (collectConfig == null) {
				LOGGER.error("采集不存在: {}", collectCode);
			}
			changeHivePartition(collectConfig, topicPartition, currentHivePartition);

			receiverHandler.get().flushTopic("idle", topicPartition);
		}
	}

	private void changeHivePartition(LogCollectConfig collectConfig, String topicPartition, String currentHivePartition) {
		String lastHivePartition = topicPartitionLastHivePartition.get(topicPartition);
		if (lastHivePartition == null) {
			lastHivePartition = currentHivePartition;
			topicPartitionLastHivePartition.put(topicPartition, currentHivePartition);
		}

		if (lastHivePartition != null && !lastHivePartition.equals(currentHivePartition)) {
			topicPartitionLastHivePartition.put(topicPartition, currentHivePartition);
			hivePartitionService.addPartitionInfo(collectConfig, lastHivePartition);
		}
	}

}
