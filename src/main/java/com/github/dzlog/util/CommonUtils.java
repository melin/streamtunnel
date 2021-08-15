package com.github.dzlog.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by binsong.li
 */
public class CommonUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(CommonUtils.class);

	public static boolean supportZstd = false;

	public static long ipToLong(String ipAddress) {

		String[] ipAddressInArray = ipAddress.split("\\.");

		long result = 0;
		for (int i = 0; i < ipAddressInArray.length; i++) {

			int power = 3 - i;
			int ip = Integer.parseInt(ipAddressInArray[i]);
			result += ip * Math.pow(256, power);
		}

		return result;
	}

	public static String convertDateFormat(String date) {
		StringBuilder sb = new StringBuilder();
		sb.append(date.substring(0, 4));
		sb.append("-");
		sb.append(date.substring(4, 6));
		sb.append("-");
		sb.append(date.substring(6, 8));
		sb.append(" ");
		sb.append(date.substring(8, 10));
		sb.append(":");
		sb.append(date.substring(10, 12));
		sb.append(":");
		sb.append(date.substring(12));

		return sb.toString();
	}

	public static TopicPartition createTopicPartition(String topicPartition) {
		int index = StringUtils.lastIndexOf(topicPartition, "-");
		String topic = StringUtils.substring(topicPartition, 0, index);
		String partition = StringUtils.substring(topicPartition, index + 1);

		return new TopicPartition(topic, Integer.parseInt(partition));
	}

}
