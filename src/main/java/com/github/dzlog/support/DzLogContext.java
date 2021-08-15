package com.github.dzlog.support;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Calendar;
import java.util.Date;

/**
 * Created by binsong.li
 */
@Service
public class DzLogContext implements InitializingBean {

	private Calendar calendar = Calendar.getInstance();

	protected Date dateInCurrentPeriod = new Date();

	protected long nextCheck;

	@Autowired
	private ConfigurationLoader configurationLoader;

	@Override
	public void afterPropertiesSet() throws Exception {
		long time = System.currentTimeMillis();
		computeNextCheck(time);
	}

	public Configuration getConfiguration() {
		return configurationLoader.getConfiguration();
	}

	public void computeNextCheck(long now) {
		dateInCurrentPeriod.setTime(now);
		nextCheck = getNextTriggeringDate(dateInCurrentPeriod).getTime();
	}

	private Date getNextTriggeringDate(Date now) {
		calendar.setTime(now);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		calendar.add(Calendar.MINUTE, 1);
		return calendar.getTime();
	}
}
