package com.github.dzlog.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;

/**
 * Created by libinsong
 */
public class ThreadUtils {

	public static ThreadPoolExecutor newDaemonFixedThreadPool(int nThreads, String prefix) {
		ThreadFactory threadFactory = namedThreadFactory(prefix);
		return (ThreadPoolExecutor) Executors.newFixedThreadPool(nThreads, threadFactory);
	}

	public static ExecutorService newDaemonSingleThreadExecutor(String prefix) {
		ThreadFactory threadFactory = namedThreadFactory(prefix);
		return Executors.newSingleThreadExecutor(threadFactory);
	}

    /**
     * Wrapper over ScheduledThreadPoolExecutor.
     */
	public static ScheduledExecutorService newDaemonSingleThreadScheduledExecutor(String prefix) {
		ThreadFactory threadFactory = namedThreadFactory(prefix);
		ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, threadFactory);
        // By default, a cancelled task is not automatically removed from the work queue until its delay
        // elapses. We have to enable it manually.
		executor.setRemoveOnCancelPolicy(true);
		return executor;
	}

	public static ThreadFactory namedThreadFactory(String prefix) {
		return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(prefix + "-%d").build();
	}
}
