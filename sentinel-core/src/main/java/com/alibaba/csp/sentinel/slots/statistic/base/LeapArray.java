/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.slots.statistic.base;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.csp.sentinel.util.AssertUtil;
import com.alibaba.csp.sentinel.util.TimeUtil;

/**
 * <p>
 * Basic data structure for statistic metrics in Sentinel.
 * </p>
 * <p>
 * Leap array use sliding window algorithm to count data. Each bucket cover {@code windowLengthInMs} time span,
 * and the total time span is {@link #intervalInMs}, so the total bucket amount is:
 * {@code sampleCount = intervalInMs / windowLengthInMs}.
 * </p>
 *
 * @param <T> type of statistic data
 * @author jialiang.linjl
 * @author Eric Zhao
 * @author Carpenter Lee
 */
public abstract class LeapArray<T>
{

	/** 窗口时间间隔，单位为毫秒 */
	protected int windowLengthInMs;
	/** 样本数，即一个统计时间间隔中包含的滑动窗口的桶数，在相同统计时间间隔内，样本数越多，抽样的统计数据就越精确，相应的需要的内存也越多。*/
	protected int sampleCount;
	/** 一次统计的时间间隔，分秒和毫秒两个单位 */
	protected int intervalInMs;
	private double intervalInSecond;
	/** 一个统计中滑动窗口的数组，从这里也可以看出，一个滑动窗口就是使用的 WindowWrap< MetricBucket > 来表示。*/
	protected final AtomicReferenceArray<WindowWrap<T>> array;

	/**
	 * The conditional (predicate) update lock is used only when current bucket is deprecated.
	 */
	private final ReentrantLock updateLock = new ReentrantLock();

	/**
	 * The total bucket count is: {@code sampleCount = intervalInMs / windowLengthInMs}.
	 *
	 * @param sampleCount  bucket count of the sliding window
	 * @param intervalInMs the total time interval of this {@link LeapArray} in milliseconds
	 */
	public LeapArray(int sampleCount, int intervalInMs)
	{
		AssertUtil.isTrue(sampleCount > 0, "bucket count is invalid: " + sampleCount);
		AssertUtil.isTrue(intervalInMs > 0, "total time interval of the sliding window should be positive");
		AssertUtil.isTrue(intervalInMs % sampleCount == 0, "time span needs to be evenly divided");

		this.windowLengthInMs = intervalInMs / sampleCount;
		this.intervalInMs = intervalInMs;
		this.intervalInSecond = intervalInMs / 1000.0;
		this.sampleCount = sampleCount;

		this.array = new AtomicReferenceArray<>(sampleCount);
	}

	/**
	 * Get the bucket at current timestamp.
	 * 根据当前时间获取窗口桶
	 *
	 * @return the bucket at current timestamp
	 */
	public WindowWrap<T> currentWindow()
	{
		return currentWindow(TimeUtil.currentTimeMillis());
	}

	/**
	 * Create a new statistic value for bucket.
	 *
	 * @param timeMillis current time in milliseconds
	 * @return the new empty bucket
	 */
	public abstract T newEmptyBucket(long timeMillis);

	/**
	 * Reset given bucket to provided start time and reset the value.
	 * 将给定的桶重置为所提供的开始时间，并重置该值。
	 *
	 * @param startTime  the start time of the bucket in milliseconds
	 * @param windowWrap current bucket
	 * @return new clean bucket at given start time
	 */
	protected abstract WindowWrap<T> resetWindowTo(WindowWrap<T> windowWrap, long startTime);

	/**
	 * 计算当前时间戳所在的时间窗口下标
	 * （timeMillis/windowLengthInMs）%array.length
	 * @param timeMillis
	 * @return
	 */
	private int calculateTimeIdx(/*@Valid*/ long timeMillis)
	{
		long timeId = timeMillis / windowLengthInMs;
		// Calculate current index so we can map the timestamp to the leap array.
		return (int) (timeId % array.length());
	}

	/**
	 * 计算当前时间戳所在的时间窗口的开始时间，即要计算出 WindowWrap 中 windowStart 的值，其实就是要算出小于当前时间戳并且是 windowLengthInMs 的整数倍最大的数字
	 * Sentinel 给出是算法为 ( timeMillis - timeMillis % windowLengthInMs )
	 * @param timeMillis
	 * @return
	 */
	protected long calculateWindowStart(/*@Valid*/ long timeMillis)
	{
		return timeMillis - timeMillis % windowLengthInMs;
	}

	/**
	 * Get bucket item at provided timestamp.
	 *
	 * @param timeMillis a valid timestamp in milliseconds
	 * @return current bucket item at provided timestamp if the time is valid; null if time is invalid
	 */
	public WindowWrap<T> currentWindow(long timeMillis)
	{
		if (timeMillis < 0)
		{
			return null;
		}

		int idx = calculateTimeIdx(timeMillis);
		// Calculate current bucket start time.
		long windowStart = calculateWindowStart(timeMillis);

		/*
		 * Get bucket item at given time from the array.
		 * 根据当前时间获取窗口桶
		 * (1) Bucket is absent, then just create a new bucket and CAS update to circular array.
		 * 如果窗口桶是空的，则创建并通过cas设置到滑动窗口的数组中，使用cas的原因是可能存在并发情况
		 * (2) Bucket is up-to-date, then just return the bucket.
		 * 如果窗口桶是最新的，则直接返回
		 * (3) Bucket is deprecated, then reset current bucket and clean all deprecated buckets.
		 * 如果窗口桶被弃用，然后重置当前桶并清除所有弃用的桶。
		 */
		while (true)
		{
			WindowWrap<T> old = array.get(idx);
			if (old == null)
			{
				/*
				 *     B0       B1      B2    NULL      B4
				 * ||_______|_______|_______|_______|_______||___
				 * 200     400     600     800     1000    1200  timestamp
				 *                             ^
				 *                          time=888
				 *            bucket is empty, so create new and update
				 *
				 * If the old bucket is absent, then we create a new bucket at {@code windowStart},
				 * then try to update circular array via a CAS operation. Only one thread can
				 * succeed to update, while other threads yield its time slice.
				 */
				WindowWrap<T> window = new WindowWrap<T>(windowLengthInMs, windowStart, newEmptyBucket(timeMillis));
				if (array.compareAndSet(idx, null, window))
				{
					// Successfully updated, return the created bucket.
					return window;
				}
				else
				{
					// Contention failed, the thread will yield its time slice to wait for bucket available.
					// cas设置失败，线程将它的时间片用来等待bucket可用
					Thread.yield();
				}
			}
			else if (windowStart == old.windowStart())
			{
				/*
				 *     B0       B1      B2     B3      B4
				 * ||_______|_______|_______|_______|_______||___
				 * 200     400     600     800     1000    1200  timestamp
				 *                             ^
				 *                          time=888
				 *            startTime of Bucket 3: 800, so it's up-to-date
				 *
				 * If current {@code windowStart} is equal to the start timestamp of old bucket,
				 * that means the time is within the bucket, so directly return the bucket.
				 */
				return old;
			}
			else if (windowStart > old.windowStart())
			{
				/*
				 *   (old)
				 *             B0       B1      B2    NULL      B4
				 * |_______||_______|_______|_______|_______|_______||___
				 * ...    1200     1400    1600    1800    2000    2200  timestamp
				 *                              ^
				 *                           time=1676
				 *          startTime of Bucket 2: 400, deprecated, should be reset
				 *
				 * If the start timestamp of old bucket is behind provided time, that means
				 * the bucket is deprecated. We have to reset the bucket to current {@code windowStart}.
				 * Note that the reset and clean-up operations are hard to be atomic,
				 * so we need a update lock to guarantee the correctness of bucket update.
				 *
				 * The update lock is conditional (tiny scope) and will take effect only when
				 * bucket is deprecated, so in most cases it won't lead to performance loss.
				 */
				if (updateLock.tryLock())//ReentrantLock
				{
					try
					{
						// Successfully get the update lock, now we reset the bucket.
						return resetWindowTo(old, windowStart);
					}
					finally
					{
						updateLock.unlock();
					}
				}
				else
				{
					// Contention failed, the thread will yield its time slice to wait for bucket available.
					Thread.yield();
				}
			}
			else if (windowStart < old.windowStart())
			{
				// Should not go through here, as the provided time is already behind.
				// 正常情况下不应该走到此逻辑，异常场景：时钟回拨的情况
				return new WindowWrap<T>(windowLengthInMs, windowStart, newEmptyBucket(timeMillis));
			}
		}
	}

	/**
	 * Get the previous bucket item before provided timestamp.
	 * 根据给定时间，获取前一个有效滑动窗口
	 * @param timeMillis a valid timestamp in milliseconds
	 * @return the previous bucket item before provided timestamp
	 */
	public WindowWrap<T> getPreviousWindow(long timeMillis)
	{
		if (timeMillis < 0)
		{
			return null;
		}
		// 用当前时间减去一个窗口时间间隔，然后去定位所在 LeapArray 中 数组的下标。
		int idx = calculateTimeIdx(timeMillis - windowLengthInMs);
		timeMillis = timeMillis - windowLengthInMs;
		WindowWrap<T> wrap = array.get(idx);
		// 如果为空或已过期，则返回 null
		if (wrap == null || isWindowDeprecated(wrap))
		{
			return null;
		}
		//如果定位的窗口的开始时间再加上 windowLengthInMs 小于 timeMills ，说明失效，则返回 null，通常是不会走到该分支
		if (wrap.windowStart() + windowLengthInMs < (timeMillis))
		{
			return null;
		}

		return wrap;
	}

	/**
	 * Get the previous bucket item for current timestamp.
	 * 获取当前时间前一个有效的滑动窗口
	 * @return the previous bucket item for current timestamp
	 */
	public WindowWrap<T> getPreviousWindow()
	{
		return getPreviousWindow(TimeUtil.currentTimeMillis());
	}

	/**
	 * Get statistic value from bucket for provided timestamp.
	 * 获取给定时间对应的窗口桶的统计数据
	 * @param timeMillis a valid timestamp in milliseconds
	 * @return the statistic value if bucket for provided timestamp is up-to-date; otherwise null
	 */
	public T getWindowValue(long timeMillis)
	{
		if (timeMillis < 0)
		{
			return null;
		}
		int idx = calculateTimeIdx(timeMillis);

		WindowWrap<T> bucket = array.get(idx);

		if (bucket == null || !bucket.isTimeInWindow(timeMillis))
		{
			return null;
		}

		return bucket.value();
	}

	/**
	 * Check if a bucket is deprecated, which means that the bucket
	 * has been behind for at least an entire window time span.
	 * 用来检查当前窗口桶是否过期，即：当前窗口桶的开始时间晚于给定时间超过一个统计的时间间隔
	 * @param windowWrap a non-null bucket
	 * @return true if the bucket is deprecated; otherwise false
	 */
	public boolean isWindowDeprecated(/*@NonNull*/ WindowWrap<T> windowWrap)
	{
		return isWindowDeprecated(TimeUtil.currentTimeMillis(), windowWrap);
	}

	public boolean isWindowDeprecated(long time, WindowWrap<T> windowWrap)
	{
		return time - windowWrap.windowStart() > intervalInMs;
	}

	/**
	 * Get valid bucket list for entire sliding window.
	 * The list will only contain "valid" buckets.
	 *
	 * @return valid bucket list for entire sliding window.
	 */
	public List<WindowWrap<T>> list()
	{
		return list(TimeUtil.currentTimeMillis());
	}

	public List<WindowWrap<T>> list(long validTime)
	{
		int size = array.length();
		List<WindowWrap<T>> result = new ArrayList<WindowWrap<T>>(size);

		for (int i = 0; i < size; i++)
		{
			WindowWrap<T> windowWrap = array.get(i);
			if (windowWrap == null || isWindowDeprecated(validTime, windowWrap))
			{
				continue;
			}
			result.add(windowWrap);
		}

		return result;
	}

	/**
	 * Get all buckets for entire sliding window including deprecated buckets.
	 *
	 * @return all buckets for entire sliding window
	 */
	public List<WindowWrap<T>> listAll()
	{
		int size = array.length();
		List<WindowWrap<T>> result = new ArrayList<WindowWrap<T>>(size);

		for (int i = 0; i < size; i++)
		{
			WindowWrap<T> windowWrap = array.get(i);
			if (windowWrap == null)
			{
				continue;
			}
			result.add(windowWrap);
		}

		return result;
	}

	/**
	 * Get aggregated value list for entire sliding window.
	 * The list will only contain value from "valid" buckets.
	 *
	 * @return aggregated value list for entire sliding window
	 */
	public List<T> values()
	{
		return values(TimeUtil.currentTimeMillis());
	}

	public List<T> values(long timeMillis)
	{
		if (timeMillis < 0)
		{
			return new ArrayList<T>();
		}
		int size = array.length();
		List<T> result = new ArrayList<T>(size);

		for (int i = 0; i < size; i++)
		{
			WindowWrap<T> windowWrap = array.get(i);
			if (windowWrap == null || isWindowDeprecated(timeMillis, windowWrap))
			{
				continue;
			}
			result.add(windowWrap.value());
		}
		return result;
	}

	/**
	 * Get the valid "head" bucket of the sliding window for provided timestamp.
	 * Package-private for test.
	 *
	 * @param timeMillis a valid timestamp in milliseconds
	 * @return the "head" bucket if it exists and is valid; otherwise null
	 */
	WindowWrap<T> getValidHead(long timeMillis)
	{
		// Calculate index for expected head time.
		int idx = calculateTimeIdx(timeMillis + windowLengthInMs);

		WindowWrap<T> wrap = array.get(idx);
		if (wrap == null || isWindowDeprecated(wrap))
		{
			return null;
		}

		return wrap;
	}

	/**
	 * Get the valid "head" bucket of the sliding window at current timestamp.
	 *
	 * @return the "head" bucket if it exists and is valid; otherwise null
	 */
	public WindowWrap<T> getValidHead()
	{
		return getValidHead(TimeUtil.currentTimeMillis());
	}

	/**
	 * Get sample count (total amount of buckets).
	 *
	 * @return sample count
	 */
	public int getSampleCount()
	{
		return sampleCount;
	}

	/**
	 * Get total interval length of the sliding window in milliseconds.
	 *
	 * @return interval in second
	 */
	public int getIntervalInMs()
	{
		return intervalInMs;
	}

	/**
	 * Get total interval length of the sliding window.
	 *
	 * @return interval in second
	 */
	public double getIntervalInSecond()
	{
		return intervalInSecond;
	}

	public void debug(long time)
	{
		StringBuilder sb = new StringBuilder();
		List<WindowWrap<T>> lists = list(time);
		sb.append("Thread_").append(Thread.currentThread().getId()).append("_");
		for (WindowWrap<T> window : lists)
		{
			sb.append(window.windowStart()).append(":").append(window.value().toString());
		}
		System.out.println(sb.toString());
	}

	public long currentWaiting()
	{
		// TODO: default method. Should remove this later.
		return 0;
	}

	public void addWaiting(long time, int acquireCount)
	{
		// Do nothing by default.
		throw new UnsupportedOperationException();
	}
}
