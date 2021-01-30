/**
 * Copyright 2020 Viridian Software Ltd.
 */
package com.elefana.http;

import org.checkerframework.checker.units.qual.A;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class InterruptTest {

	@Test
	public void testInterrupt() {
		final CountDownLatch countDownLatch = new CountDownLatch(2);
		final Thread [] threads = new Thread[2];
		final ReentrantLock lock = new ReentrantLock();
		final AtomicInteger interruptCount = new AtomicInteger(-1);
		final AtomicBoolean condition = new AtomicBoolean(true);

		threads[0] = new Thread(() -> {
			try {
				countDownLatch.countDown();
				countDownLatch.await();
			} catch (Exception e) {}

			try {
				lock.lock();
				interruptCount.incrementAndGet();
				synchronized (this) {
					this.wait(30000L);
				}
			} catch (InterruptedException e) {
			} finally {
				lock.unlock();
			}

			Assert.assertFalse(lock.isHeldByCurrentThread());
		});
		threads[1] = new Thread(() -> {
			try {
				countDownLatch.countDown();
				countDownLatch.await();
			} catch (Exception e) {}

			while(interruptCount.get() < 0) {
				try {
					Thread.sleep(100);
				} catch (Exception e) {}
			}

			threads[0].interrupt();
		});

		for(int i = 0; i < threads.length; i++) {
			threads[i].start();
		}
		for(int i = 0; i < threads.length; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {}
		}

		Assert.assertFalse(lock.isLocked());
	}
}
