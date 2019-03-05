package com.elefana.util;

public class CumulativeAverage {
	private int entries = 0;
	private int average = 0;

	public CumulativeAverage() {
		super();
	}

	public CumulativeAverage(int initialValue) {
		super();
		add(initialValue);
	}

	public synchronized int add(int x) {
		return average += (x - average) / ++entries;
	}

	public synchronized int avg() {
		return average;
	}
}
