package edu.itu.bigdata;

public class SamplerThreadGroup extends ThreadGroup {
	private Throwable throwable;

	public SamplerThreadGroup(String s) {
		super(s);
	}

	@Override
	public void uncaughtException(Thread thread, Throwable throwable) {
		this.throwable = throwable;
	}

	public Throwable getThrowable() {
		return this.throwable;
	}

}
