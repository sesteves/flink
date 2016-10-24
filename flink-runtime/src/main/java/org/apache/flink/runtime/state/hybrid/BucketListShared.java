package org.apache.flink.runtime.state.hybrid;

/**
 * Created by sesteves on 24-10-2016.
 */
public class BucketListShared {
	private boolean isFinalProcessing = false;

	public boolean isFinalProcessing() {
		return isFinalProcessing;
	}

	public void setFinalProcessing(boolean isFinalProcessing) {
		this.isFinalProcessing = isFinalProcessing;
	}
}
