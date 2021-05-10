package ztwu.flink.domain;

public class UserBrowseLog {
	private String eventTime;
	private long eventTimeTimestamp;

	public long getEventTimeTimestamp() {
		return eventTimeTimestamp;
	}

	public void setEventTimeTimestamp(long eventTimeTimestamp) {
		this.eventTimeTimestamp = eventTimeTimestamp;
	}

	public String getEventTime() {
		return eventTime;
	}

	public void setEventTime(String eventTime) {
		eventTime = eventTime;
	}

}
