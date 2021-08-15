package com.github.dzlog.kafka;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author melin 2021/7/19 5:16 下午
 */
public class LogEvent {
    private String topic;

    private Integer partition;

    private Long offset;

    private String code;

    private String receivedTime;

    private ByteBuffer msgByteBuffer;

    private int msgBytes;

    public String getTopic() {
        return topic;
    }

    public String getTopicPartition() {
        return topic + "-" + partition;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getReceivedTime() {
        return receivedTime;
    }

    public void setReceivedTime(String receivedTime) {
        this.receivedTime = receivedTime;
    }

    public ByteBuffer getMsgByteBuffer() {
        return msgByteBuffer;
    }

    public void setMsgByteBuffer(ByteBuffer msgByteBuffer) {
        this.msgBytes = msgByteBuffer.remaining();
        this.msgByteBuffer = msgByteBuffer;
    }

    public int getMsgBytes() {
        return msgBytes;
    }

    @Override
    public String toString() {
        return "LogEvent{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", code='" + code + '\'' +
                ", receivedTime='" + receivedTime + '\'' +
                '}';
    }
}
