package com.phuongheh.kafka.streams.interactivequeries;

import java.util.Objects;

public class KeyValueBean {
    private String key;
    private Long value;

    public KeyValueBean() {
    }

    public KeyValueBean(String key, Long value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "KeyValueBean{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KeyValueBean)) return false;

        KeyValueBean that = (KeyValueBean) o;

        if (getKey() != null ? !getKey().equals(that.getKey()) : that.getKey() != null) return false;
        return getValue() != null ? getValue().equals(that.getValue()) : that.getValue() == null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }
}
