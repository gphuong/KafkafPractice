package com.phuongheh.kafka.streams.interactivequeries;

import java.util.Objects;
import java.util.Set;

public class HostStoreInfo {
    private String host;
    private int port;
    private Set<String> storeNames;

    public HostStoreInfo() {
    }

    public HostStoreInfo(String host, int port, Set<String> storeNames) {
        this.host = host;
        this.port = port;
        this.storeNames = storeNames;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Set<String> getStoreNames() {
        return storeNames;
    }

    public void setStoreNames(Set<String> storeNames) {
        this.storeNames = storeNames;
    }

    @Override
    public String toString() {
        return "HostStoreInfo{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", storeNames=" + storeNames +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final HostStoreInfo that = (HostStoreInfo) o;
        return port == that.port &&
                Objects.equals(host, that.host) &&
                Objects.equals(storeNames, that.storeNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, storeNames);
    }
}
