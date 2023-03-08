package com.phuongheh.kafka.streams.microservices.utils;

public class Paths {
    private final String base;

    public Paths(String base) {
        this.base = base;
    }

    public Paths(String host, int port) {
        base = "http://" + host + ":" + port;
    }

    public String urlGet(int id) {
        return base + "/v1/orders/" + id;
    }

    public String urlGet(String id) {
        return base + "/v1/orders/" + id;
    }
    public String urlGetValidated(int id){
        return base + "/v1/orders/"+id + "/validated";
    }
    public String urlGetValidated(String id){
        return base + "/v1/orders/"+id+"/validated";
    }
    public String urlPost(){
        return base + "/v1/orders/";
    }
}
