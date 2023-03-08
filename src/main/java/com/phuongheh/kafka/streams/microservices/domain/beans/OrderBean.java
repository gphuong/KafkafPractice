package com.phuongheh.kafka.streams.microservices.domain.beans;

import com.phuongheh.kafka.streams.avro.microservices.Order;
import com.phuongheh.kafka.streams.avro.microservices.OrderState;
import com.phuongheh.kafka.streams.avro.microservices.Product;

public class OrderBean {
    private String id;
    private long customerId;
    private OrderState state;
    private Product product;
    private int quantity;
    private double price;

    public OrderBean() {
    }

    public OrderBean(String id, long customerId, OrderState state, Product product, int quantity, double price) {
        this.id = id;
        this.customerId = customerId;
        this.state = state;
        this.product = product;
        this.quantity = quantity;
        this.price = price;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getCustomerId() {
        return customerId;
    }

    public OrderState getState() {
        return state;
    }

    public Product getProduct() {
        return product;
    }

    public int getQuantity() {
        return quantity;
    }

    public double getPrice() {
        return price;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof OrderBean)) return false;

        OrderBean orderBean = (OrderBean) o;

        if (getCustomerId() != orderBean.getCustomerId()) return false;
        if (getQuantity() != orderBean.getQuantity()) return false;
        if (Double.compare(orderBean.getPrice(), getPrice()) != 0) return false;
        if (getId() != null ? !getId().equals(orderBean.getId()) : orderBean.getId() != null) return false;
        if (getState() != orderBean.getState()) return false;
        return getProduct() == orderBean.getProduct();
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = getId() != null ? getId().hashCode() : 0;
        result = 31 * result + (int) (getCustomerId() ^ (getCustomerId() >>> 32));
        result = 31 * result + (getState() != null ? getState().hashCode() : 0);
        result = 31 * result + (getProduct() != null ? getProduct().hashCode() : 0);
        result = 31 * result + getQuantity();
        temp = Double.doubleToLongBits(getPrice());
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "id='" + id + '\'' +
                ", customerId=" + customerId +
                ", state=" + state +
                ", product=" + product +
                ", quantity=" + quantity +
                ", price=" + price +
                '}';
    }

    public static OrderBean toBean(Order order) {
        return new OrderBean(order.getId(),
                order.getCustomerId(),
                order.getState(),
                order.getProduct(),
                order.getQuantity(),
                order.getPrice());
    }

    public static Order fromBean(OrderBean order) {
        return new Order(order.getId(),
                order.getCustomerId(),
                order.getState(),
                order.getProduct(),
                order.getQuantity(),
                order.getPrice());
    }
}
