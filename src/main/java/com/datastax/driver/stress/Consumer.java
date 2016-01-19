package com.datastax.driver.stress;

import agoda.search.models.protobuf.HotelProtos;
import agoda.search.models.protobuf.Suppliers;

import java.util.HashMap;
import java.util.List;

/**
 * Created by malam on 1/5/16.
 */
public interface Consumer {

    public void start();

    public void join();
}