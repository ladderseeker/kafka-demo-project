package com.xiao.kafkaproducerdemoproject.services;

import java.util.concurrent.LinkedBlockingDeque;

public class ResourcesPool {

    public final static LinkedBlockingDeque<String> MESSAGE_QUEUE = new LinkedBlockingDeque<>(1000);

    public final static VehiclePassMessageGenerator MESSAGE_GENERATOR = new VehiclePassMessageGenerator();

}
