package com.myapp.kafka.domain;

import java.math.BigDecimal;

public record OrderLineItem(String item, Integer count, BigDecimal amount) {
}
