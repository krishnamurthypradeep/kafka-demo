package com.myapp.kafka.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

import java.util.Collection;

@Component
public class RebalanceListener implements ConsumerAwareRebalanceListener {

    @Override
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
      //  ConsumerAwareRebalanceListener.super.onPartitionsRevokedBeforeCommit(consumer, partitions);
        System.out.printf(">> Revoked Before Commit member=%s partitions=%s%n",
                consumer.groupMetadata().memberId(),partitions);

    }

    @Override
    public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        //ConsumerAwareRebalanceListener.super.onPartitionsRevokedAfterCommit(consumer, partitions);
        System.out.printf(">> AfterCommit member=%s partitions=%s%n",
                consumer.groupMetadata().memberId(),partitions);
    }

    @Override
    public void onPartitionsAssigned(Consumer<?,?> consumer, Collection<TopicPartition> partitions) {
        //ConsumerAwareRebalanceListener.super.onPartitionsAssigned(partitions);
        System.out.printf(">> Assigned members%s partitions=%s%n",
                consumer.groupMetadata().memberId(),partitions);
    }
}
