package broker.service;

import broker.dto.MessageDto;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

@Service
public class MessageReadWriteService {
    private final Map<String, Queue<MessageDto>> partitionNameMessageQueueMap = new HashMap<>();

    public void push(String partitionName, MessageDto message) {
        if (!partitionNameMessageQueueMap.containsKey(partitionName)) {
            partitionNameMessageQueueMap.put(partitionName, new LinkedList<>());
        }
        partitionNameMessageQueueMap.get(partitionName).add(message);
    }

    public MessageDto pull(String partitionName) {
        if (!partitionNameMessageQueueMap.containsKey(partitionName)) {
            partitionNameMessageQueueMap.put(partitionName, new LinkedList<>());
        }
        return partitionNameMessageQueueMap.get(partitionName).poll();
    }

    public long getTotalNumberOfMessages() {
        return partitionNameMessageQueueMap.values().stream().mapToInt(Queue::size).sum();
    }

    public int getTotalNumberOfQueues() {
        return partitionNameMessageQueueMap.size();
    }
}
