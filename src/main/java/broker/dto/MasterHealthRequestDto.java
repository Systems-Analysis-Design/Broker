package broker.dto;

public record MasterHealthRequestDto(String brokerName, long totalNumberOfMessages, int totalNumberOfQueues) {
}
