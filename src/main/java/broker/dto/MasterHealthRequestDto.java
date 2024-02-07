package broker.dto;

public record MasterHealthRequestDto(long totalNumberOfMessages, int totalNumberOfQueues) {
}
