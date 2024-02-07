package broker.dto;

public record PullResponseDto(MessageDto messageDto, Boolean allReplicasGotMessage) {
}
