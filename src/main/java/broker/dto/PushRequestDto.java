package broker.dto;

public record PushRequestDto(String partition, MessageDto messageDto) {
}
