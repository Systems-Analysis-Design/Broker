package broker.dto;

public record MessageFileRecord(long messageNumber, long startingByte, long endingByte) {
}
