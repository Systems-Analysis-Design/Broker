package broker.dto;

import java.util.List;

public record PushRequestDto(String name, MessageDto message, List<String> replicas) {
}
