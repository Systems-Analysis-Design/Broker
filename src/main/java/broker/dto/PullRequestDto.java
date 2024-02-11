package broker.dto;

import java.util.List;

public record PullRequestDto(String name, List<String> replicas) {
}
