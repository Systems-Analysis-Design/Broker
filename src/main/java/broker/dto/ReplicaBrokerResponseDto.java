package broker.dto;

import java.util.List;

public record ReplicaBrokerResponseDto(String primaryPartition,
                                       List<ReplicaBrokerDto> replicaBrokerDtoList) {
}
