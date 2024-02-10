package broker.dto;

import java.util.List;

public record ReplicaBrokerResponseDto(String name,
                                       String primaryPartition,
                                       List<ReplicaBrokerDto> replicaBrokerDtoList) {
}
