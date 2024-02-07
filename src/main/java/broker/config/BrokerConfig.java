package broker.config;

import broker.dto.ReplicaBrokerDto;
import broker.dto.ReplicaPartitionDto;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
@Setter
@Getter
public class BrokerConfig {
    private String primaryPartition;
    private List<ReplicaBrokerDto> replicaBrokerDtoList;
    private List<ReplicaPartitionDto> replicaPartitionDtoList;
}
