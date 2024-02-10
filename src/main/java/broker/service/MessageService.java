package broker.service;

import broker.config.BrokerConfig;
import broker.dto.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
@RequiredArgsConstructor
@Slf4j
public class MessageService {
    @Value("${application.master-host}")
    private String masterHost;
    private final BrokerConfig brokerConfig;
    private final RestTemplate restTemplate;
    private final MessageReadWriteService messageReadWriteService;

    public PushResponseDto push(final PushRequestDto pushRequestDto) {
        messageReadWriteService.push(pushRequestDto.partition(), pushRequestDto.messageDto());
        log.info("message pushed: " + pushRequestDto.messageDto().key());
        if (pushRequestDto.partition().equals(brokerConfig.getPrimaryPartition())) {
            boolean allReplicasGotMessage = brokerConfig.getReplicaBrokerDtoList()
                                                        .stream()
                                                        .allMatch(x -> pushReplica(x,
                                                                                   brokerConfig.getPrimaryPartition(),
                                                                                   pushRequestDto.messageDto()));
            return new PushResponseDto(allReplicasGotMessage);
        }
        return new PushResponseDto(false);
    }

    public PullResponseDto pull(final PullRequestDto pullRequestDto) {
        MessageDto messageDto = messageReadWriteService.pull(pullRequestDto.partition());
        log.info("message pulled: " + messageDto.key());
        if (pullRequestDto.partition().equals(brokerConfig.getPrimaryPartition())) {
            boolean allReplicasGotMessage = brokerConfig.getReplicaBrokerDtoList()
                                                        .stream()
                                                        .allMatch(x -> pullReplica(x, brokerConfig.getPrimaryPartition()));
            return new PullResponseDto(messageDto, allReplicasGotMessage);
        }
        return new PullResponseDto(messageDto, false);
    }

    public void addReplica(AddReplicaRequestDto dto) {
        ReplicaBrokerDto replicaBrokerDto = new ReplicaBrokerDto(dto.address());
        brokerConfig.getReplicaBrokerDtoList().add(replicaBrokerDto);
    }

    @Scheduled(fixedRateString = "${application.master-health-interval}")
    public void callMasterHealth() {
        String uri = masterHost + "/api/health";
        MasterHealthRequestDto masterHealthRequestDto = new MasterHealthRequestDto(brokerConfig.getName(),
                                                                                   messageReadWriteService.getTotalNumberOfMessages(),
                                                                                   messageReadWriteService.getTotalNumberOfQueues());
        RequestEntity<MasterHealthRequestDto> requestEntity = RequestEntity.post(uri)
                                                                           .contentType(MediaType.APPLICATION_JSON)
                                                                           .body(masterHealthRequestDto);
        restTemplate.exchange(requestEntity, Void.class);
    }

    private boolean pushReplica(final ReplicaBrokerDto replicaBrokerDto, final String partition, final MessageDto messageDto) {
        try {
            String uri = replicaBrokerDto.host() + "/api/push";
            PushRequestDto pushRequestDto = new PushRequestDto(partition, messageDto);
            RequestEntity<PushRequestDto> requestEntity = RequestEntity.post(uri)
                                                                       .contentType(MediaType.APPLICATION_JSON)
                                                                       .body(pushRequestDto);
            ResponseEntity<PushResponseDto> result = restTemplate.exchange(requestEntity, PushResponseDto.class);
            return result.getStatusCode().equals(HttpStatusCode.valueOf(200));
        } catch (Exception e) {
            return false;
        }

    }

    private boolean pullReplica(final ReplicaBrokerDto replicaBrokerDto, final String partition) {
        try {
            String uri = replicaBrokerDto.host() + "/api/pull";
            PullRequestDto pullRequestDto = new PullRequestDto(partition);
            RequestEntity<PullRequestDto> requestEntity = RequestEntity.post(uri)
                                                                       .contentType(MediaType.APPLICATION_JSON)
                                                                       .body(pullRequestDto);
            ResponseEntity<PullResponseDto> result = restTemplate.exchange(requestEntity, PullResponseDto.class);
            return result.getStatusCode().equals(HttpStatusCode.valueOf(200));
        } catch (Exception e) {
            return false;
        }
    }
}
