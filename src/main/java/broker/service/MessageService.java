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

import java.util.Collections;

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

    @Scheduled(fixedRateString = "${application.master-health-interval}")
    public void callMasterHealth() {
        String uri = masterHost + "/api/health";
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.setContentType(MediaType.APPLICATION_JSON);
        MasterHealthRequestDto masterHealthRequestDto = new MasterHealthRequestDto(messageReadWriteService.getTotalNumberOfMessages(),
                                                                                   messageReadWriteService.getTotalNumberOfQueues());
        HttpEntity<MasterHealthRequestDto> entity = new HttpEntity<>(masterHealthRequestDto, headers);
        restTemplate.exchange(uri, HttpMethod.POST, entity, Void.class);
    }

    private boolean pushReplica(final ReplicaBrokerDto replicaBrokerDto, final String partition, final MessageDto messageDto) {
        try {
            String uri = replicaBrokerDto.host() + "/api/push";
            HttpHeaders headers = new HttpHeaders();
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
            PushRequestDto pushRequestDto = new PushRequestDto(partition, messageDto);
            HttpEntity<PushRequestDto> entity = new HttpEntity<>(pushRequestDto, headers);
            ResponseEntity<PushResponseDto> result = restTemplate.exchange(uri, HttpMethod.POST, entity, PushResponseDto.class);
            return result.getStatusCode().equals(HttpStatusCode.valueOf(200));
        } catch (Exception e) {
            return false;
        }

    }

    private boolean pullReplica(final ReplicaBrokerDto replicaBrokerDto, final String partition) {
        try {
            String uri = replicaBrokerDto.host() + "/api/pull";
            HttpHeaders headers = new HttpHeaders();
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
            PullRequestDto pullRequestDto = new PullRequestDto(partition);
            HttpEntity<PullRequestDto> entity = new HttpEntity<>(pullRequestDto, headers);
            ResponseEntity<PullResponseDto> result = restTemplate.exchange(uri, HttpMethod.POST, entity, PullResponseDto.class);
            return result.getStatusCode().equals(HttpStatusCode.valueOf(200));
        } catch (Exception e) {
            return false;
        }
    }
}
