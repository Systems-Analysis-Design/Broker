package broker.service;

import broker.config.BrokerConfig;
import broker.dto.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class MessageService {
    @Value("${application.master-host}")
    private String masterHost;
    private final BrokerConfig brokerConfig;
    private final RestTemplate restTemplate;
    private final MessageReadWriteService messageReadWriteService;

    public void push(final PushRequestDto pushRequestDto) {
        messageReadWriteService.push(pushRequestDto.name(), pushRequestDto.message());
        log.info("message pushed: " + pushRequestDto.message().key());
        pushRequestDto.replicas().forEach(x -> pushReplica(x, pushRequestDto.message()));
    }

    public MessageDto pull(final PullRequestDto pullRequestDto) {
        MessageDto messageDto = messageReadWriteService.pull(pullRequestDto.name());
        log.info("message pulled: " + messageDto.key());
        pullRequestDto.replicas().forEach(this::pullReplica);
        return messageDto;
    }

//    @Scheduled(fixedRateString = "${application.master-health-interval}")
//    public void callMasterHealth() {
//        String uri = masterHost + "/api/health";
//        MasterHealthRequestDto masterHealthRequestDto = new MasterHealthRequestDto(brokerConfig.getName(),
//                                                                                   messageReadWriteService.getTotalNumberOfMessages(),
//                                                                                   messageReadWriteService.getTotalNumberOfQueues());
//        RequestEntity<MasterHealthRequestDto> requestEntity = RequestEntity.post(uri)
//                                                                           .contentType(MediaType.APPLICATION_JSON)
//                                                                           .body(masterHealthRequestDto);
//        restTemplate.exchange(requestEntity, Void.class);
//    }

    private void pushReplica(final String host, final MessageDto message) {
        try {
            String uri = host + "/api/push";
            PushRequestDto pushRequestDto = new PushRequestDto(brokerConfig.getName(), message, List.of());
            RequestEntity<PushRequestDto> requestEntity = RequestEntity.post(uri)
                                                                       .contentType(MediaType.APPLICATION_JSON)
                                                                       .body(pushRequestDto);
            ResponseEntity<Void> result = restTemplate.exchange(requestEntity, Void.class);
            if (!result.getStatusCode().equals(HttpStatusCode.valueOf(200))) {
                log.error("error in sync replicas for push");
            }
        } catch (Exception e) {
            log.error("error in sync replicas for push", e);
        }

    }

    private void pullReplica(final String host) {
        try {
            String uri = host + "/api/pull";
            PullRequestDto pullRequestDto = new PullRequestDto(brokerConfig.getName(), List.of());
            RequestEntity<PullRequestDto> requestEntity = RequestEntity.post(uri)
                                                                       .contentType(MediaType.APPLICATION_JSON)
                                                                       .body(pullRequestDto);
            ResponseEntity<MessageDto> result = restTemplate.exchange(requestEntity, MessageDto.class);
            if (!result.getStatusCode().equals(HttpStatusCode.valueOf(200))) {
                log.error("error in sync replicas for pull");
            }
        } catch (Exception e) {
            log.error("error in sync replicas for pull", e);
        }
    }
}
