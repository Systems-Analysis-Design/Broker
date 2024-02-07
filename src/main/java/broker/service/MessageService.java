package broker.service;

import broker.config.BrokerConfig;
import broker.dto.*;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;

@Service
@RequiredArgsConstructor
public class MessageService {
    @Value("${application.master-host}")
    private String masterHost;
    private final BrokerConfig brokerConfig;
    private final RestTemplate restTemplate;

    public void push(final PushRequestDto pushRequestDto) {
        // TODO read or write file
        if (pushRequestDto.partition().equals(brokerConfig.getPrimaryPartition())) {
            brokerConfig.getReplicaBrokerDtoList().forEach(x -> pushReplica(x, brokerConfig.getPrimaryPartition(), pushRequestDto.messageDto()));
        }
    }

    public MessageDto pull(final PullRequestDto pullRequestDto) {
        // TODO read or write file
        if (pullRequestDto.partition().equals(brokerConfig.getPrimaryPartition())) {
            brokerConfig.getReplicaBrokerDtoList().forEach(x -> pullReplica(x, brokerConfig.getPrimaryPartition()));
        }
        return null;
    }

    @Scheduled(fixedRateString = "${application.master-health-interval}")
    public void callMasterHealth() {
        String uri = masterHost + "/api/health";
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        MasterHealthRequestDto masterHealthRequestDto = new MasterHealthRequestDto();
        HttpEntity<MasterHealthRequestDto> entity = new HttpEntity<>(masterHealthRequestDto, headers);
        try {
            ResponseEntity<MasterHealthResponseDto> result = restTemplate.exchange(uri, HttpMethod.POST, entity, MasterHealthResponseDto.class);
            MasterHealthResponseDto body = result.getBody();
        } catch (Exception e) {

        }
    }

    private void pushReplica(final ReplicaBrokerDto replicaBrokerDto, final String partition, final MessageDto messageDto) {
        String uri = replicaBrokerDto.host() + "/api/push";
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        PushRequestDto pushRequestDto = new PushRequestDto(partition, messageDto);
        HttpEntity<PushRequestDto> entity = new HttpEntity<>(pushRequestDto, headers);
        ResponseEntity<Void> result = restTemplate.exchange(uri, HttpMethod.POST, entity, Void.class);
        if (!result.getStatusCode().equals(HttpStatusCode.valueOf(200))) {
            throw new RuntimeException("error in sync replica");
        }
    }

    private void pullReplica(final ReplicaBrokerDto replicaBrokerDto, final String partition) {
        String uri = replicaBrokerDto.host() + "/api/pull";
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        PullRequestDto pullRequestDto = new PullRequestDto(partition);
        HttpEntity<PullRequestDto> entity = new HttpEntity<>(pullRequestDto, headers);
        ResponseEntity<MessageDto> result = restTemplate.exchange(uri, HttpMethod.POST, entity, MessageDto.class);
        if (!result.getStatusCode().equals(HttpStatusCode.valueOf(200))) {
            throw new RuntimeException("error in sync replica");
        }
    }
}
