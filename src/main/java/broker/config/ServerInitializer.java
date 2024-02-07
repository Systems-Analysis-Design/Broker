package broker.config;

import broker.dto.BrokerJoinRequestDto;
import broker.dto.ReplicaBrokerResponseDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.*;
import org.springframework.retry.annotation.Retryable;
import org.springframework.retry.policy.TimeoutRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;

@Component
@Slf4j
public class ServerInitializer implements ApplicationRunner, ApplicationContextAware {
    private ApplicationContext context;

    private RestTemplate restTemplate;

    @Value("${application.master-host}")
    private String masterHost;

    @Value("${application.master-health-timeout}")
    private long masterHealthTimeout;

    @Value("${server.address}")
    private String serverAddress;

    @Value("${server.port}")
    private String serverPort;

    @Override
    public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        this.context = ctx;

    }

    @Override
    @Retryable
    public void run(ApplicationArguments applicationArguments) {
        this.restTemplate = (RestTemplate) context.getBean("restTemplate");
        String uri = masterHost + "/api/join";
        if (!callMaster(uri)) {
            log.error("error in joining broker");
            ((ConfigurableApplicationContext) context).close();
        }

    }

    private boolean callMaster(String uri) {
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        BrokerJoinRequestDto brokerJoinRequestDto = new BrokerJoinRequestDto(serverAddress + ":" + serverPort);
        HttpEntity<BrokerJoinRequestDto> entity = new HttpEntity<>(brokerJoinRequestDto, headers);
        final RetryTemplate template = new RetryTemplate();
        final TimeoutRetryPolicy policy = new TimeoutRetryPolicy(masterHealthTimeout);
        template.setRetryPolicy(policy);
        return template.execute(context -> {
            try {
                ResponseEntity<ReplicaBrokerResponseDto> result = restTemplate.exchange(uri, HttpMethod.POST, entity, ReplicaBrokerResponseDto.class);
                ReplicaBrokerResponseDto body = result.getBody();
                if (body == null) {
                    return false;
                }
                BrokerConfig brokerConfig = (BrokerConfig) this.context.getBean("brokerConfig");
                brokerConfig.setReplicaBrokerDtoList(body.replicaBrokerDtoList());
                brokerConfig.setPrimaryPartition(body.primaryPartition());
                return true;
            } catch (final Exception ex) {
                return false;
            }
        });
    }
}
