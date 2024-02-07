package broker.controller;

import broker.dto.*;
import broker.service.MessageService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/message")
@RequiredArgsConstructor
public class MessageController {
    private final MessageService messageService;

    @PostMapping("/push")
    public ResponseEntity<PushResponseDto> push(@RequestBody PushRequestDto pushRequestDto) {
        try{
            return ResponseEntity.ok(messageService.push(pushRequestDto));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @PostMapping("/pull")
    public ResponseEntity<PullResponseDto> pull(@RequestBody PullRequestDto pullRequestDto) {
        try{
            return ResponseEntity.ok(messageService.pull(pullRequestDto));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }
}
