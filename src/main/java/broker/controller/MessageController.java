package broker.controller;

import broker.dto.*;
import broker.service.MessageService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class MessageController {
    private final MessageService messageService;

    @PostMapping("/push")
    public ResponseEntity<Void> push(@RequestBody PushRequestDto pushRequestDto) {
        try{
            messageService.push(pushRequestDto);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @PostMapping("/pull")
    public ResponseEntity<MessageDto> pull(@RequestBody PullRequestDto pullRequestDto) {
        try{
            return ResponseEntity.ok(messageService.pull(pullRequestDto));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/health")
    public ResponseEntity<Void> health() {
        return ResponseEntity.ok().build();
    }
}
