package com.demo.notification.api;

import com.demo.notification.application.NotificationQueryService;
import com.demo.notification.stream.NotificationStream;
import java.io.IOException;
import java.util.List;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequestMapping("/api/notifications")
@CrossOrigin(origins = {"http://localhost:8081", "http://127.0.0.1:8081"}, methods = org.springframework.web.bind.annotation.RequestMethod.GET)
public class NotificationController {

    private static final int HISTORY_LIMIT = 20;

    private final NotificationQueryService queryService;
    private final NotificationStream notificationStream;

    public NotificationController(NotificationQueryService queryService, NotificationStream notificationStream) {
        this.queryService = queryService;
        this.notificationStream = notificationStream;
    }

    @GetMapping
    public List<NotificationResponse> recent(@RequestParam(defaultValue = "20") int limit) {
        return queryService.recent(clampLimit(limit));
    }

    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() throws IOException {
        SseEmitter emitter = notificationStream.subscribe();
        try {
            notificationStream.sendSnapshot(emitter, queryService.recent(HISTORY_LIMIT));
            return emitter;
        } catch (IOException | RuntimeException exception) {
            notificationStream.remove(emitter);
            emitter.completeWithError(exception);
            throw exception;
        }
    }

    private int clampLimit(int requested) {
        return Math.max(1, Math.min(requested, 50));
    }
}
