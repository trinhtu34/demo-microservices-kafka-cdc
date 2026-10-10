package com.demo.notification.stream;

import com.demo.notification.api.NotificationResponse;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Component
public class NotificationStream {

    private final CopyOnWriteArrayList<SseEmitter> clients = new CopyOnWriteArrayList<>();

    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(0L);
        clients.add(emitter);
        emitter.onCompletion(() -> remove(emitter));
        emitter.onTimeout(() -> remove(emitter));
        emitter.onError(exception -> remove(emitter));
        return emitter;
    }

    public void sendSnapshot(SseEmitter emitter, List<NotificationResponse> notifications) throws IOException {
        emitter.send(SseEmitter.event().name("snapshot").data(notifications));
    }

    public void publish(NotificationResponse notification) {
        for (SseEmitter client : clients) {
            try {
                client.send(SseEmitter.event()
                        .id(Long.toString(notification.id()))
                        .name("notification")
                        .data(notification));
            } catch (IOException | IllegalStateException exception) {
                remove(client);
                client.completeWithError(exception);
            }
        }
    }

    public void remove(SseEmitter emitter) {
        clients.remove(emitter);
    }
}
