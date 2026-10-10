package com.demo.notification.stream;

import com.demo.notification.api.NotificationResponse;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Component
public class NotificationStream {

    private final CopyOnWriteArrayList<Subscriber> clients = new CopyOnWriteArrayList<>();

    public SseEmitter subscribe(String customerId) {
        SseEmitter emitter = new SseEmitter(0L);
        clients.add(new Subscriber(customerId, emitter));
        emitter.onCompletion(() -> remove(emitter));
        emitter.onTimeout(() -> remove(emitter));
        emitter.onError(exception -> remove(emitter));
        return emitter;
    }

    public void sendSnapshot(SseEmitter emitter, List<NotificationResponse> notifications) throws IOException {
        emitter.send(SseEmitter.event().name("snapshot").data(notifications));
    }

    public void publish(NotificationResponse notification) {
        for (Subscriber subscriber : clients) {
            if (!subscriber.customerId().equals(notification.customerId())) {
                continue;
            }
            try {
                subscriber.emitter().send(SseEmitter.event()
                        .id(Long.toString(notification.id()))
                        .name("notification")
                        .data(notification));
            } catch (IOException | IllegalStateException exception) {
                remove(subscriber.emitter());
                subscriber.emitter().completeWithError(exception);
            }
        }
    }

    public void remove(SseEmitter emitter) {
        clients.removeIf(subscriber -> subscriber.emitter() == emitter);
    }

    private record Subscriber(String customerId, SseEmitter emitter) {
    }
}
