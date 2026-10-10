package com.demo.notification.stream;

import com.demo.notification.api.FlowProgressEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@Component
public class FlowProgressStream {

    private static final int RECENT_EVENT_LIMIT = 200;

    private final CopyOnWriteArrayList<Subscriber> clients = new CopyOnWriteArrayList<>();
    private final List<FlowProgressEvent> recentEvents = new ArrayList<>();

    public SseEmitter subscribe(String customerId) {
        SseEmitter emitter = new SseEmitter(0L);
        clients.add(new Subscriber(customerId, emitter));
        emitter.onCompletion(() -> remove(emitter));
        emitter.onTimeout(() -> remove(emitter));
        emitter.onError(exception -> remove(emitter));
        try {
            emitter.send(SseEmitter.event().name("snapshot").data(recentSnapshot(customerId)));
        } catch (IOException | IllegalStateException exception) {
            remove(emitter);
            emitter.completeWithError(exception);
        }
        return emitter;
    }

    public void publish(FlowProgressEvent progress) {
        synchronized (recentEvents) {
            recentEvents.add(progress);
            if (recentEvents.size() > RECENT_EVENT_LIMIT) {
                recentEvents.remove(0);
            }
        }

        for (Subscriber subscriber : clients) {
            if (!subscriber.customerId().equals(progress.customerId())) {
                continue;
            }
            try {
                subscriber.emitter().send(SseEmitter.event().name("flow").data(progress));
            } catch (IOException | IllegalStateException exception) {
                remove(subscriber.emitter());
                subscriber.emitter().completeWithError(exception);
            }
        }
    }

    private List<FlowProgressEvent> recentSnapshot(String customerId) {
        synchronized (recentEvents) {
            return recentEvents.stream()
                    .filter(event -> customerId.equals(event.customerId()))
                    .toList();
        }
    }

    public void remove(SseEmitter emitter) {
        clients.removeIf(subscriber -> subscriber.emitter() == emitter);
    }

    private record Subscriber(String customerId, SseEmitter emitter) {
    }
}
