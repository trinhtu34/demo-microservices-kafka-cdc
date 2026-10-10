package com.demo.notification.application;

import com.demo.notification.api.NotificationResponse;
import com.demo.notification.consumer.InvalidCdcRecordException;
import com.demo.notification.consumer.ReservationCdcEvent;
import com.demo.notification.persistence.NotificationRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class NotificationPersistenceService {

    private final NotificationRepository repository;

    public NotificationPersistenceService(NotificationRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public NotificationResponse persist(ReservationCdcEvent event,
                                        String topic, int partition, long offset) {
        repository.insertIfAbsent(event.orderId(), event.sku(), event.quantity(),
                event.status(), event.reason(), topic, partition, offset);
        NotificationResponse stored = repository.findByOrderId(event.orderId())
                .orElseThrow(() -> new IllegalStateException("Notification was not stored"));
        if (!matches(stored, event)) {
            throw new InvalidCdcRecordException(
                    "An order id was replayed with a different final reservation result");
        }
        return stored;
    }

    @Transactional
    public void quarantine(String topic, int partition, long offset,
                           byte[] key, byte[] value, String reason) {
        repository.insertErrorIfAbsent(topic, partition, offset, key, value, reason);
    }

    private boolean matches(NotificationResponse notification, ReservationCdcEvent event) {
        return notification.sku().equals(event.sku())
                && notification.quantity() == event.quantity()
                && notification.status().equals(event.status())
                && java.util.Objects.equals(notification.reason(), event.reason());
    }
}
