package com.demo.notification.application;

import com.demo.notification.api.NotificationResponse;
import com.demo.notification.persistence.NotificationRepository;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class NotificationQueryService {

    private final NotificationRepository repository;

    public NotificationQueryService(NotificationRepository repository) {
        this.repository = repository;
    }

    public List<NotificationResponse> recent(int limit) {
        return repository.findRecent(limit);
    }
}
