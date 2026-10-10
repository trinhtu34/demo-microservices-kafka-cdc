package com.demo.notification.api;

import com.demo.notification.stream.FlowProgressStream;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
@RequestMapping("/api/flow")
@CrossOrigin(originPatterns = {"http://localhost:*", "http://127.0.0.1:*", "http://100.*:*"},
        methods = org.springframework.web.bind.annotation.RequestMethod.GET)
public class FlowController {

    private final FlowProgressStream flowProgressStream;

    public FlowController(FlowProgressStream flowProgressStream) {
        this.flowProgressStream = flowProgressStream;
    }

    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(@RequestParam DemoCustomerId customerId) {
        return flowProgressStream.subscribe(customerId.name());
    }
}
