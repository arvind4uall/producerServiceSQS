package com.fsdarvind.sqs.producerService.controller;

import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class MessageProducerController {

    // SqsTemplate is the Spring Cloud AWS helper to send messages — think of it like RestTemplate but for SQS
    private final SqsTemplate sqsTemplate;

    // Reading queue names from application.yml — good practice, no hardcoding
    @Value("${app.queues.main}")
    private String mainQueue;

    @Value("${app.queues.dlq}")
    private String dlqQueue;

    public MessageProducerController(SqsTemplate sqsTemplate) {
        this.sqsTemplate = sqsTemplate;
    }

    // This endpoint sends a normal message to the main queue
    // Call it like: POST /api/send  with body: {"message": "Hello World"}
    @PostMapping("/send")
    public String sendToMainQueue(@RequestBody String message) {

        // sqsTemplate.send() takes the queue name and the message payload
        // Spring Cloud AWS resolves the full queue URL automatically using the queue name
        sqsTemplate.send(mainQueue, message);

        System.out.println("Sent to main queue: " + message);
        return "Message sent to main queue: " + message;
    }

    // This endpoint sends a message DIRECTLY to the DLQ — bypassing the main queue
    // Useful for testing: "what happens when a bad message lands in the DLQ?"
    // Call it like: POST /api/send-to-dlq  with body: {"message": "Bad message"}
    @PostMapping("/send-to-dlq")
    public String sendToDlq(@RequestBody String message) {

        // Same sqsTemplate.send() — just pointing at the DLQ instead
        sqsTemplate.send(dlqQueue, message);

        System.out.println("Sent directly to DLQ: " + message);
        return "Message sent directly to DLQ: " + message;
    }
}