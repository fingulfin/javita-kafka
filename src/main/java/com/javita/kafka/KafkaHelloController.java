package com.javita.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@RestController
public class KafkaHelloController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final List<String> receivedMessages = new CopyOnWriteArrayList<>();
    private final List<HelloMessage> receivedJsonMessages = new ArrayList<HelloMessage>();

    private static final String TOPIC = "hello-topic";

    public KafkaHelloController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/kafka/send")
    public String send(@RequestParam("message") String message) {
        kafkaTemplate.send(TOPIC, message);
        return "Sent: " + message;
    }

    @PostMapping("/kafka/send-json")
    public String sendJson(@RequestBody HelloMessage message) {
        kafkaTemplate.send(TOPIC, message.toString());
        return "Sent: " + message;
    }

    @GetMapping("/kafka/received")
    public List<String> getReceivedMessages() {
        return new ArrayList<>(receivedMessages);
    }

    @GetMapping("/kafka/hello")
    public String hello() {
        return "Kafka Hello World! Use POST /kafka/send?message=... to send messages.";
    }

    @KafkaListener(topics = TOPIC, groupId = "hello-group")
    public void listen(String message) {
        receivedMessages.add(message);
    }
    @KafkaListener(topics = TOPIC, groupId = "hello-group")
    public void listen(HelloMessage message) {
        receivedJsonMessages.add(message);
    }

    public record HelloMessage(String greeting, String target) {
        @Override
        public String toString() {
            return greeting + ", " + target + "!";
        }
    }
}