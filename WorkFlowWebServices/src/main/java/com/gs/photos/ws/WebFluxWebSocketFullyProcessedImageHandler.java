package com.gs.photos.ws;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.gs.photo.common.workflow.Mailbox;
import com.workflow.model.files.FileToProcess;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class WebFluxWebSocketFullyProcessedImageHandler implements WebSocketHandler {

    public static class EventListener {
        protected Mailbox<FileToProcess> lastEvent;

        public FileToProcess getLastEvent() throws InterruptedException {
            try {
                return this.lastEvent.read();
            } finally {
                WebFluxWebSocketFullyProcessedImageHandler.LOGGER.info("Returning a message");
            }
        }

        @Subscribe
        public void lastEvent(FileToProcess lastEvent) { this.lastEvent.post(lastEvent); }

        public EventListener() { this.lastEvent = new Mailbox<>(); }

    }

    protected static Logger LOGGER   = LoggerFactory.getLogger(WebFluxWebSocketFullyProcessedImageHandler.class);

    @Autowired
    protected ObjectMapper  obkectMapper;

    private EventBus        eventBus = new EventBus();

    private Flux<FileToProcess> getPublisher() {

        EventListener listener = new EventListener();
        this.eventBus.register(listener);

        Executor executor = Executors.newSingleThreadExecutor();
        return Flux.create((e) -> {
            executor.execute(() -> {
                while (true) {
                    try {
                        e.next(listener.getLastEvent());
                    } catch (InterruptedException e1) {
                        throw new RuntimeException(e1);
                    }
                }
            });
        });
    }

    @Override
    public List<String> getSubProtocols() { return Arrays.asList("v11.stomp"); }

    @KafkaListener(topics = "${topic.topicFullyProcessedImage}", containerFactory = "kafkaListenerContainerFactoryForFileToProcess")
    public void consumeFullyImageProcessed(@Payload(required = false) FileToProcess message) {
        if (message != null) {
            WebFluxWebSocketComponentEventHandler.LOGGER.info("[FULLY_PROCESSED]Receiving a message {}", message);
            this.eventBus.post(message);
        } else {
            WebFluxWebSocketFullyProcessedImageHandler.LOGGER.warn("Kafka : Receive message null !");
        }
    }

    @Override
    public Mono<Void> handle(WebSocketSession webSocketSession) {
        WebFluxWebSocketFullyProcessedImageHandler.LOGGER.info("New session {}", webSocketSession);
        return webSocketSession.send(
            this.getPublisher()
                .map(f -> {
                    try {
                        return this.obkectMapper.writeValueAsString(f);
                    } catch (JsonProcessingException e) {
                        // TODO Auto-generated catch block
                        throw new RuntimeException(e);
                    }
                })
                .map(t -> webSocketSession.textMessage(t)));
    }

}