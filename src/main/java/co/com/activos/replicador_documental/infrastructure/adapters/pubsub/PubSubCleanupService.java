package co.com.activos.replicador_documental.infrastructure.adapters.pubsub;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ProjectSubscriptionName;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class PubSubCleanupService {
    
    @Value("${activos.gcp.pubsub.project-id}")
    private String projectId;
    
    @Value("${activos.gcp.pubsub.topics.vinculacion.name}")
    private String topicName;
    
    public void cleanupPendingMessages() {
        String subscriptionId = topicName + "-sub";
        ProjectSubscriptionName subscription = ProjectSubscriptionName.of(projectId, subscriptionId);
        
        log.info("Iniciando limpieza de mensajes pendientes en subscription: {}", subscriptionId);
        
        CountDownLatch latch = new CountDownLatch(1);
        
        MessageReceiver receiver = (PubsubMessage message, AckReplyConsumer consumer) -> {
            try {
                String payload = message.getData().toStringUtf8();
                log.info("Haciendo ACK a mensaje pendiente: {}", payload);
                
                // Hacer ack inmediato sin procesar
                consumer.ack();
                
            } catch (Exception e) {
                log.error("Error haciendo ACK a mensaje: {}", e.getMessage());
                consumer.nack(); // Reintentar si hay error
            }
        };
        
        Subscriber subscriber = Subscriber.newBuilder(subscription, receiver).build();
        subscriber.startAsync();
        
        try {
            // Esperar 30 segundos para procesar mensajes pendientes
            boolean finished = latch.await(30, TimeUnit.SECONDS);
            
            if (finished) {
                log.info("Limpieza completada exitosamente");
            } else {
                log.info("Tiempo de limpieza finalizado - procesados los mensajes disponibles");
            }
            
        } catch (InterruptedException e) {
            log.warn("Limpieza interrumpida");
        } finally {
            subscriber.stopAsync();
            log.info("Cleanup detenido");
        }
    }
    
    public void quickAck() {
        log.info("Iniciando ACK rápido de todos los mensajes pendientes...");
        cleanupPendingMessages();
    }
}
