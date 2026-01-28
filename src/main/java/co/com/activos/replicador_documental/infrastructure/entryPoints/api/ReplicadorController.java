package co.com.activos.replicador_documental.infrastructure.entryPoints.api;


import co.com.activos.replicador_documental.domain.usecase.ReplicarUseCase;
import co.com.activos.replicador_documental.infrastructure.adapters.pubsub.ManualPubSubPublisher;
import co.com.activos.replicador_documental.domain.model.MigrationMessage;
import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/replicar")
@AllArgsConstructor
class ReplicadorController {

    private final ReplicarUseCase replicarUseCase;
    private final ManualPubSubPublisher manualPubSubPublisher;

    @PostMapping("/{txpCodigo}/anio/{anio}/async")
    public ResponseEntity<String> ejecutarReplicacionAsync(@PathVariable Long txpCodigo, @PathVariable int anio) {
        try {
            MigrationMessage message = MigrationMessage.builder()
                    .txpCodigo(txpCodigo)
                    .anio(anio)
                    .messageId(java.util.UUID.randomUUID().toString())
                    .status("PENDING")
                    .build();
            
            manualPubSubPublisher.publish("migration-topic", message);
            return ResponseEntity.ok("Proceso iniciado exitosamente.");
            
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Error publicando en Pub/Sub: " + e.getMessage());
        }
    }

    @GetMapping("/bhv/{txpCodigo}")
    public ResponseEntity<String> replicarBhv(@PathVariable Long txpCodigo) {
        try {
            String resultado = replicarUseCase.ejecutar(txpCodigo);
            return ResponseEntity.ok(resultado);
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body("Error al replicar carpetas: " + e.getMessage());
        }
    }
}
