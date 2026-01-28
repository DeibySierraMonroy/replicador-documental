package co.com.activos.replicador_documental.domain.usecase;

import co.com.activos.replicador_documental.domain.model.*;
import co.com.activos.replicador_documental.helpers.UseCase;
import co.com.activos.replicador_documental.infrastructure.adapters.rest.DocumentRegistrationClient;
import co.com.activos.replicador_documental.infrastructure.adapters.rest.model.DocumentRegistrationRequest;
import co.com.activos.replicador_documental.infrastructure.adapters.soap.BatchSoapClientAdapter;
import co.com.activos.replicador_documental.infrastructure.adapters.soap.SoapClientManualImpl;
import co.com.activos.replicador_documental.infrastructure.adapters.soap.model.SolicitarArchivoResponse;
import co.com.activos.replicador_documental.domain.model.MigrationMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ReplicarUseCase implements UseCase<Long, String> {

    private final ParamRepository paramRepository;
    private final AzDigitalRepository azDigitalRepository;
    private final SoapClientManualImpl soapClientManual;
    private final BatchSoapClientAdapter batchSoapClientAdapter;
    private final DocumentRegistrationClient documentRegistrationClient;
    private final ObjectMapper objectMapper;

    private static final int BATCH_SIZE = 200;
    private static final int PARALLEL_THREADS = 8;
    private static final int MAX_PAGES = 100000;

    @Override
    public String ejecutar(Long txpCodigo) {
        long executionId = System.currentTimeMillis();
        String txpCodigoStr = txpCodigo.toString();
        
        AtomicLong totalCarpetas = new AtomicLong(0);
        AtomicLong totalDocumentos = new AtomicLong(0);
        AtomicLong documentosMigrados = new AtomicLong(0);
        AtomicLong documentosFallidos = new AtomicLong(0);
        
        log.info("Iniciando migración para txpCodigo: {}", txpCodigo);
        
        ExecutorService executor = Executors.newFixedThreadPool(PARALLEL_THREADS);
        
        try {
            int pageSize = 50;
            int pageNumber = 0;
            
            while (pageNumber < MAX_PAGES) {
                try {
                    List<TaxonomiaParam> parametros = paramRepository.listarPorTipoFlujo(txpCodigo, pageNumber, pageSize);
                    
                    if (parametros.isEmpty()) {
                        break;
                    }
                    
                    totalCarpetas.addAndGet(parametros.size());
                    
                    List<CompletableFuture<Void>> futures = parametros.stream()
                            .map(param -> CompletableFuture.runAsync(() -> {
                                try {
                                    procesarCarpeta(param, txpCodigoStr, executionId, 
                                            totalDocumentos, totalCarpetas, documentosMigrados, documentosFallidos);
                                } catch (Exception e) {
                                    log.error("Error carpeta {}: {}", param.getCodigo(), e.getMessage());
                                }
                            }, executor))
                            .toList();
                    
                    try {
                        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                                .get(5, TimeUnit.MINUTES);
                    } catch (java.util.concurrent.TimeoutException e) {
                        log.error("Timeout procesando página {}", pageNumber);
                        futures.forEach(f -> f.cancel(true));
                    }
                    
                    pageNumber++;
                    Thread.sleep(10);
                    
                } catch (Exception e) {
                    pageNumber++;
                    continue;
                }
            }
            
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        long tiempoTotal = (System.currentTimeMillis() - executionId) / 1000;
        log.info("Migración completada - Carpetas: {}, Migrados: {}, Fallidos: {}, Tiempo: {}s", 
                totalCarpetas.get(), documentosMigrados.get(), documentosFallidos.get(), tiempoTotal);
        
        return String.format("Migración completada. Carpetas: %d, Migrados: %d, Fallidos: %d", 
                totalCarpetas.get(), documentosMigrados.get(), documentosFallidos.get());
    }

    
    public String ejecutarPorAnio(String payload) {
        try {
            String cleanPayload = payload.replaceFirst("^\"|\"$", "");
            MigrationMessage message = objectMapper.readValue(cleanPayload, MigrationMessage.class);
            
            log.info("Iniciando migración asíncrona - txpCodigo: {}, año: {}", message.getTxpCodigo(), message.getAnio());
            
            long executionId = System.currentTimeMillis();
            String txpCodigoStr = message.getTxpCodigo().toString();
        
        AtomicLong totalCarpetas = new AtomicLong(0);
        AtomicLong totalDocumentos = new AtomicLong(0);
        AtomicLong documentosMigrados = new AtomicLong(0);
        AtomicLong documentosFallidos = new AtomicLong(0);
        
        ExecutorService executor = Executors.newFixedThreadPool(PARALLEL_THREADS);
        
        try {
            int pageSize = 100;
            int pageNumber = 0;
            
            while (pageNumber < MAX_PAGES) {
                try {
                    List<TaxonomiaParam> parametros = paramRepository.listarPorTipoFlujoYAnio(message.getTxpCodigo(), message.getAnio(), pageNumber, pageSize);
                    
                    if (parametros.isEmpty()) {
                        break;
                    }
                    
                    totalCarpetas.addAndGet(parametros.size());
                    
                    List<CompletableFuture<Void>> futures = parametros.stream()
                            .map(param -> CompletableFuture.runAsync(() -> {
                                try {
                                    procesarCarpeta(param, txpCodigoStr, executionId, 
                                            totalDocumentos, totalCarpetas, documentosMigrados, documentosFallidos);
                                } catch (Exception e) {
                                    log.error("Error carpeta {}: {}", param.getCodigo(), e.getMessage());
                                }
                            }, executor))
                            .toList();
                    
                    try {
                        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                                .get(15, TimeUnit.MINUTES);
                    } catch (java.util.concurrent.TimeoutException e) {
                        log.error("Timeout procesando página {} (año {})", pageNumber, message.getAnio());
                        futures.forEach(f -> f.cancel(true));
                    }
                    
                    pageNumber++;
                    Thread.sleep(10);
                    
                } catch (Exception e) {
                    pageNumber++;
                    continue;
                }
            }
            
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        long tiempoTotal = (System.currentTimeMillis() - executionId) / 1000;
        log.info("Migración año {} completada - Carpetas: {}, Migrados: {}, Fallidos: {}, Tiempo: {}s", 
                message.getAnio(), totalCarpetas.get(), documentosMigrados.get(), documentosFallidos.get(), tiempoTotal);
        
        return String.format("Migración año %d completada. Carpetas: %d, Migrados: %d, Fallidos: %d", 
                message.getAnio(), totalCarpetas.get(), documentosMigrados.get(), documentosFallidos.get());
                
        } catch (Exception e) {
            log.error("Error procesando migración asíncrona: {}", e.getMessage());
            return "Error en migración asíncrona: " + e.getMessage();
        }
    }
    
    // Método de conteo eliminado - ahora el total se va descubriendo gradualmente durante el procesamiento
    
    private void procesarCarpeta(TaxonomiaParam param, String txpCodigoStr, long executionId,
                                AtomicLong totalDocumentos, AtomicLong totalCarpetas, AtomicLong documentosMigrados, AtomicLong documentosFallidos) throws InterruptedException {
        
        List<String> codigosCliente = new ArrayList<>();
        List<AzDigital> todosLosAzDigitales = new ArrayList<>();
        Pageable pageable = PageRequest.of(0, BATCH_SIZE);
        List<AzDigital> azDigitales;
        
        do {
            azDigitales = azDigitalRepository.listarPorCarpeta(param.getCodigo(), pageable);
            totalDocumentos.addAndGet(azDigitales.size());
            
            codigosCliente.addAll(azDigitales.stream()
                    .map(AzDigital::getCodigoCli)
                    .collect(Collectors.toList()));
            
            todosLosAzDigitales.addAll(azDigitales);
            
            pageable = pageable.next();
        } while (!azDigitales.isEmpty());
        
        if (codigosCliente.isEmpty()) {
            return;
        }
        
        long currentTotal = totalDocumentos.get();
        if (currentTotal > 0 && currentTotal % 1000 == 0) {
            log.info("Total documentos descubiertos: {}", currentTotal);
        }
        
        int batchSize = 20;
        
        Map<String, AzDigital> azDigitalMap = todosLosAzDigitales.stream()
                .collect(Collectors.toMap(AzDigital::getCodigoCli, az -> az));
        
        for (int i = 0; i < codigosCliente.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, codigosCliente.size());
            List<String> batch = codigosCliente.subList(i, endIndex);
            
            for (String codigoCliente : batch) {
                try {
                    AzDigital azDigital = azDigitalMap.get(codigoCliente);
                    
                    if (azDigital == null) {
                        continue;
                    }
                    
                    SolicitarArchivoResponse soapResponse = soapClientManual.solicitarArchivo(codigoCliente);
                    
                    if (soapResponse == null || soapResponse.getArchivo() == null) {
                        continue;
                    }
                    
                    DocumentRegistrationRequest request = crearDocumentRegistrationRequest(azDigital, soapResponse, param);
                    documentRegistrationClient.registerDocument(request);
                    
                    documentosMigrados.incrementAndGet();
                    
                } catch (Exception e) {
                    log.error("Error procesando documento {}: {}", codigoCliente, e.getMessage());
                    documentosFallidos.incrementAndGet();
                }
            }
            
            long current = documentosMigrados.get() + documentosFallidos.get();
            if (current % 100 == 0) {
                log.info("Progreso: Docs {}/{} - Carpetas: {}", current, totalDocumentos.get(), totalCarpetas.get());
            }
            
            if (i + batchSize < codigosCliente.size()) {
                Thread.sleep(5);
            }
        }
    }
    
    private void procesarDocumento(AzDigital azDigital, TaxonomiaParam param, String txpCodigoStr, long executionId,
                                 AtomicLong documentosMigrados, AtomicLong documentosFallidos) {
        try {
            SolicitarArchivoResponse archivoResponse = soapClientManual.solicitarArchivo(azDigital.getCodigoCli());
            
            if (archivoResponse == null || archivoResponse.getArchivo() == null) {
                documentosFallidos.incrementAndGet();
                return;
            }
            
            DocumentRegistrationRequest request = crearDocumentRegistrationRequest(azDigital, archivoResponse, param);
            documentRegistrationClient.registerDocument(request);
            
            documentosMigrados.incrementAndGet();
            
        } catch (Exception e) {
            documentosFallidos.incrementAndGet();
        }
    }

    private DocumentRegistrationRequest crearDocumentRegistrationRequest(
            AzDigital azDigital, SolicitarArchivoResponse archivoResponse,
            TaxonomiaParam param) {

        return new DocumentRegistrationRequest(
                azDigital.getPrdCodigo(),
                base64ToMultipart( archivoResponse.getArchivo().getContenido(), archivoResponse.getArchivo().getNombre(), archivoResponse.getArchivo().getContenido()),
                extraerTipoYNumeroDocumento(param.getNombre()));
    }

    private MultipartFile base64ToMultipart(String base64, String fileName, String contentType) {
        byte[] fileBytes = Base64.getDecoder().decode(base64);
        return new MockMultipartFile(fileName, fileName, contentType, fileBytes);
    }

    private DocumentRegistrationRequest.DocumentParams extraerTipoYNumeroDocumento(String nombre) {
        String[] partes = nombre.split(" ");
        String tipoDoc = partes.length > 1 ? partes[1] : "";
        String numeroDoc = partes.length > 2 ? partes[2] : "";
        return new DocumentRegistrationRequest.DocumentParams(tipoDoc, numeroDoc);
    }

}
