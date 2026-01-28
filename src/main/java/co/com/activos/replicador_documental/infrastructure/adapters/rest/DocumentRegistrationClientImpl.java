package co.com.activos.replicador_documental.infrastructure.adapters.rest;

import co.com.activos.replicador_documental.infrastructure.adapters.rest.model.DocumentRegistrationRequest;
import co.com.activos.replicador_documental.infrastructure.adapters.rest.model.DocumentRegistrationResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.Collections;

/**
 * Implementation of the DocumentRegistrationClient using Spring's RestTemplate
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DocumentRegistrationClientImpl implements DocumentRegistrationClient {

    @Value("${document.registration.api.url:https://ms-tr-gestor-documental-194964492367.us-east1.run.app/api/register}")
    private String apiUrl;

    @Value("${document.registration.api.token:sk_app_prueba_72K9xYpQrStUvWxZaBcDeFgHiJmNoPqR1234567890}")
    private String apiToken;

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public DocumentRegistrationResponse registerDocument(DocumentRegistrationRequest request) {
        try {
            log.info("Iniciando registro de documento con ID: {}", request.getDocumentId());

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.MULTIPART_FORM_DATA);
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
            headers.add("X-Current-Token",apiToken);

            MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();

            // Add document ID
            body.add("documentId", request.getDocumentId());

            // Add file
            if (request.getFile() != null && !request.getFile().isEmpty()) {
                body.add("file", new ByteArrayResource(request.getFile().getBytes()) {
                    @Override
                    public String getFilename() {
                        return request.getFile().getOriginalFilename();
                    }
                });
            }

            // Add params as JSON string
            if (request.getParams() != null) {
                String paramsJson = objectMapper.writeValueAsString(request.getParams());
                body.add("params", paramsJson);
            }

            HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(body, headers);

            log.debug("Enviando solicitud de registro de documento a: {}", apiUrl);

            ResponseEntity<DocumentRegistrationResponse> response = restTemplate.exchange(
                    apiUrl,
                    HttpMethod.POST,
                    requestEntity,
                    DocumentRegistrationResponse.class
            );

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                log.info("Documento registrado exitosamente con ID: {}", request.getDocumentId());
                return response.getBody();
            } else {
                log.error("Error al registrar el documento. Código de estado: {}", response.getStatusCodeValue());
                return DocumentRegistrationResponse.builder()
                        .success(false)
                        .message("Error al registrar el documento. Código: " + response.getStatusCodeValue())
                        .documentId(request.getDocumentId())
                        .build();
            }

        } catch (IOException e) {
            log.error("Error al procesar el archivo para el documento ID: " + request.getDocumentId(), e);
            return DocumentRegistrationResponse.builder()
                    .success(false)
                    .message("Error al procesar el archivo: " + e.getMessage())
                    .documentId(request.getDocumentId())
                    .build();
        } catch (Exception e) {
            log.error("Error inesperado al registrar el documento ID: " + request.getDocumentId(), e);
            return DocumentRegistrationResponse.builder()
                    .success(false)
                    .message("Error inesperado: " + e.getMessage())
                    .documentId(request.getDocumentId())
                    .build();
        }
    }
}
