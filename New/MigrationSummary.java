package com.example.migrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public record MigrationSummary(
        String spaceKey,
        String localZipPath,
        String fileReference,
        String xwikiFinalStatusJson,
        long exportTimeMs,
        long importTimeMs,
        long totalTimeMs,
        String result,
        String errorDetails
) {

    public String toPrettyJson() {
        try {
            ObjectMapper m = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
            return m.writeValueAsString(this);
        } catch (Exception e) {
            return this.toString();
        }
    }
}
