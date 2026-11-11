/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.service;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Raised when a signal, command or query references a table that is not part of the provisioned
 * floor plan. Surfaced as {@code 404 Not Found} at the REST boundary.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public class TableNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String tableId;

    /**
     * @param tableId the table identifier that could not be resolved
     */
    public TableNotFoundException(String tableId) {
        super("table '" + tableId + "' is not part of the provisioned floor plan");
        this.tableId = tableId;
    }

    /**
     * @return the table identifier that could not be resolved
     */
    public String tableId() {
        return tableId;
    }
}
