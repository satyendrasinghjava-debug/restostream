/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.repository;

import com.satyendra.restostream.domain.TableSnapshot;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Persistence contract for the table aggregate. Fully non-blocking, and deliberately narrow: the
 * only mutating operation available to the orchestration layer is a <em>compare-and-set</em>.</p>
 *
 * <p>There is no unconditional {@code save}. Offering one would let a caller clobber a concurrent
 * transition from another RestoStream instance, and the resulting lost update would desynchronise
 * the floor plan from physical reality — exactly the failure a table-management system cannot
 * tolerate. Forcing every write through a version check makes that mistake unrepresentable.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public interface TableSnapshotRepository {

    /**
     * Loads one table.
     *
     * @param tableId the table identifier; must not be {@code null}
     * @return the snapshot, or an empty {@link Mono} when the table is not provisioned
     */
    Mono<TableSnapshot> findById(String tableId);

    /**
     * Loads the whole floor plan.
     *
     * @return every provisioned table, in unspecified order
     */
    Flux<TableSnapshot> findAll();

    /**
     * Atomically replaces a table snapshot only if its stored version still matches
     * {@code expectedVersion}.
     *
     * @param next            the snapshot to store; its {@code version} must be
     *                        {@code expectedVersion + 1}
     * @param expectedVersion the version the caller based its decision on
     * @return {@code true} when the write was applied, {@code false} when another writer won the
     *         race and the caller must re-read and re-decide
     */
    Mono<Boolean> compareAndSave(TableSnapshot next, long expectedVersion);

    /**
     * Provisions a table if, and only if, it does not already exist.
     *
     * @param snapshot the genesis snapshot; must be at version {@code 0}
     * @return {@code true} when this call created the table, {@code false} when it already existed
     */
    Mono<Boolean> provisionIfAbsent(TableSnapshot snapshot);

    /**
     * @return the number of provisioned tables
     */
    Mono<Long> count();

    /**
     * Removes the entire floor plan. Intended for test fixtures and operator-initiated resets.
     *
     * @return a {@link Mono} completing when the key space has been cleared
     */
    Mono<Void> deleteAll();
}
