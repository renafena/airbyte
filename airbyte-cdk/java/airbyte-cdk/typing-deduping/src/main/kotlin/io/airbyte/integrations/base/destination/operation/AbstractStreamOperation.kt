/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.base.destination.operation

import io.airbyte.cdk.integrations.destination.StreamSyncSummary
import io.airbyte.cdk.integrations.destination.async.model.PartialAirbyteMessage
import io.airbyte.integrations.base.destination.typing_deduping.DestinationInitialStatus
import io.airbyte.integrations.base.destination.typing_deduping.InitialRawTableStatus
import io.airbyte.integrations.base.destination.typing_deduping.StreamConfig
import io.airbyte.integrations.base.destination.typing_deduping.migrators.MinimumDestinationState
import io.airbyte.protocol.models.v0.AirbyteStreamStatusTraceMessage.AirbyteStreamStatus
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.Optional
import java.util.stream.Stream

abstract class AbstractStreamOperation<DestinationState : MinimumDestinationState, Data>(
    private val storageOperation: StorageOperation<Data>,
    destinationInitialStatus: DestinationInitialStatus<DestinationState>,
    private val disableTypeDedupe: Boolean = false
) : StreamOperation<DestinationState> {
    private val log = KotlinLogging.logger {}

    // State maintained to make decision between async calls
    private val isTruncateSync: Boolean
    private val rawTableSuffix: String
    private val finalTmpTableSuffix: String
    private val initialRawTableStatus: InitialRawTableStatus =
        destinationInitialStatus.initialRawTableStatus

    /**
     * After running any sync setup code, we may update the destination state. This field holds that
     * updated destination state.
     */
    final override val updatedDestinationState: DestinationState

    init {
        val stream = destinationInitialStatus.streamConfig

        isTruncateSync = when (stream.minimumGenerationId) {
            0L -> false
            stream.generationId -> true
            else -> {
                // This is technically already handled in CatalogParser.
                throw IllegalArgumentException("Hybrid refreshes are not yet supported.")
            }
        }

        if (isTruncateSync) {
            if (initialRawTableStatus.tempRawTableExists) {
                val tempStageGeneration =
                    storageOperation.getStageGeneration(stream.id, TMP_TABLE_SUFFIX)
                if (tempStageGeneration != null && tempStageGeneration != stream.generationId) {
                    // The temp stage is from the wrong generation. Nuke it.
                    storageOperation.prepareStage(
                        stream.id,
                        TMP_TABLE_SUFFIX,
                        replace = true,
                    )
                }
                // (if the existing temp stage is from the correct generation, then we're resuming
                // a truncate refresh, and should keep the previous temp stage).
            } else {
                // We're initiating a new truncate refresh. Create a new temp stage.
                storageOperation.prepareStage(
                    stream.id,
                    TMP_TABLE_SUFFIX,
                )
            }
            rawTableSuffix = TMP_TABLE_SUFFIX
        } else {
            if (initialRawTableStatus.tempRawTableExists) {
                // There was a previous truncate refresh attempt, which failed, and left some
                // records behind.
                // Retrieve those records and put them in the real stage.
                storageOperation.transferFromTempStage(stream.id, TMP_TABLE_SUFFIX)
                // TODO refetch initial table status? or set initialRawTableStatus.hasUnprocessedRecords=true
            }
            rawTableSuffix = NO_SUFFIX
            storageOperation.prepareStage(stream.id, NO_SUFFIX)
        }

        if (!disableTypeDedupe) {
            // Prepare final tables based on sync mode.
            finalTmpTableSuffix = prepareFinalTable(destinationInitialStatus)
        } else {
            log.info { "Typing and deduping disabled, skipping final table initialization" }
            finalTmpTableSuffix = NO_SUFFIX
        }
        updatedDestinationState = destinationInitialStatus.destinationState.withSoftReset(false)
    }

    companion object {
        private const val NO_SUFFIX = ""
        private const val TMP_TABLE_SUFFIX = "_airbyte_tmp"
    }

    private fun prepareFinalTable(
        initialStatus: DestinationInitialStatus<DestinationState>
    ): String {
        val stream = initialStatus.streamConfig
        // No special handling if final table doesn't exist, just create and return
        if (!initialStatus.isFinalTablePresent) {
            log.info {
                "Final table does not exist for stream ${initialStatus.streamConfig.id.finalName}, creating."
            }
            storageOperation.createFinalTable(stream, NO_SUFFIX, false)
            return NO_SUFFIX
        }

        log.info { "Final Table exists for stream ${stream.id.finalName}" }
        // The table already exists. Decide whether we're writing to it directly, or
        // using a tmp table.
        if (isTruncateSync) {
            // Truncate refresh. Use a temp final table.
            return prepareFinalTableForOverwrite(initialStatus)
        } else {
            if (
                initialStatus.isSchemaMismatch ||
                initialStatus.destinationState.needsSoftReset()
            ) {
                // We're loading data directly into the existing table.
                // Make sure it has the right schema.
                // Also, if a raw table migration wants us to do a soft reset, do that
                // here.
                log.info { "Executing soft-reset on final table of stream $stream" }
                storageOperation.softResetFinalTable(stream)
            }
            return NO_SUFFIX
        }
    }

    private fun prepareFinalTableForOverwrite(
        initialStatus: DestinationInitialStatus<DestinationState>
    ): String {
        val stream = initialStatus.streamConfig
        if (!initialStatus.isFinalTableEmpty || initialStatus.isSchemaMismatch) {
            // overwrite an existing tmp table if needed.
            storageOperation.createFinalTable(stream, TMP_TABLE_SUFFIX, true)
            log.info {
                "Using temp final table for table ${stream.id.finalName}, this will be overwritten at end of sync"
            }
            // We want to overwrite an existing table. Write into a tmp table.
            // We'll overwrite the table at the end of the sync.
            return TMP_TABLE_SUFFIX
        }

        log.info {
            "Final Table for stream ${stream.id.finalName} is empty and matches the expected v2 format, writing to table directly"
        }
        return NO_SUFFIX
    }

    override fun writeRecords(streamConfig: StreamConfig, stream: Stream<PartialAirbyteMessage>) {
        // redirect to the appropriate raw table (potentially the temp raw table).
        writeRecordsImpl(
            streamConfig.copy(id = streamConfig.id.copy(rawName = streamConfig.id.rawName + rawTableSuffix)),
            stream,
        )
    }

    /** Write records will be destination type specific, Insert vs staging based on format */
    abstract fun writeRecordsImpl(
        streamConfig: StreamConfig,
        stream: Stream<PartialAirbyteMessage>
    )

    override fun finalizeTable(streamConfig: StreamConfig, syncSummary: StreamSyncSummary) {
        // Delete staging directory, implementation will handle if it has to do it or not or a No-OP
        storageOperation.cleanupStage(streamConfig.id)

        // Overwrite the raw table before doing anything else.
        // This ensures that if T+D fails, we can easily retain the records on the next sync.
        // It also means we don't need to run T+D using the temp raw table,
        // which is possible (`typeAndDedupe(streamConfig.id.copy(rawName = streamConfig.id.rawName + suffix))`
        // but annoying and confusing.
        if (isTruncateSync) {
            storageOperation.overwriteStage(streamConfig.id, rawTableSuffix)
        }

        if (disableTypeDedupe) {
            log.info {
                "Typing and deduping disabled, skipping final table finalization. " +
                    "Raw records can be found at ${streamConfig.id.rawNamespace}.${streamConfig.id.rawName}"
            }
            return
        }

        val shouldRunTypingDeduping =
            // Normal syncs should T+D regardless of status, so the user sees progress after
            // every attempt. And we should T+D records from this sync, _or_ a previous sync.
            (!isTruncateSync && (syncSummary.recordsWritten > 0 || initialRawTableStatus.hasUnprocessedRecords)) ||
                // But truncate syncs should only T+D if the sync was successful, since we're T+Ding
                // into a temp final table anyway. And we only need to check if _this_ sync emitted
                // records, since we've nuked the old raw data.
                (isTruncateSync && syncSummary.terminalStatus == AirbyteStreamStatus.COMPLETE && syncSummary.recordsWritten > 0)
        if (!shouldRunTypingDeduping) {
            log.info {
                "Skipping typing and deduping for stream ${streamConfig.id.originalNamespace}.${streamConfig.id.originalName} " +
                    "because it had no records during this sync and no unprocessed records from a previous sync."
            }
            return
        }

        // In truncate mode, we want to read all the raw records. Typically, this is equivalent
        // to filtering on timestamp, but might as well be explicit.
        val timestampFilter =
            if (!isTruncateSync) {
                initialRawTableStatus.maxProcessedTimestamp
            } else {
                Optional.empty()
            }
        storageOperation.typeAndDedupe(streamConfig, timestampFilter, finalTmpTableSuffix)

        // The `shouldRunTypingDeduping` check means we'll only ever reach this point if stream
        // status was COMPLETE, so we don't need to include
        // `&& syncSummary.terminalStatus == AirbyteStreamStatus.COMPLETE` in this clause.
        if (isTruncateSync && finalTmpTableSuffix.isNotBlank()) {
            storageOperation.overwriteFinalTable(streamConfig, finalTmpTableSuffix)
        }
    }
}
