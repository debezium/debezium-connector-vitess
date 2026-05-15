/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.transforms;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import io.debezium.Module;

/**
 * A Single Message Transform that enforces a maximum record size.
 *
 * This is useful when downstream systems have a maximum message size limit.
 * The transform estimates the serialized size of the record and, if it exceeds
 * the configured maximum, applies a size reduction strategy.
 *
 * Supported strategies:
 * <ul>
 *   <li>Proportional column truncation: truncates string/bytes columns proportionally
 *       (larger columns are truncated more)</li>
 * </ul>
 *
 * @author Thomas Thornton
 */
public class EnforceRecordSize<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    public static final String MAX_BYTES_CONF = "max.bytes";
    public static final String COMPRESSION_RATIO_CONF = "compression.ratio";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(MAX_BYTES_CONF,
                    ConfigDef.Type.INT,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "The maximum record size in bytes. Records exceeding this size will have their " +
                            "string and bytes columns truncated proportionally to fit within this limit.")
            .define(COMPRESSION_RATIO_CONF,
                    ConfigDef.Type.DOUBLE,
                    1.0,
                    ConfigDef.Importance.MEDIUM,
                    "Ratio to account for record serialization differences. The estimated record size " +
                            "is multiplied by this ratio before comparing to the max size. " +
                            "For example, if your serialization compresses raw record size by 50%, " +
                            "set this to 0.5. Downstream systems typically provide metrics to discover " +
                            "the effective ratio, e.g. Kafka exposes " +
                            "kafka.producer:type=producer-metrics,client-id=<id>/compression-rate-avg. " +
                            "Default is 1.0 (no adjustment).");

    private int maxBytes;
    private double compressionRatio;

    @Override
    public R apply(R record) {
        if (record == null) {
            return null;
        }

        if (!(record.value() instanceof Struct)) {
            return record;
        }

        int rawEstimate = estimateRecordSizeBytes(record);
        int currentSize = (int) Math.ceil(rawEstimate * compressionRatio);
        if (currentSize <= maxBytes) {
            return record;
        }

        Struct value = (Struct) record.value();
        int excess = currentSize - maxBytes;

        truncateStructFields(value, "before", excess, currentSize);
        truncateStructFields(value, "after", excess, currentSize);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                value,
                record.timestamp());
    }

    private void truncateStructFields(Struct envelope, String fieldName, int excess, int totalSize) {
        Schema envelopeSchema = envelope.schema();
        if (envelopeSchema.field(fieldName) == null) {
            return;
        }
        Object fieldValue = envelope.get(fieldName);
        if (!(fieldValue instanceof Struct)) {
            return;
        }

        Struct dataStruct = (Struct) fieldValue;
        List<TruncatableField> truncatableFields = findTruncatableFields(dataStruct);
        if (truncatableFields.isEmpty()) {
            return;
        }

        int totalTruncatableBytes = truncatableFields.stream()
                .mapToInt(field -> field.sizeBytes)
                .sum();

        if (totalTruncatableBytes == 0) {
            return;
        }

        for (TruncatableField field : truncatableFields) {
            double proportion = (double) field.sizeBytes / totalTruncatableBytes;
            int bytesToRemove = (int) Math.ceil(proportion * excess);
            int newSizeBytes = Math.max(0, field.sizeBytes - bytesToRemove);

            if (field.isString) {
                String original = (String) field.value;
                if (original.length() > newSizeBytes) {
                    dataStruct.put(field.fieldName, original.substring(0, newSizeBytes));
                }
            }
            else if (field.isBytes) {
                ByteBuffer original = (ByteBuffer) field.value;
                if (original.limit() > newSizeBytes) {
                    dataStruct.put(field.fieldName, ByteBuffer.wrap(Arrays.copyOfRange(original.array(), 0, newSizeBytes)));
                }
            }
        }
    }

    // Uses str.length() as approximation (assumes 1 byte per char).
    // Not exact for multi-byte characters but avoids O(n) getBytes allocation per field.
    private static int estimateStringSize(String str) {
        return str.length();
    }

    private static int estimateByteBufferSize(ByteBuffer buffer) {
        return buffer.limit();
    }

    private static int estimateByteArraySize(byte[] bytes) {
        return bytes.length;
    }

    private static int estimateObjectSize(Object value) {
        if (value == null) {
            return 0;
        }
        if (value instanceof String) {
            return estimateStringSize((String) value);
        }
        if (value instanceof ByteBuffer) {
            return estimateByteBufferSize((ByteBuffer) value);
        }
        if (value instanceof byte[]) {
            return estimateByteArraySize((byte[]) value);
        }
        if (value instanceof Struct) {
            return estimateStructSize((Struct) value);
        }
        return 8;
    }

    private List<TruncatableField> findTruncatableFields(Struct dataStruct) {
        List<TruncatableField> result = new ArrayList<>();
        Schema schema = dataStruct.schema();

        for (Field field : schema.fields()) {
            Object value = dataStruct.get(field);
            if (value == null) {
                continue;
            }

            Schema.Type type = field.schema().type();

            if (type == Schema.Type.STRING) {
                result.add(new TruncatableField(field.name(), value, estimateStringSize((String) value), true, false));
            }
            else if (type == Schema.Type.BYTES) {
                result.add(new TruncatableField(field.name(), value, estimateByteBufferSize((ByteBuffer) value), false, true));
            }
        }

        return result;
    }

    public static <R extends ConnectRecord<R>> int estimateRecordSizeBytes(ConnectRecord<R> record) {
        int size = 0;
        size += estimateObjectSize(record.key());
        size += estimateObjectSize(record.value());
        return size;
    }

    private static int estimateStructSize(Struct struct) {
        int size = 0;
        Schema schema = struct.schema();
        for (Field field : schema.fields()) {
            size += estimateObjectSize(field.name());
            Object value = struct.get(field);
            size += estimateObjectSize(value);
        }
        return size;
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> props) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, props);
        int maxSize = config.getInt(MAX_BYTES_CONF);
        if (maxSize <= 0) {
            throw new ConfigException(MAX_BYTES_CONF, maxSize, "Must be a positive integer");
        }
        this.maxBytes = maxSize;

        double ratio = config.getDouble(COMPRESSION_RATIO_CONF);
        if (ratio <= 0) {
            throw new ConfigException(COMPRESSION_RATIO_CONF, ratio, "Must be a positive number");
        }
        this.compressionRatio = ratio;
    }

    private static class TruncatableField {
        final String fieldName;
        final Object value;
        final int sizeBytes;
        final boolean isString;
        final boolean isBytes;

        TruncatableField(String fieldName, Object value, int sizeBytes, boolean isString, boolean isBytes) {
            this.fieldName = fieldName;
            this.value = value;
            this.sizeBytes = sizeBytes;
            this.isString = isString;
            this.isBytes = isBytes;
        }
    }
}
