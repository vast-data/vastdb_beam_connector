package com.example.utils;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Utilities for converting between Beam and Arrow schemas.
 */
public class SchemaConverter {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaConverter.class);

    private SchemaConverter() {}

    /**
     * Converts a Beam schema to an Arrow schema.
     */
    public static Schema beamSchemaToArrowSchema(org.apache.beam.sdk.schemas.Schema beamSchema) {
        List<Field> fields = new ArrayList<>();
        
        for (org.apache.beam.sdk.schemas.Schema.Field field : beamSchema.getFields()) {
            Field arrowField = beamFieldToArrowField(field);
            if (arrowField != null) {
                fields.add(arrowField);
            }
        }
        
        return new Schema(fields);
    }

    /**
     * Converts a Beam field to an Arrow field.
     */
    private static Field beamFieldToArrowField(org.apache.beam.sdk.schemas.Schema.Field field) {
        String fieldName = field.getName();
        String typeName = field.getType().getTypeName().toString();
        
        switch (typeName) {
            case "STRING":
                return new Field(fieldName, FieldType.nullable(new ArrowType.Utf8()), null);
            case "INT64":
                return new Field(fieldName, FieldType.nullable(new ArrowType.Int(64, true)), null);
            case "INT32":
                return new Field(fieldName, FieldType.nullable(new ArrowType.Int(32, true)), null);
            case "FLOAT":
                return new Field(fieldName, FieldType.nullable(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE)), null);
            case "DOUBLE":
                return new Field(fieldName, FieldType.nullable(new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)), null);
            case "BOOLEAN":
                return new Field(fieldName, FieldType.nullable(new ArrowType.Bool()), null);
            // Add more type mappings as needed
            default:
                LOG.warn("Unsupported Beam field type: {} for field {}", typeName, fieldName);
                return null;
        }
    }

    /**
     * Attempts to map an Arrow schema to a Beam schema.
     */
    public static org.apache.beam.sdk.schemas.Schema arrowSchemaToBeamSchema(Schema arrowSchema) {
        org.apache.beam.sdk.schemas.Schema.Builder builder = org.apache.beam.sdk.schemas.Schema.builder();
        
        for (Field field : arrowSchema.getFields()) {
            // Skip row_id field which is managed by VastDB
            if (field.getName().equals("$row_id")) {
                continue;
            }
            
            org.apache.beam.sdk.schemas.Schema.FieldType fieldType = arrowFieldToBeamFieldType(field);
            if (fieldType != null) {
                builder.addNullableField(field.getName(), fieldType);
            }
        }
        
        return builder.build();
    }

    /**
     * Converts an Arrow field to a Beam field type.
     */
    private static org.apache.beam.sdk.schemas.Schema.FieldType arrowFieldToBeamFieldType(Field field) {
        ArrowType arrowType = field.getType();
        
        if (arrowType instanceof ArrowType.Utf8) {
            return org.apache.beam.sdk.schemas.Schema.FieldType.STRING;
        } else if (arrowType instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) arrowType;
            if (intType.getBitWidth() <= 32) {
                return org.apache.beam.sdk.schemas.Schema.FieldType.INT32;
            } else {
                return org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
            }
        } else if (arrowType instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint fpType = (ArrowType.FloatingPoint) arrowType;
            if (fpType.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE) {
                return org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT;
            } else {
                return org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE;
            }
        } else if (arrowType instanceof ArrowType.Bool) {
            return org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN;
        }
        
        LOG.warn("Unsupported Arrow type: {} for field {}", arrowType, field.getName());
        return null;
    }

    /**
     * Uses Apache Beam's built-in Arrow conversion if available.
     */
    public static org.apache.beam.sdk.schemas.Schema arrowSchemaToBeamSchemaWithBeamUtils(Schema arrowSchema) {
        try {
            // For compatibility with earlier versions of Beam that may not have ArrowConversion
            Class<?> arrowConversionClass = Class.forName("org.apache.beam.sdk.extensions.arrow.ArrowConversion");
            java.lang.reflect.Method method = arrowConversionClass.getMethod("arrowSchemaToBeamSchema", Schema.class);
            return (org.apache.beam.sdk.schemas.Schema) method.invoke(null, arrowSchema);
        } catch (Exception e) {
            LOG.warn("Failed to use Beam's Arrow conversion, falling back to custom converter", e);
            return arrowSchemaToBeamSchema(arrowSchema);
        }
    }
    
    /**
     * Uses Apache Beam's built-in Arrow conversion if available.
     */
    public static Schema beamSchemaToArrowSchemaWithBeamUtils(org.apache.beam.sdk.schemas.Schema beamSchema) {
        try {
            // For compatibility with earlier versions of Beam that may not have ArrowConversion
            Class<?> arrowConversionClass = Class.forName("org.apache.beam.sdk.extensions.arrow.ArrowConversion");
            java.lang.reflect.Method method = arrowConversionClass.getMethod("beamSchemaToArrowSchema", org.apache.beam.sdk.schemas.Schema.class);
            return (Schema) method.invoke(null, beamSchema);
        } catch (Exception e) {
            LOG.warn("Failed to use Beam's Arrow conversion, falling back to custom converter", e);
            return beamSchemaToArrowSchema(beamSchema);
        }
    }
}