package com.example.config;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Validation;

/**
 * Options for configuring VastDB pipeline.
 */
public interface VastDbPipelineOptions extends PipelineOptions {
    
    @Description("VastDB endpoint URL")
    @Validation.Required
    ValueProvider<String> getVastDbEndpoint();
    void setVastDbEndpoint(ValueProvider<String> value);

    @Description("VastDB schema name")
    @Default.String("vastdb/s2")
    ValueProvider<String> getVastDbSchemaName();
    void setVastDbSchemaName(ValueProvider<String> value);

    @Description("VastDB table name")
    @Validation.Required
    ValueProvider<String> getVastDbTableName();
    void setVastDbTableName(ValueProvider<String> value);

    @Description("Input file path")
    @Validation.Required
    ValueProvider<String> getInputFile();
    void setInputFile(ValueProvider<String> value);

    @Description("Batch size for VastDB writes")
    @Default.Integer(100)
    ValueProvider<Integer> getBatchSize();
    void setBatchSize(ValueProvider<Integer> value);
}
