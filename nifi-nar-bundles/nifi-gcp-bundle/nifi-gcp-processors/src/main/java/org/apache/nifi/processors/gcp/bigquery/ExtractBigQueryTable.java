package org.apache.nifi.processors.gcp.bigquery;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;
import org.threeten.bp.Duration;
import org.threeten.bp.temporal.ChronoUnit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"google", "google cloud", "bq", "bigquery"})
@CapabilityDescription("Extract BigQuery table content to GCS or local.")
public class ExtractBigQueryTable extends AbstractBigQueryProcessor {

    private static final PropertyDescriptor DESTINATION_FORMAT = new PropertyDescriptor.Builder()
            .name("DesinationFormat")
            .displayName("Destination Format")
            .description("Support data format: AVRO, CSV (default), NEWLINE_DELIMITED_JSON")
            .required(true)
            .defaultValue("CSV")
            .allowableValues("AVRO", "CSV", "NEWLINE_DELIMITED_JSON")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor COMPRESSION_TYPE = new PropertyDescriptor.Builder()
            .name("CompressionType")
            .displayName("Compression Type")
            .description("Support compression type: GZIP (for CSV, JSON), DEFLATE, SNAPPY (for AVRO)")
            .required(true)
            .defaultValue("GZIP")
            .allowableValues("GZIP", "DEFLATE", "SNAPPY")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final PropertyDescriptor DESTINATION_URIS = new PropertyDescriptor.Builder()
            .name("DestinationUris")
            .displayName("Destination Uris")
            .description("The destinationUris property indicates the location(s) and file name(s) where BigQuery should export your files. Ref: https://cloud.google.com/bigquery/docs/exporting-data")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private static final PropertyDescriptor DESTINATION_LOCATION = new PropertyDescriptor.Builder()
            .name("DestinationLocation")
            .displayName("Destination Location")
            .description("(Optional) The location of GCS. It must be the same location with BigQuery table.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .add(DESTINATION_FORMAT)
                .add(COMPRESSION_TYPE)
                .add(DESTINATION_URIS)
                .add(DESTINATION_LOCATION)
                .build();
    }

    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        super.onScheduled(context);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String projectId = context.getProperty(PROJECT_ID).evaluateAttributeExpressions().getValue();
        final String dataset = context.getProperty(DATASET).evaluateAttributeExpressions(flowFile).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String destinationFormat = context.getProperty(DESTINATION_FORMAT).evaluateAttributeExpressions().getValue();
        final String compressType = context.getProperty(COMPRESSION_TYPE).evaluateAttributeExpressions().getValue();
        final String destinationUris = context.getProperty(DESTINATION_URIS).evaluateAttributeExpressions(flowFile).getValue().trim();
        final String destinationLocation = context.getProperty(DESTINATION_LOCATION).evaluateAttributeExpressions(flowFile).getValue();

        final TableId tableId;
        if (StringUtils.isEmpty(projectId)) {
            tableId = TableId.of(dataset, tableName);
        } else {
            tableId = TableId.of(projectId, dataset, tableName);
        }

        try {
            List<String> listDestinationUri = new ArrayList<>();
            if (destinationUris.startsWith("[") && destinationUris.endsWith("]")) {
                Gson gson = new GsonBuilder().create();
                listDestinationUri = gson.fromJson(destinationUris, ArrayList.class);
            } else {
                listDestinationUri.add(destinationUris);
            }

            ExtractJobConfiguration configuration = ExtractJobConfiguration
                    .newBuilder(tableId, listDestinationUri)
                    .setFormat(destinationFormat)
                    .setCompression(compressType)
                    .build();

            Job job = getCloudService().create(JobInfo.of(configuration));
            //Job job = getCloudService().getTable(tableId).extract(destinationFormat, listDestinationUri);
            // Wait for the job to complete
            try {
                Long timePeriod = context.getProperty(READ_TIMEOUT).evaluateAttributeExpressions(flowFile).asTimePeriod(TimeUnit.SECONDS);
                Duration waitFor = Duration.of(timePeriod, ChronoUnit.SECONDS);
                job = job.waitFor(RetryOption.totalTimeout(waitFor));

                if (job != null) {
                    final Map<String, String> attributes = new HashMap<>();

                    attributes.put(BigQueryAttributes.JOB_CREATE_TIME_ATTR, Long.toString(job.getStatistics().getCreationTime()));
                    attributes.put(BigQueryAttributes.JOB_END_TIME_ATTR, Long.toString(job.getStatistics().getEndTime()));
                    attributes.put(BigQueryAttributes.JOB_START_TIME_ATTR, Long.toString(job.getStatistics().getStartTime()));
                    attributes.put(BigQueryAttributes.JOB_LINK_ATTR, job.getSelfLink());

                    boolean jobError = (job.getStatus().getError() != null);

                    if (jobError) {
                        attributes.put(BigQueryAttributes.JOB_ERROR_MSG_ATTR, job.getStatus().getError().getMessage());
                        attributes.put(BigQueryAttributes.JOB_ERROR_REASON_ATTR, job.getStatus().getError().getReason());
                        attributes.put(BigQueryAttributes.JOB_ERROR_LOCATION_ATTR, job.getStatus().getError().getLocation());
                    } else {
                        // in case it got looped back from error
                        flowFile = session.removeAttribute(flowFile, BigQueryAttributes.JOB_ERROR_MSG_ATTR);
                        flowFile = session.removeAttribute(flowFile, BigQueryAttributes.JOB_ERROR_REASON_ATTR);
                        flowFile = session.removeAttribute(flowFile, BigQueryAttributes.JOB_ERROR_LOCATION_ATTR);

                        // add the number of records successfully added
                        if (job.getStatistics() instanceof JobStatistics.LoadStatistics) {
                            final JobStatistics.LoadStatistics stats = (JobStatistics.LoadStatistics) job.getStatistics();
                            attributes.put(BigQueryAttributes.JOB_NB_RECORDS_ATTR, Long.toString(stats.getOutputRows()));
                        }
                    }

                    if (!attributes.isEmpty()) {
                        flowFile = session.putAllAttributes(flowFile, attributes);
                    }

                    if (jobError) {
                        getLogger().log(LogLevel.WARN, job.getStatus().getError().getMessage());
                        flowFile = session.penalize(flowFile);
                        session.transfer(flowFile, REL_FAILURE);
                    } else {
                        session.getProvenanceReporter().send(flowFile, job.getSelfLink(), job.getStatistics().getEndTime() - job.getStatistics().getStartTime());
                        session.transfer(flowFile, REL_SUCCESS);
                    }
                }
            } catch (InterruptedException e) {
                // Handle interrupted wait
            }

        } catch (Exception ex) {
            getLogger().log(LogLevel.ERROR, ex.getMessage(), ex);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
