package org.apache.nifi.processors.gcp.bigquery;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableList;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * A processor for batch loading data into a Google BigQuery table
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"google", "google cloud", "bq", "bigquery"})
@CapabilityDescription("Submit a BigQuery SQL, only return status SUCC or FAILED")
public class CopyBigQueryTable extends AbstractGenericBigQueryProcessor {

    private static final PropertyDescriptor SOURCE_PROJECT = new PropertyDescriptor
            .Builder().name("Source Project")
            .displayName("Source Project")
            .description("The source project that contains the source dataset.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private static final PropertyDescriptor SOURCE_DATASET = new PropertyDescriptor
            .Builder().name("SourceDataset")
            .displayName("Source Dataset")
            .description("The source dataset that contains the table will be copied.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private static final PropertyDescriptor SOURCE_TABLE = new PropertyDescriptor
            .Builder().name("SourceTable")
            .displayName("Source Table")
            .description("The source table that will be copied.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private static final PropertyDescriptor DESTINATION_PROJECT = new PropertyDescriptor
            .Builder().name("Destination Project")
            .displayName("Destination Project")
            .description("The destination project that contains the destination dataset.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private static final PropertyDescriptor DESTINATION_DATASET = new PropertyDescriptor
            .Builder().name("Destination Dataset")
            .displayName("Destination Dataset")
            .description("The destination dataset that contains the destination table. It should be the same location with source dataset.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    private static final PropertyDescriptor DESTINATION_TABLE = new PropertyDescriptor
            .Builder().name("Destination Table")
            .displayName("Destination Table")
            .description("The destination table that will contain copied data.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.JOB_READ_TIMEOUT_ATTR)
            .displayName("Read Timeout")
            .description(BigQueryAttributes.JOB_READ_TIMEOUT_DESC)
            .required(true)
            .defaultValue("5 minutes")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor CREATE_DISPOSITION = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.CREATE_DISPOSITION_ATTR)
            .displayName("Create Disposition")
            .description(BigQueryAttributes.CREATE_DISPOSITION_DESC)
            .required(true)
            .allowableValues(BigQueryAttributes.CREATE_IF_NEEDED, BigQueryAttributes.CREATE_NEVER)
            .defaultValue(BigQueryAttributes.CREATE_IF_NEEDED.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor WRITE_DISPOSITION = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.WRITE_DISPOSITION_ATTR)
            .displayName("Write Disposition")
            .description(BigQueryAttributes.WRITE_DISPOSITION_DESC)
            .required(true)
            .allowableValues(BigQueryAttributes.WRITE_EMPTY, BigQueryAttributes.WRITE_APPEND, BigQueryAttributes.WRITE_TRUNCATE)
            .defaultValue(BigQueryAttributes.WRITE_EMPTY.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAXBAD_RECORDS = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.MAX_BADRECORDS_ATTR)
            .displayName("Max Bad Records")
            .description(BigQueryAttributes.MAX_BADRECORDS_DESC)
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();


    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .add(SOURCE_PROJECT)
                .add(SOURCE_DATASET)
                .add(SOURCE_TABLE)
                .add(DESTINATION_PROJECT)
                .add(DESTINATION_DATASET)
                .add(DESTINATION_TABLE)
                .add(READ_TIMEOUT)
                .add(CREATE_DISPOSITION)
                .add(WRITE_DISPOSITION)
                .add(MAXBAD_RECORDS)
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
        final String sourceProject = context.getProperty(SOURCE_PROJECT).evaluateAttributeExpressions(flowFile).getValue();
        final String sourceDataset = context.getProperty(SOURCE_DATASET).evaluateAttributeExpressions(flowFile).getValue();
        final String sourceTable = context.getProperty(SOURCE_TABLE).evaluateAttributeExpressions(flowFile).getValue();
        final String destinationProject = context.getProperty(DESTINATION_PROJECT).evaluateAttributeExpressions(flowFile).getValue();
        final String destinationDataset = context.getProperty(DESTINATION_DATASET).evaluateAttributeExpressions(flowFile).getValue();
        final String destinationTable = context.getProperty(DESTINATION_TABLE).evaluateAttributeExpressions(flowFile).getValue();


        final TableId srcTableId = TableId.of(sourceProject, sourceDataset, sourceTable);
        final TableId destTableId = TableId.of(destinationProject, destinationDataset, destinationTable);

        try {

            CopyJobConfiguration configuration = CopyJobConfiguration
                    .newBuilder(destTableId, srcTableId)
                    .setCreateDisposition(JobInfo.CreateDisposition.valueOf(context.getProperty(CREATE_DISPOSITION).getValue()))
                    .setWriteDisposition(JobInfo.WriteDisposition.valueOf(context.getProperty(WRITE_DISPOSITION).getValue()))
                    .build();
            Job job = getCloudService().create(JobInfo.of(configuration));
//            Job job = getCloudService().getTable(srcTableId).copy(destTableId);

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
