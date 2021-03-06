package org.apache.nifi.processors.gcp.bigquery;

import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
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

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"google", "google cloud", "bq", "bigquery"})
@CapabilityDescription("Load data from GCS, local into BigQuery table.")
public class LoadBigQueryTable extends AbstractBigQueryProcessor {

    private static final List<String> TYPES = Arrays.asList(FormatOptions.json().getType(), FormatOptions.csv().getType(), FormatOptions.avro().getType());

    private static final Validator FORMAT_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            final ValidationResult.Builder builder = new ValidationResult.Builder();
            builder.subject(subject).input(input);
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return builder.valid(true).explanation("Contains Expression Language").build();
            }

            if(TYPES.contains(input.toUpperCase())) {
                builder.valid(true);
            } else {
                builder.valid(false).explanation("Load File Type must be one of the following options: " + StringUtils.join(TYPES, ", "));
            }

            return builder.build();
        }
    };

    private static final PropertyDescriptor SOURCE_URIS = new PropertyDescriptor.Builder()
            .name("SourceUris")
            .displayName("Source Uris")
            .description("The sourceUris property indicates the location(s) and file name(s) where BigQuery will read your files. Ref: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor SOURCE_TYPE = new PropertyDescriptor
            .Builder().name(BigQueryAttributes.SOURCE_TYPE_ATTR)
            .displayName("Load file type")
            .description(BigQueryAttributes.SOURCE_TYPE_DESC)
            .required(true)
            .allowableValues("CSV", "AVRO", "NEWLINE_DELIMITED_JSON")
            .defaultValue("AVRO")
            .addValidator(FORMAT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor AUTO_DETECT_SCHEMA = new PropertyDescriptor.Builder()
            .name("AutoDetectSchema")
            .displayName("Auto Detect Schema")
            .description("The BigQuery will be check random about 100 record and try to detect type of data. It's only applying for JSON/CSV")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor COMPLETION_STRATEGY = new PropertyDescriptor.Builder()
            .name("CompletionStrategy")
            .displayName("Completion Strategy")
            .description("Specifies what to do with the original file on the file system once it has been pulled into NiFi")
            .required(true)
            .allowableValues("None", "Move File", "Delete File")
            .defaultValue("None")
            .addValidator(FORMAT_VALIDATOR)
            .build();

    public static final PropertyDescriptor MOVE_DESTINATION_URI = new PropertyDescriptor.Builder()
            .name("MoveDestinationDirectory")
            .displayName("Move Destination Directory")
            .description("The directory to the move the original file to once it has been fetched from the file system. This property is ignored unless the Completion Strategy is set to \"Move File\". If the directory does not exist, it will be created.\n" +
                    "Supports Expression Language: true (will be evaluated using flow file attributes and variable registry)")
            .required(false)
            .defaultValue("${config.movedestdir}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor IGNORE_UNKNOWN = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.IGNORE_UNKNOWN_ATTR)
            .displayName("Ignore Unknown Values")
            .description(BigQueryAttributes.IGNORE_UNKNOWN_DESC)
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
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

    public static final PropertyDescriptor CSV_ALLOW_JAGGED_ROWS = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.CSV_ALLOW_JAGGED_ROWS_ATTR)
            .displayName("CSV Input - Allow Jagged Rows")
            .description(BigQueryAttributes.CSV_ALLOW_JAGGED_ROWS_DESC)
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor CSV_ALLOW_QUOTED_NEW_LINES = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.CSV_ALLOW_QUOTED_NEW_LINES_ATTR)
            .displayName("CSV Input - Allow Quoted New Lines")
            .description(BigQueryAttributes.CSV_ALLOW_QUOTED_NEW_LINES_DESC)
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor CSV_CHARSET = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.CSV_CHARSET_ATTR)
            .displayName("CSV Input - Character Set")
            .description(BigQueryAttributes.CSV_CHARSET_DESC)
            .required(true)
            .allowableValues("UTF-8", "ISO-8859-1")
            .defaultValue("UTF-8")
            .build();

    public static final PropertyDescriptor CSV_FIELD_DELIMITER = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.CSV_FIELD_DELIMITER_ATTR)
            .displayName("CSV Input - Field Delimiter")
            .description(BigQueryAttributes.CSV_FIELD_DELIMITER_DESC)
            .required(true)
            .defaultValue(",")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor CSV_QUOTE = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.CSV_QUOTE_ATTR)
            .displayName("CSV Input - Quote")
            .description(BigQueryAttributes.CSV_QUOTE_DESC)
            .required(true)
            .defaultValue("\"")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor CSV_SKIP_LEADING_ROWS = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.CSV_SKIP_LEADING_ROWS_ATTR)
            .displayName("CSV Input - Skip Leading Rows")
            .description(BigQueryAttributes.CSV_SKIP_LEADING_ROWS_DESC)
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .add(SOURCE_URIS)
                .add(SOURCE_TYPE)
                .add(AUTO_DETECT_SCHEMA)
                .add(COMPLETION_STRATEGY)
                .add(MOVE_DESTINATION_URI)
                .add(CREATE_DISPOSITION)
                .add(WRITE_DISPOSITION)
                .add(MAXBAD_RECORDS)
                .add(IGNORE_UNKNOWN)
                .add(CSV_ALLOW_JAGGED_ROWS)
                .add(CSV_ALLOW_QUOTED_NEW_LINES)
                .add(CSV_CHARSET)
                .add(CSV_FIELD_DELIMITER)
                .add(CSV_QUOTE)
                .add(CSV_SKIP_LEADING_ROWS)
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
        final String sourceUris = context.getProperty(SOURCE_URIS).evaluateAttributeExpressions(flowFile).getValue().trim();
        final String type = context.getProperty(SOURCE_TYPE).evaluateAttributeExpressions(flowFile).getValue();
        final Boolean autoDetectSchema = context.getProperty(AUTO_DETECT_SCHEMA).evaluateAttributeExpressions().asBoolean();
        final String completionStrategy = context.getProperty(COMPLETION_STRATEGY).getValue();
        final String moveDestinationUri = context.getProperty(MOVE_DESTINATION_URI).evaluateAttributeExpressions(flowFile).getValue();

        final TableId tableId;
        if (StringUtils.isEmpty(projectId)) {
            tableId = TableId.of(dataset, tableName);
        } else {
            tableId = TableId.of(projectId, dataset, tableName);
        }

        try {
            List<String> listSourceUri = new ArrayList<>();
            // NOTE:
            // You can use only one wildcard for objects (filenames) within your bucket.
            // The wildcard can appear inside the object name or at the end of the object name.
            // Appending a wildcard to the bucket name is unsupported.
            if (sourceUris.startsWith("[") && sourceUris.endsWith("]")) {
                Gson gson = new GsonBuilder().create();
                listSourceUri = gson.fromJson(sourceUris, ArrayList.class);
            } else {
                listSourceUri.add(sourceUris);
            }

            // set type option
            FormatOptions formatOption;

            if(type.equals(FormatOptions.csv().getType())) {
                formatOption = FormatOptions.csv().toBuilder()
                        .setAllowJaggedRows(context.getProperty(CSV_ALLOW_JAGGED_ROWS).asBoolean())
                        .setAllowQuotedNewLines(context.getProperty(CSV_ALLOW_QUOTED_NEW_LINES).asBoolean())
                        .setEncoding(context.getProperty(CSV_CHARSET).getValue())
                        .setFieldDelimiter(context.getProperty(CSV_FIELD_DELIMITER).evaluateAttributeExpressions(flowFile).getValue())
                        .setQuote(context.getProperty(CSV_QUOTE).evaluateAttributeExpressions(flowFile).getValue())
                        .setSkipLeadingRows(context.getProperty(CSV_SKIP_LEADING_ROWS).evaluateAttributeExpressions(flowFile).asInteger())
                        .build();
            } else {
                formatOption = FormatOptions.of(type);
            }

            // build schema
            final Schema schema = BigQueryUtils.schemaFromString(context.getProperty(TABLE_SCHEMA).evaluateAttributeExpressions(flowFile).getValue());

            LoadJobConfiguration.Builder confBuidler = LoadJobConfiguration
                    .newBuilder(tableId, listSourceUri)
                    .setCreateDisposition(JobInfo.CreateDisposition.valueOf(context.getProperty(CREATE_DISPOSITION).getValue()))
                    .setWriteDisposition(JobInfo.WriteDisposition.valueOf(context.getProperty(WRITE_DISPOSITION).getValue()))
                    .setIgnoreUnknownValues(context.getProperty(IGNORE_UNKNOWN).asBoolean())
                    .setMaxBadRecords(context.getProperty(MAXBAD_RECORDS).asInteger())
                    .setFormatOptions(formatOption)
                    .setAutodetect(autoDetectSchema);
            if (schema != null) {
                confBuidler.setSchema(schema);
            }
            LoadJobConfiguration configuration = confBuidler.build();

            Job job = getCloudService().create(JobInfo.of(configuration));

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
                            attributes.put("bq.records.badrecords", Long.toString(stats.getBadRecords()));
                            attributes.put("bq.records.inputfiles", Long.toString(stats.getInputFiles()));
                            attributes.put("bq.records.outputbytes", Long.toString(stats.getOutputBytes()));
                            attributes.put("bq.records.inoutbytes", Long.toString(stats.getInputBytes()));
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
            } catch (InterruptedException ex) {
                // Handle interrupted wait
                getLogger().log(LogLevel.ERROR, ex.getMessage(), ex);
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
            }

        } catch (Exception ex) {
            getLogger().log(LogLevel.ERROR, ex.getMessage(), ex);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
