package org.apache.nifi.processors.gcp.bigquery;

import com.google.cloud.bigquery.EmptyTableResult;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * A processor for batch loading data into a Google BigQuery table
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"google", "google cloud", "bq", "bigquery"})
@CapabilityDescription("Submit a BigQuery SQL, only return status SUCC or FAILED")
public class ExecuteBigQuerySQL extends AbstractGenericBigQueryProcessor {

    private static final PropertyDescriptor SQL_CMD = new PropertyDescriptor
            .Builder().name("BigQuerySQL")
            .displayName("A BigQuery sql")
            .description("The BigQuery SQL query will be submit to server to execute remotely.")
            .required(true)
//            .defaultValue("")
//            .allowableValues()
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .add(SQL_CMD)
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

        final String sqlStr = context.getProperty(SQL_CMD).evaluateAttributeExpressions(flowFile).getValue();

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlStr).build();
        try {
            TableResult result = getCloudService().query(queryConfig);
            final Map<String, String> attributes = new HashMap<>();
            attributes.put(BigQueryAttributes.JOB_NB_RECORDS_ATTR, Long.toString(result.getTotalRows()));
            flowFile = session.putAllAttributes(flowFile, attributes);

            session.getProvenanceReporter().send(flowFile, String.format("Execute sql %s is successful", sqlStr));
            session.transfer(flowFile, REL_SUCCESS);
            getLogger().log(LogLevel.INFO, String.format("Execute sql %s is successful", sqlStr));
        } catch (Exception ex) {
            getLogger().log(LogLevel.ERROR, ex.getMessage(), ex);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
