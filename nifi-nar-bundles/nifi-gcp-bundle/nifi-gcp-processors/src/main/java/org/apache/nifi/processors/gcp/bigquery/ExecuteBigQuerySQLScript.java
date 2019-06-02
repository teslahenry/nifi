package org.apache.nifi.processors.gcp.bigquery;

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

import java.util.List;


/**
 * A processor for batch loading data into a Google BigQuery table
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"google", "google cloud", "bq", "bigquery"})
@CapabilityDescription("Submit a BigQuery SQL, only return status SUCC or FAILED")
public class ExecuteBigQuerySQLScript extends AbstractGenericBigQueryProcessor {

    private static final PropertyDescriptor SQL_CMD = new PropertyDescriptor
            .Builder().name("BigQuerySQLScript")
            .displayName("Run BigQuery sql script")
            .description("This processor support execute many sql commands delimited by ;.")
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

        final String sqlScriptStr = context.getProperty(SQL_CMD).evaluateAttributeExpressions(flowFile).getValue();
        String[] sqlStrs = sqlScriptStr.trim().split(";");
        for (String sqlStr : sqlStrs) {
            if (!executeSQL(sqlStr.trim())) {
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
            } else {
                getLogger().log(LogLevel.INFO, String.format("Execute the sql is successful:\n %s", sqlStr));
            }
        }

        session.getProvenanceReporter().send(flowFile, String.format("Execute the sql script is successful:\n %s", sqlScriptStr));
        session.transfer(flowFile, REL_SUCCESS);
        getLogger().log(LogLevel.INFO, String.format("Execute the sql script is successful, detail:\n %s", sqlScriptStr));
    }

    private Boolean executeSQL(String sqlStr) {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlStr).build();
        try {
            TableResult result = getCloudService().query(queryConfig);
            getLogger().log(LogLevel.INFO, String.format("Execute the sql is successful:\n %s \nTotal row result: %d", sqlStr, result.getTotalRows()));
            return true;
        } catch (Exception ex) {
            getLogger().log(LogLevel.ERROR, ex.getMessage(), ex);
            return false;
        }
    }

}
