/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.gcp.storage;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.*;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.nifi.processors.gcp.storage.StorageAttributes.*;


@SupportsBatching
@Tags({"google cloud", "gcs", "google", "storage", "delete"})
@CapabilityDescription("Moving objects in the Google Cloud Bucket. " +
        "If attempting to move files that does not exist, FlowFile is routed to success.")
@SeeAlso({PutGCSObject.class, FetchGCSObject.class, ListGCSBucket.class})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class MoveGCSObject extends AbstractGCSProcessor {
    public static final PropertyDescriptor SOURCE_BUCKET = new PropertyDescriptor
            .Builder().name("gcs-source-bucket")
            .displayName("SourceBucket")
            .description(BUCKET_DESC)
            .required(true)
            .defaultValue("${gcs.source.bucket}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SOURCE_BLOB_URIS = new PropertyDescriptor
            .Builder().name("gcs-blob-name")
            .displayName("SourceBlodUris")
            .description(KEY_DESC)
            .required(true)
            .defaultValue("${gcs.source.uris}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USE_GENERATIONS = new PropertyDescriptor.Builder()
            .name("gcs-use-generations")
            .displayName("Use Generations")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
            .description("Specifies whether to use GCS Generations, if applicable.  If false, only the latest version of each object will be returned.")
            .build();

    public static final PropertyDescriptor DESTINATION_BUCKET = new PropertyDescriptor
            .Builder().name("gcs-dest-bucket")
            .displayName("DestinationBucket")
            .description(BUCKET_DESC)
            .required(true)
            .defaultValue("${gcs.dest.bucket}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DEST_BLOB_DIRECTORY = new PropertyDescriptor
            .Builder().name("gcs-dest-blob-directory")
            .displayName("DestinationBlobDirectory")
            .description(KEY_DESC)
            .required(true)
            .defaultValue("${gcs.dest.directory}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .add(SOURCE_BUCKET)
                .add(SOURCE_BLOB_URIS)
                .add(USE_GENERATIONS)
                .add(DESTINATION_BUCKET)
                .add(DEST_BLOB_DIRECTORY)
                .build();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();

        final String sourceBucket = context.getProperty(SOURCE_BUCKET).evaluateAttributeExpressions(flowFile).getValue();
        final String sourceBlobUris = context.getProperty(SOURCE_BLOB_URIS).evaluateAttributeExpressions(flowFile).getValue();
        final String destBucket = context.getProperty(DESTINATION_BUCKET).evaluateAttributeExpressions(flowFile).getValue();
        final String destBlobDir = context.getProperty(DEST_BLOB_DIRECTORY).evaluateAttributeExpressions(flowFile).getValue();
        final boolean useGenerations = context.getProperty(USE_GENERATIONS).asBoolean();

        final Storage storage = getCloudService();

        try {
            // List all blobs
            List<String> listSourceUri = new ArrayList<>();
            if (sourceBlobUris.startsWith("[") && sourceBlobUris.endsWith("]")) {
                Gson gson = new GsonBuilder().create();
                listSourceUri = gson.fromJson(sourceBlobUris, ArrayList.class);
            } else {
                listSourceUri.add(sourceBlobUris);
            }

            if (!moveBlobObject(storage, sourceBucket, listSourceUri, destBucket, destBlobDir, useGenerations)) {
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (Exception e) {
            getLogger().error(e.getMessage(), e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        getLogger().info("Successfully moved GCS Object for {} in {} millis; routing to success", new Object[]{flowFile, millis});
    }

    private String getBucketName(String uri) {
        Matcher m = Pattern.compile("gs://(.*)/").matcher(uri);
        if (m.find()) {
            return m.group(1);
        } else {
            return null;
        }
    }

    /**
     * @param storage
     * @param sourceBucket  exp: test-bucket
     * @param listSourceUri exp: data/test-file*.csv or [data/test-file-1.csv, data/test-file-2.csv]
     * @param destBucket    exp: test-bucket-2
     * @param destDirectory exp: data/
     * @return --> all file "test-file*.csv" will be move to gs://test-bucket-2/data/test-file*.csv
     */
    private Boolean moveBlobObject(Storage storage, String sourceBucket, List<String> listSourceUri, String destBucket, String destDirectory, boolean useGenerations) {
        if (listSourceUri.size() > 1) {
            StorageBatch batch = storage.batch();
            for (String sourceBlod : listSourceUri) {
                String[] tmp = sourceBlod.split("/");
                String blobName = tmp[tmp.length - 1];
                moveSingleBlobObject(storage, sourceBucket, sourceBlod, destBucket, destDirectory + blobName);
            }
        } else {
            List<Storage.BlobListOption> listOptions = new ArrayList<>();
            if (destDirectory != null) {
                listOptions.add(Storage.BlobListOption.prefix(destDirectory));
            }

            if (useGenerations) {
                listOptions.add(Storage.BlobListOption.versions(true));
            }

            Page<Blob> blobPages = storage.list(sourceBucket, listOptions.toArray(new Storage.BlobListOption[listOptions.size()]));
            do {
                for (Blob blob : blobPages.getValues()) {
                    moveSingleBlobObject(blob, destBucket, destDirectory + blob.getName());
                }

                // get next-page
                blobPages = blobPages.getNextPage();
            } while (blobPages != null);

        }
        return true;
    }

    private Blob moveSingleBlobObject(Storage storage, String sourceBucket, String sourceBlob, String destBucket, String destBlob) {
        Blob blob = storage.get(BlobId.of(sourceBucket, sourceBlob));
        // [START storageMoveFile]
        CopyWriter copyWriter = blob.copyTo(destBucket, destBlob);
        Blob copiedBlob = copyWriter.getResult();
        boolean deleted = blob.delete();
        // [END storageMoveFile]
        if (deleted) {
            return copiedBlob;
        } else {
            return null;
        }
    }

    private Blob moveSingleBlobObject(Blob blob, String destBucket, String destBlob) {
        // [START storageMoveFile]
        CopyWriter copyWriter = blob.copyTo(destBucket, destBlob);
        Blob copiedBlob = copyWriter.getResult();
        boolean deleted = blob.delete();
        // [END storageMoveFile]
        if (deleted) {
            return copiedBlob;
        } else {
            return null;
        }
    }
}
