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
package org.apache.nifi.processors.nifiredisbatch;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({ "example" })
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class MyProcessor extends AbstractProcessor {

    public static final PropertyDescriptor REDIS_HOST = new PropertyDescriptor.Builder()
            .name("Redis Host")
            .description("The Redis server hostname or IP address")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor REDIS_PORT = new PropertyDescriptor.Builder()
            .name("Redis Port")
            .description("The Redis server port")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .defaultValue("6379")
            .build();

    public static final PropertyDescriptor REDIS_KEY = new PropertyDescriptor.Builder()
            .name("Redis Key")
            .description("The Redis key where data will be inserted")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The number of FlowFiles to process in a batch")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final PropertyDescriptor DATA_PATH = new PropertyDescriptor.Builder()
            .name("Data Path")
            .description("The path of the field in the flow file content to be inserted into Redis")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are successfully processed are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that fail processing are routed to this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(REDIS_HOST);
        descriptors.add(REDIS_PORT);
        descriptors.add(REDIS_KEY);
        descriptors.add(BATCH_SIZE);
        descriptors.add(DATA_PATH);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // Get Redis host, port, key, batch size, and data path from the context
        String redisHost = context.getProperty(REDIS_HOST).getValue();
        int redisPort = context.getProperty(REDIS_PORT).asInteger();
        String redisKey = context.getProperty(REDIS_KEY).getValue();
        int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        String dataPath = context.getProperty(DATA_PATH).getValue();

        // Fetch FlowFiles in batches based on the batch size property
        List<FlowFile> flowFiles = session.get(batchSize);

        try (Jedis jedis = new Jedis(redisHost, redisPort)) {
            for (FlowFile flowFile : flowFiles) {
                // Read flow file content
                try (InputStream inputStream = session.read(flowFile)) {
                    String content = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);

                    // Extract the data using the provided data path
                    String dataToInsert = extractData(content, dataPath);

                    // Insert the extracted data into Redis
                    jedis.lpush(redisKey, dataToInsert);

                    // Transfer FlowFile to success
                    session.transfer(flowFile, REL_SUCCESS);
                } catch (IOException e) {
                    getLogger().error("Failed to read flow file content", e);
                    session.transfer(flowFile, REL_FAILURE);
                }
            }
        } catch (Exception e) {
            getLogger().error("Failed to insert records into Redis", e);
            session.transfer(flowFiles, REL_FAILURE); // Route all to failure on error
        }
    }

    private String extractData(String content, String dataPath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(content);

        // Use JSON Pointer (if the data path is like "/field/subfield")
        JsonNode extractedNode = jsonNode.at(dataPath);

        // If it's a simple field name (not a JSON Pointer), fallback to this:
        if (extractedNode.isMissingNode()) {
            extractedNode = jsonNode.get(dataPath); // Fallback for simple fields
        }

        if (extractedNode != null && !extractedNode.isMissingNode()) {
            return extractedNode.asText(); // Convert to string for insertion into Redis
        } else {
            throw new IOException("Data not found at the given path: " + dataPath);
        }
    }

}
