/*
 * Licensed to the Apache Software Foundation (ASF) uner one or more
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
package com.compose.nifi.processors;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.bson.Document;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;

@EventDriven
@Tags({ "mongodb", "batch insert", "batch update", "write", "put" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Writes the json array contents of a FlowFile to MongoDB")
public class ComposeBatchPutMongo extends AbstractProcessor {
    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("All FlowFiles that are written to MongoDB are routed to this relationship").build();
    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("All FlowFiles that cannot be written to MongoDB are routed to this relationship").build();

    private static final PropertyDescriptor COLLECTION_NAME = new PropertyDescriptor.Builder()
            .name("MongoWrapper Collection Regex")
            .description("The regex to match collections. Uses java.util regexes. The default of '.*' matches all collections")
            .required(true)
            .defaultValue(".*")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    private static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
        .name("Character Set")
        .description("The Character Set in which the data is encoded")
        .required(true)
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .defaultValue("UTF-8")
        .build();

    private static final Set<Relationship> relationships;
    private static final List<PropertyDescriptor> propertyDescriptors;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(MongoWrapper.descriptors);
        _propertyDescriptors.add(COLLECTION_NAME);
        _propertyDescriptors.add(CHARACTER_SET);
        _propertyDescriptors.add(MongoWrapper.WRITE_CONCERN);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    private MongoWrapper mongoWrapper;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @OnScheduled
    public final void createClient(ProcessContext context) throws IOException {
        mongoWrapper = new MongoWrapper();
        mongoWrapper.createClient(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final String collectionName = context.getProperty(COLLECTION_NAME).getValue();
        final WriteConcern writeConcern = mongoWrapper.getWriteConcern(context);

        final MongoCollection<Document> collection = mongoWrapper.getDatabase(context).getCollection(collectionName).withWriteConcern(writeConcern);

        try {
            // Read the contents of the FlowFile into a byte array
            final byte[] content = new byte[(int) flowFile.getSize()];
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, content, true);
                }
            });

            //Hack away :)
            ArrayList<Document> docs = new ArrayList<>();
            JsonParser parser = new JsonParser();
            JsonArray array = parser.parse(new String(content, charset)).getAsJsonArray();
            for(JsonElement element: array) {
              //Terrible. surely a better way. learn more.
              Document doc = Document.parse(element.toString());
              docs.add(doc);
            }

            collection.insertMany(docs);

            session.getProvenanceReporter().send(flowFile, mongoWrapper.getURI(context));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error("Failed to insert {} into MongoDB due to {}", new Object[] {flowFile, e}, e);
            session.transfer(flowFile, REL_FAILURE);
            context.yield();
        }
    }

    @OnStopped
    public final void closeClient() {
        mongoWrapper.closeClient();
    }

}
