package com.compose.nifi.processors;

import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
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
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static com.mongodb.client.model.Filters.eq;

/**
 * Created by hayshutton on 8/26/16.
 */
@EventDriven
@TriggerSerially
@Tags({"compose", "mongodb", "put"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Puts Documents and Operations into a MongoDB. Relies on attributes to decide on type of operation such as insert, update, or delete.")
public class ComposeTailingPutMongo extends AbstractProcessor {
  private static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("the unhappy path. check for appropriate attributes among other things.").build();
  private static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("the happy path. probably auto terminate it.").build();

  private static final Set<Relationship> relationships;

  private static final List<PropertyDescriptor> propertyDescriptors;

  static {
    List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
    _propertyDescriptors.addAll(MongoWrapper.descriptors);
    _propertyDescriptors.add(MongoWrapper.WRITE_CONCERN);
    propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

    Set<Relationship> _relationships = new HashSet<>();
    _relationships.add(REL_FAILURE);
    _relationships.add(REL_SUCCESS);
    relationships = Collections.unmodifiableSet(_relationships);
  }

  private MongoWrapper mongoWrapper;

  @Override
  public final Set<Relationship> getRelationships() {
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

  @OnStopped
  public final void closeClient() {
    mongoWrapper.closeClient();
  }

  @Override
  public final void onTrigger(final ProcessContext context, final ProcessSession session) {
    FlowFile flowFile = session.get();
    if(flowFile == null) {
      transferToRelationship(session, flowFile, REL_FAILURE);
      return;
    }

    WriteConcern writeConcern = mongoWrapper.getWriteConcern(context);

    String id = flowFile.getAttribute("mongo.id");
    String operation = flowFile.getAttribute("mongo.op");
    String databaseName = flowFile.getAttribute("mongo.db");
    String collectionName = flowFile.getAttribute("mongo.collection");

    MongoCollection<Document> collection = mongoWrapper.getDatabase(databaseName).getCollection(collectionName).withWriteConcern(writeConcern);

    try {
                  // Read the contents of the FlowFile into a byte array
        final byte[] content = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, content, true);
            }
        });
        Document doc = Document.parse(new String(content));

        switch(operation) {
          case "q":
          case "i":
            collection.insertOne(doc);
            break;
          case "d":
            collection.deleteOne(eq("_id", new ObjectId(id)));
            break;
          case "u":
            doc.remove("_id");
            collection.updateOne(eq("_id", new ObjectId(id)), new Document("$set", doc));
            break;
          case "n":
            break;
          case "c":
            //TODO add $cmd handlers for create and drop collections
            break;
          default:
            throw new BadOperationException("Unhandled operation");
        }

        session.transfer(flowFile, REL_SUCCESS);
    } catch (Exception e) {
        getLogger().error("Failed to insert {} into MongoDB due to {}", new Object[] {flowFile, e}, e);
        session.transfer(flowFile, REL_FAILURE);
        context.yield();
    }
  }

  private void transferToRelationship(ProcessSession session,
                                      FlowFile flowFile,
                                      Relationship relationship) {
    session.getProvenanceReporter().route(flowFile, relationship);
    session.transfer(flowFile, relationship);
  }

}
