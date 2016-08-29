package com.compose.nifi.processors;

import com.mongodb.client.*;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by hayshutton on 7/22/16.
 *
 * Started with the default GetMongo from the NiFi source nar bundle
 */
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"compose", "mongodb", "streaming query"})
@WritesAttributes({
    @WritesAttribute(attribute="mime.type", description = "This is the content type for the content. Always equal to application/json."),
    @WritesAttribute(attribute="mongo.id", description = "The MongoDB object_id for the document in hex format."),
    @WritesAttribute(attribute="mongo.op", description = "The MongoDB operation to match other Processors. `q` for query is used. It is made up"),
    @WritesAttribute(attribute="mongo.db", description = "The MongoDB db."),
    @WritesAttribute(attribute="mongo.collection", description = "The MongoDB collection")
})
@CapabilityDescription("Streams documents from collection(s) instead of waiting for query to finish before next step.")
public class ComposeStreamingGetMongo extends AbstractSessionFactoryProcessor {



    private static final PropertyDescriptor COLLECTION_REGEX = new PropertyDescriptor.Builder()
            .name("Mongo Collection Regex")
            .description("The regex to match collections. Uses java.util regexes. The default of '.*' matches all collections")
            .required(true)
            .defaultValue(".*")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    private final static Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All good documents go this way").build();

  private final static Set<Relationship> relationships;

    private final static List<PropertyDescriptor> propertyDescriptors;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(MongoWrapper.descriptors);
        _propertyDescriptors.add(COLLECTION_REGEX);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(_relationships);
    }

  private Pattern userCollectionNamePattern;

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
    public final void initPattern(ProcessContext context) {
        userCollectionNamePattern = Pattern.compile(context.getProperty(COLLECTION_REGEX).getValue());
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

    private ArrayList<String> getUserCollectionNames(final ProcessContext context) {
      ArrayList<String> userCollectionNames = new ArrayList<>();
      MongoIterable<String> names = mongoWrapper.getDatabase(context).listCollectionNames();
      for(String name: names) {
        if(userCollectionNamePattern.matcher(name).matches()) {
          userCollectionNames.add(name);
          getLogger().debug("Adding collectionName: {} due to match of {}", new Object[] {name, context.getProperty(COLLECTION_REGEX).getValue()});
        }
      }
      return userCollectionNames;
    }

    @Override
    public final void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
      for(String collectionName: getUserCollectionNames(context)) {
        if(MongoWrapper.systemIndexesPattern.matcher(collectionName).matches()) {
          continue;
        }

        final MongoCollection<Document> collection = mongoWrapper.getDatabase(context).getCollection(collectionName);

        try {
            final FindIterable<Document> it = collection.find();
            final MongoCursor<Document> cursor = it.iterator();
            final String dbName = mongoWrapper.getDatabase(context).getName();

            try {
                while (cursor.hasNext()) {
                    ProcessSession session = sessionFactory.createSession();

                    FlowFile flowFile = session.create();

                    final Document currentDoc = cursor.next();
                    ObjectId currentObjectId = currentDoc.getObjectId("_id");

                    flowFile = session.putAttribute(flowFile, "mime.type", "application/json");
                    flowFile = session.putAttribute(flowFile, "mongo.id", currentObjectId.toHexString());
                    flowFile = session.putAttribute(flowFile, "mongo.op", "q");
                    flowFile = session.putAttribute(flowFile, "mongo.db", dbName);
                    flowFile = session.putAttribute(flowFile, "mongo.collection", collectionName);

                    flowFile = session.write(flowFile, new OutputStreamCallback() {
                                @Override
                                public void process(OutputStream out) throws IOException {
                                    IOUtils.write(currentDoc.toJson(), out);
                                }
                            });

                    session.getProvenanceReporter().receive(flowFile, mongoWrapper.getURI(context));
                    session.transfer(flowFile, REL_SUCCESS);
                    session.commit();
                }
            } finally {
                cursor.close();
            }

        } catch (final Throwable t) {
            getLogger().error("{} failed to process due to {}; rolling back session", new Object[]{this, t});
            throw t;
        }
      }

    }
}
