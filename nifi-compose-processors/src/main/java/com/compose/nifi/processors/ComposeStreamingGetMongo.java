package com.compose.nifi.processors;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.ssl.SSLContextService;
import org.bson.Document;
import org.bson.types.ObjectId;

import javax.net.ssl.SSLContext;
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
    @WritesAttribute(attribute="logical.key", description = "The MongoDB object_id for the document in hex format."),
    @WritesAttribute(attribute="mime.type", description = "This is the content type for the content. Always equal to application/json.")})
@CapabilityDescription("Streams documents from collection(s) instead of waiting for query to finish before next step.")
public class ComposeStreamingGetMongo extends AbstractSessionFactoryProcessor {

    private static final PropertyDescriptor URI = new PropertyDescriptor.Builder()
            .name("Mongo URI")
            .description("MongoURI, typically of the form: mongodb://host1[:port1][,host2[:port2],...]")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    private static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("Mongo Database Name")
            .description("The name of the database to use")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    private static final PropertyDescriptor COLLECTION_REGEX = new PropertyDescriptor.Builder()
            .name("Mongo Collection Regex")
            .description("The regex to match collections. Uses java.util regexes. The default of '.*' matches all collections")
            .required(true)
            .defaultValue(".*")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    private static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();
    private static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("ssl-client-auth")
            .displayName("Client Auth")
            .description("Client authentication policy when connecting to secure (TLS/SSL) cluster. "
                    + "Possible values are REQUIRED, WANT, NONE. This property is only used when an SSL Context "
                    + "has been defined and enabled.")
            .required(false)
            .allowableValues(SSLContextService.ClientAuth.values())
            .defaultValue("REQUIRED")
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All good documents go this way").build();

    private final static String SYSTEM_INDEXES = "system.indexes";

    private final static Set<Relationship> relationships;

    private final static List<PropertyDescriptor> propertyDescriptors;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(URI);
        _propertyDescriptors.add(DATABASE_NAME);
        _propertyDescriptors.add(COLLECTION_REGEX);
        _propertyDescriptors.add(SSL_CONTEXT_SERVICE);
        _propertyDescriptors.add(CLIENT_AUTH);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    private final static Pattern systemIndexesPattern = Pattern.compile(SYSTEM_INDEXES);

    private Pattern userCollectionNamePattern;

    private MongoClient mongoClient;

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
        if (mongoClient != null) {
            closeClient();
        }

        getLogger().info("Creating MongoClient");

        // Set up the client for secure (SSL/TLS communications) if configured to do so
        final SSLContextService sslService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final String rawClientAuth = context.getProperty(CLIENT_AUTH).getValue();
        final SSLContext sslContext;

        if (sslService != null) {
            final SSLContextService.ClientAuth clientAuth;
            if (StringUtils.isBlank(rawClientAuth)) {
                clientAuth = SSLContextService.ClientAuth.REQUIRED;
            } else {
                try {
                    clientAuth = SSLContextService.ClientAuth.valueOf(rawClientAuth);
                } catch (final IllegalArgumentException iae) {
                    throw new AuthorizerCreationException(String.format("Unrecognized client auth '%s'. Possible values are [%s]",
                            rawClientAuth, StringUtils.join(SslContextFactory.ClientAuth.values(), ", ")));
                }
            }
            sslContext = sslService.createSSLContext(clientAuth);
        } else {
            sslContext = null;
        }

        try {
            final String uri = context.getProperty(URI).getValue();
            if(sslContext == null) {
                mongoClient = new MongoClient(new MongoClientURI(uri));
            } else {
                mongoClient = new MongoClient(new MongoClientURI(uri, getClientOptions(sslContext)));
            }
        } catch (Exception e) {
            getLogger().error("Failed to schedule PutMongo due to {}", new Object[] { e }, e);
            throw e;
        }
    }

    private MongoClientOptions.Builder getClientOptions(final SSLContext sslContext) {
        MongoClientOptions.Builder builder = MongoClientOptions.builder();
        builder.sslEnabled(true);
        builder.socketFactory(sslContext.getSocketFactory());
        return builder;
    }

    @OnStopped
    public final void closeClient() {
        if (mongoClient != null) {
            getLogger().info("Closing MongoClient");
            mongoClient.close();
            mongoClient = null;
        }
    }

    private MongoDatabase getDatabase(final ProcessContext context) {
        final String databaseName = context.getProperty(DATABASE_NAME).getValue();
        return mongoClient.getDatabase(databaseName);
    }

    private ArrayList<String> getUserCollectionNames(final ProcessContext context) {
      ArrayList<String> userCollectionNames = new ArrayList<>();
      MongoIterable<String> names = getDatabase(context).listCollectionNames();
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
        if(systemIndexesPattern.matcher(collectionName).matches()) {
          continue;
        }

        final MongoCollection<Document> collection = getDatabase(context).getCollection(collectionName);

        try {
            final FindIterable<Document> it = collection.find();
            final MongoCursor<Document> cursor = it.iterator();
            final String dbName = getDatabase(context).getName();

            FlowFile flowFile = null;
            try {
                while (cursor.hasNext()) {
                    ProcessSession session = sessionFactory.createSession();

                    flowFile = session.create();

                    final Document currentDoc = cursor.next();
                    ObjectId currentObjectId = currentDoc.getObjectId("_id");

                    flowFile = session.putAttribute(flowFile, "logical.key", currentObjectId.toHexString());
                    flowFile = session.putAttribute(flowFile, "mime.type", "application/json");
                    flowFile = session.putAttribute(flowFile, "db", dbName);
                    flowFile = session.putAttribute(flowFile, "collection", collectionName);

                    flowFile = session.write(flowFile, new OutputStreamCallback() {
                                @Override
                                public void process(OutputStream out) throws IOException {
                                    IOUtils.write(currentDoc.toJson(), out);
                                }
                            });

                    session.getProvenanceReporter().receive(flowFile, context.getProperty(URI).getValue());
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
