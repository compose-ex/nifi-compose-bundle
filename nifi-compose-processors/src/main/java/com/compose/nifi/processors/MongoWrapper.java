package com.compose.nifi.processors;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.components.PropertyDescriptor;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by hayshutton on 8/6/16.
 */
class MongoWrapper {

  static final String WRITE_CONCERN_ACKNOWLEDGED = "ACKNOWLEDGED";
  static final String WRITE_CONCERN_UNACKNOWLEDGED = "UNACKNOWLEDGED";
  static final String WRITE_CONCERN_JOURNALED = "JOURNALED";
  static final String WRITE_CONCERN_MAJORITY = "MAJORITY";
  static final String W1 = "W1";
  static final String W2 = "W2";
  static final String W3 = "W3";

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

  public static final PropertyDescriptor WRITE_CONCERN = new PropertyDescriptor.Builder()
        .name("Write Concern")
        .description("The write concern to use")
        .required(true)
        .allowableValues(WRITE_CONCERN_ACKNOWLEDGED, WRITE_CONCERN_UNACKNOWLEDGED,
                WRITE_CONCERN_JOURNALED, WRITE_CONCERN_MAJORITY,
                W1, W2, W3)
        .defaultValue(WRITE_CONCERN_ACKNOWLEDGED)
        .build();

  private final static String SYSTEM_INDEXES = "system.indexes";
  static final Pattern systemIndexesPattern = Pattern.compile(SYSTEM_INDEXES);


  static List<PropertyDescriptor> descriptors = new ArrayList<>();

  static {
    descriptors.add(URI);
    descriptors.add(DATABASE_NAME);
    descriptors.add(SSL_CONTEXT_SERVICE);
    descriptors.add(CLIENT_AUTH);
  }

  private MongoClient mongoClient;

  private MongoClientOptions.Builder getClientOptions(final SSLContext sslContext) {
    MongoClientOptions.Builder builder = MongoClientOptions.builder();
    builder.sslEnabled(true);
    builder.socketFactory(sslContext.getSocketFactory());
    return builder;
  }

  public MongoDatabase getDatabase(final ProcessContext context) {
    final String databaseName = context.getProperty(DATABASE_NAME).getValue();
    return mongoClient.getDatabase(databaseName);
  }

  public MongoDatabase getDatabase(final String databaseName) {
    return mongoClient.getDatabase(databaseName);
  }

  public MongoDatabase getLocalDatabase() {
    return mongoClient.getDatabase("local");
  }

  public String getURI(final ProcessContext context) {
    return context.getProperty(URI).getValue();
  }

  public void createClient(ProcessContext context) throws IOException {
    if (mongoClient != null) {
      closeClient();
    }

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
      throw e;
    }
  }

  public final void closeClient() {
    if (mongoClient != null) {
      mongoClient.close();
      mongoClient = null;
    }
  }

  protected WriteConcern getWriteConcern(final ProcessContext context) {
    final String writeConcernProperty = context.getProperty(WRITE_CONCERN).getValue();
    WriteConcern writeConcern = null;
    switch (writeConcernProperty) {
      case WRITE_CONCERN_ACKNOWLEDGED:
        writeConcern = WriteConcern.ACKNOWLEDGED;
        break;
      case WRITE_CONCERN_UNACKNOWLEDGED:
        writeConcern = WriteConcern.UNACKNOWLEDGED;
        break;
      case WRITE_CONCERN_JOURNALED:
        writeConcern = WriteConcern.JOURNALED;
        break;
      case WRITE_CONCERN_MAJORITY:
        writeConcern = WriteConcern.MAJORITY;
        break;
      case W1:
        writeConcern = WriteConcern.W1;
        break;
      case W2:
        writeConcern = WriteConcern.W2;
        break;
      case W3:
        writeConcern = WriteConcern.W3;
        break;
      default:
        writeConcern = WriteConcern.ACKNOWLEDGED;
    }
    return writeConcern;
  }

}
