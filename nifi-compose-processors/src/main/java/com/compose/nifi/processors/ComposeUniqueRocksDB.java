package com.compose.nifi.processors;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

@EventDriven
@Tags({ "compose", "local", "RocksDB, unique, deduplicate" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Indexes keys in a local RocksDB for gating duplicates by routing to SEEN and UNSEEN. Doesn't store any values only indexes a configured attribute of a FlowFile. This attribute name should hold the logical key for an entity. ")
public class ComposeUniqueRocksDB extends AbstractProcessor {
  private static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
              .name("Directory")
              .description("The directory for the RocksDB files")
              .required(true)
              .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
              .expressionLanguageSupported(false)
              .build();

  private static final PropertyDescriptor LOGICAL_KEY = new PropertyDescriptor.Builder()
              .name("Logical Key")
              .description("The attribute name to be checked for duplicates")
              .required(true)
              .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
              .expressionLanguageSupported(true)
              .build();

  private static final Relationship REL_SEEN = new Relationship.Builder()
              .name("seen")
              .description("FlowFiles that have been seen before based on an attribute that represents a logical key")
              .build();

  private static final Relationship REL_UNSEEN = new Relationship.Builder()
              .name("unseen")
              .description("FlowFiles that have not been seen before based on an attribute that represents a logical key")
              .build();

  private static final Relationship REL_FAILURE = new Relationship.Builder()
              .name("failure")
              .description("FlowFiles that do not have a logical key")
              .build();

  private static final byte[] dummy = new byte[]{1};

  private List<PropertyDescriptor> properties;
  private Set<Relationship> relationships;

  private String logicalKeyName;
  private RocksDB db;
  private Options options;

  @Override
  protected void init(final ProcessorInitializationContext context) {
    final Set<Relationship> procRels = new HashSet<>();
    procRels.add(REL_SEEN);
    procRels.add(REL_UNSEEN);
    procRels.add(REL_FAILURE);
    relationships = Collections.unmodifiableSet(procRels);

    final List<PropertyDescriptor> supDescriptors = new ArrayList<>();
    supDescriptors.add(DIRECTORY);
    supDescriptors.add(LOGICAL_KEY);
    properties = Collections.unmodifiableList(supDescriptors);
  }

  @Override
  public Set<Relationship> getRelationships() {
    return relationships;
  }

  @Override
  public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return properties;
  }

  @OnScheduled
  public final void createDbEnvironment(ProcessContext context) throws IOException, RocksDBException {
    logicalKeyName = context.getProperty(LOGICAL_KEY).getValue();

    RocksDB.loadLibrary();
    options = new Options().setCreateIfMissing(true);
    db = RocksDB.open(options, context.getProperty(DIRECTORY).getValue());
  }

  @OnStopped
  public final void closeDbEnvironment(ProcessContext context) {

    if(db != null) {
      db.close();
    }
    options.dispose();
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) {
    final FlowFile flowFile = session.get();
    if(flowFile == null) {
      session.transfer(flowFile, REL_FAILURE);
      return;
    }

    String logical_key = flowFile.getAttribute(logicalKeyName);
    if(logical_key == null) {
      session.transfer(flowFile, REL_FAILURE);
      return;
    }

    StringBuffer buffer = new StringBuffer();
    try {
      byte[] key = logical_key.getBytes("UTF-8");

      if(!db.keyMayExist(key, buffer)) {
        db.put(key, dummy);
        transferToRelationship(session, flowFile, REL_UNSEEN);
      } else {
        byte[] value = db.get(key);
        if (value[0] == dummy[0]) {
          transferToRelationship(session, flowFile, REL_SEEN);
        } else {
          db.put(key, dummy);
          transferToRelationship(session, flowFile, REL_UNSEEN);
        }
      }
    } catch (RocksDBException e) {
      context.yield();
      session.rollback();
      getLogger().error("Failed to check if {} unique due to {}", new Object[] {logical_key, e});
    } catch (UnsupportedEncodingException ue) {
      context.yield();
      session.rollback();
      getLogger().error("Failed to encode logical key {} unique due to {}", new Object[] {logical_key, ue});
    }
  }

  private void transferToRelationship(ProcessSession session,
                                      FlowFile flowFile,
                                      Relationship relationship) {
    session.getProvenanceReporter().route(flowFile, relationship);
    session.transfer(flowFile, relationship);
  }
}
