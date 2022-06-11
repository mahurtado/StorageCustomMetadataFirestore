/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.manolo.content;

import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import functions.eventpojos.GcsEvent;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.WriteResult;
import com.google.cloud.firestore.spi.v1.FirestoreRpc;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

public class InsertFile implements BackgroundFunction<GcsEvent> {
  private static final Logger logger = Logger.getLogger(InsertFile.class.getName());

  public static final String SEPARATOR = "::";
  public static final String GCS_SEPARATOR = "/";
  public static final String CONTENT_RUNTIME_VAR = "COLLECTION";
  public static final String LAST_UPDATED_ATTRIBUTE = "_updated";

  public static final String  EVENT_FINALIZE = "google.storage.object.finalize";
  public static final String  EVENT_DELETE = "google.storage.object.delete";
  public static final String  EVENT_ARCHIVE = "google.storage.object.archive";
  public static final String  EVENT_METADATA_UPDATE = "google.storage.object.metadataUpdate";

  @Override
  public void accept(GcsEvent event, Context context) {

    Blob blob = StorageOptions
    .newBuilder().build()
    .getService()
    .get(event.getBucket(), event.getName(), Storage.BlobGetOption.fields(Storage.BlobField.values()));

    logEvent(event, context, blob) ;
  
    Firestore db = FirestoreOptions.getDefaultInstance().getService();

    switch(context.eventType()){
      case EVENT_FINALIZE:
      case EVENT_METADATA_UPDATE:
        doFinalize(event, blob, db);
        break;
      case EVENT_DELETE:
      case EVENT_ARCHIVE:
        doDelete(event, blob, db);
        break;
      default:
        logger.info("no matching event: " + context.eventType());
    }

  }

  private void logEvent(GcsEvent event, Context context, Blob blob){
    StringBuffer sbEvent = new StringBuffer().
      append("bucket: " + event.getBucket()).
      append("; name: " + event.getName()).
      append("; metageneration: " + event.getMetageneration()).
      append("; timeCreated: " + event.getTimeCreated().toString()).
      append("; updated: " + event.getUpdated().toString());
    logger.info("GcsEvent: " + sbEvent.toString());
    
    StringBuffer sbContext = new StringBuffer().
      append("eventId: " + context.eventId()).
      append("; eventType: " + context.eventType()).
      append("; resource: " + context.resource()).
      append("; timestamp: " + context.timestamp());
    Map<String,String> ctxAttrs = context.attributes();
    for(String keyCtxAttr : ctxAttrs.keySet())
      sbContext.append("Context Attribute " + keyCtxAttr + ": " + ctxAttrs.get(keyCtxAttr));
    logger.info("Context: " + sbContext.toString());

    if(blob != null){
      StringBuffer sbBlob = new StringBuffer().
      append("Id: " + blob.getBlobId()).
      append("; Generation: " + blob.getGeneration()).
      append("; Metageneration: " + blob.getMetageneration()).
      append("; TimeCreated: " + new Date(blob.getCreateTime())).
      append("; Last Metadata Update: " + new Date(blob.getUpdateTime()));  
      logger.info("Blob: " + sbBlob.toString());
    }

  }

  private void doFinalize(GcsEvent event, Blob blob, Firestore db) {
    String docId = event.getBucket() + SEPARATOR + event.getName().replace(GCS_SEPARATOR, SEPARATOR); 

    try (FirestoreRpc rpc = (FirestoreRpc) FirestoreOptions.getDefaultInstance().getRpc();){
      if(isOldEvent(docId, event.getUpdated().getTime(), db))
        return;
      Map<String, Object> data = new HashMap<>();
      data.put(LAST_UPDATED_ATTRIBUTE, event.getUpdated().getTime());
      if(blob.getMetadata() != null){
        for (Map.Entry<String, String> userMetadata : blob.getMetadata().entrySet()) {
          data.put(userMetadata.getKey(), userMetadata.getValue());
        }
      }
      ApiFuture<WriteResult> future = db.collection(System.getenv(CONTENT_RUNTIME_VAR)).document(docId).set(data);
      logger.info("Document processed : " + event.getBucket() + SEPARATOR + event.getName() + "; update time : " + future.get().getUpdateTime());
    } catch (Exception e) {
      e.printStackTrace();
    }    
  }

  private void doDelete(GcsEvent event, Blob blob, Firestore db) {
    try (FirestoreRpc rpc = (FirestoreRpc) FirestoreOptions.getDefaultInstance().getRpc();){
      String docId = event.getBucket() + SEPARATOR + event.getName().replace(GCS_SEPARATOR, SEPARATOR);
      // Check file deleted (blob == null) or updated (blob != null). If updated do nothing
      if(blob == null){
        ApiFuture<WriteResult> future = db.collection(System.getenv(CONTENT_RUNTIME_VAR)).document(docId).delete();
        logger.info("Document deleted : " + event.getBucket() + SEPARATOR + event.getName() + "; update time : " + future.get().getUpdateTime());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }      
  }

  private boolean isOldEvent(String docId, long eventTime, Firestore db) throws InterruptedException, ExecutionException{
    DocumentReference docRef = db.collection(System.getenv(CONTENT_RUNTIME_VAR)).document(docId);
    ApiFuture<DocumentSnapshot> future = docRef.get();

    DocumentSnapshot document = future.get();
    if (!document.exists()) 
      return false;

    Map<String, Object> docMap = document.getData();
    Long lastEventProcessed = (Long) docMap.get(LAST_UPDATED_ATTRIBUTE);
   
    if(lastEventProcessed == null || lastEventProcessed <= eventTime)
      return false;
    else
      return true;
  }

}
