/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
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
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.WriteResult;
import com.google.cloud.firestore.spi.v1.FirestoreRpc;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class ProcessFile implements BackgroundFunction<GcsEvent> {
  private static final Logger logger = Logger.getLogger(ProcessFile.class.getName());

  public static final String SEPARATOR = "::";
  public static final String GCS_SEPARATOR = "/";
  public static final String CONTENT_RUNTIME_VAR = "COLLECTION";

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
        logger.severe("Unknown event: " + context.eventType());
    }

  }

  private void doFinalize(GcsEvent event, Blob blob, Firestore db) {
    try (FirestoreRpc rpc = (FirestoreRpc) FirestoreOptions.getDefaultInstance().getRpc();){
      String docId = getDocId(event);
      Map<String, Object> data = new HashMap<>();
      for (Map.Entry<String, String> userMetadata : blob.getMetadata().entrySet()) {
        data.put(userMetadata.getKey(), userMetadata.getValue());
      }
      ApiFuture<WriteResult> future = db.collection(System.getenv(CONTENT_RUNTIME_VAR)).document(docId).set(data);
      logger.info("Document processed : " + event.getBucket() + SEPARATOR + event.getName() + "; update time : " + future.get().getUpdateTime());
    } 
    catch (Exception e) {
      e.printStackTrace();
    }    
  }

  private void doDelete(GcsEvent event, Blob blob,Firestore db) {
    try (FirestoreRpc rpc = (FirestoreRpc) FirestoreOptions.getDefaultInstance().getRpc();){
      String docId = getDocId(event);
      // Check file deleted (blob == null) or updated (blob != null). If updated do nothing
      if(blob == null){
        ApiFuture<WriteResult> future = db.collection(System.getenv(CONTENT_RUNTIME_VAR)).document(docId).delete();
        logger.info("Document deleted : " + event.getBucket() + SEPARATOR + event.getName() + "; update time : " + future.get().getUpdateTime());
      }
    } 
    catch (Exception e) {
      e.printStackTrace();
    }      
  }

  private String getDocId(GcsEvent event){
    // Firestore don't support keys with '/'. Replacing with '::'
    return event.getBucket() + SEPARATOR + event.getName().replace(GCS_SEPARATOR, SEPARATOR);
  }

}
