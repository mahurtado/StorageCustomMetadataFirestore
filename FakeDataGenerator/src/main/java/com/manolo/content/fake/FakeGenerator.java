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

package com.manolo.content.fake;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class FakeGenerator {

    public static final String INPUT_CSV = "inputCsv";
    public static final String PROJECT_ID = "projectId";
    public static final String BUCKET_NAME  = "bucketName";
    public static final String SAMPLE_FILE  = "sampleFile";
    public static final String OBJECT_NAME_HEADER = "objectNameHeaderIndex";

    public static void main(String[] args){
        try{
            String csvFile = System.getProperty(INPUT_CSV);
            String projectId = System.getProperty(PROJECT_ID);
            String bucketName = System.getProperty(BUCKET_NAME);
            String sampleFile = System.getProperty(SAMPLE_FILE);
            int objectNameHeaderIndex = Integer.parseInt(System.getProperty(OBJECT_NAME_HEADER, "-1"));

            if((csvFile == null || csvFile.isEmpty()) || 
                (projectId == null || projectId.isEmpty()) || 
                (bucketName == null || bucketName.isEmpty())|| 
                (sampleFile == null || sampleFile.isEmpty())){
                usage();
                System.exit(0);
            }

            String fileExtension = sampleFile.substring(sampleFile.lastIndexOf(".") + 1);
            String contentType = getContentType(fileExtension);

            Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();

            BufferedReader reader = new BufferedReader(new FileReader(csvFile));
            // Header
            String line = reader.readLine();
            String field_names[] = line.split(",");
			while ((line = reader.readLine()) != null) {
                String field_values[] = line.split(",");
                String objectName = objectNameHeaderIndex != -1 ? 
                    field_values[objectNameHeaderIndex] + "." + fileExtension :
                    UUID.randomUUID() + "." + fileExtension;
                Map<String, String> customMetadata = new HashMap<>();
                for(int i = 0; i < field_names.length; i++){
                    customMetadata.put(field_names[i], field_values[i]);
                }
                uploadObject(storage, projectId, bucketName, objectName, sampleFile, customMetadata, contentType);
                try {
                    // Throtling for adapting to one upload per second limitation in GCS
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
			}
			reader.close();
		} 
        catch (IOException e) {
			e.printStackTrace();
		}
    }

    public static void uploadObject(Storage storage, String projectId, String bucketName, String objectName, String filePath, Map<String, String> customMetadata, String contentType) throws IOException {
        System.out.println("Uploading file: " + objectName);
        BlobId blobId = BlobId.of(bucketName, objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(contentType).setMetadata(customMetadata).build();
        storage.create(blobInfo, Files.readAllBytes(Paths.get(filePath)));  
    }
    
    public static String getContentType(String fileExtension){
        // Few sample content types. Not exhaustive
        String contentType = null;
        switch(fileExtension){
            case "jpeg":
            case "jpg":
                contentType = "image/jpeg";
                break;
            case "gif":
                contentType = "image/gif";
                break;
            case "png":
                contentType = "image/png";
                break;
            case "tiff":
                contentType = "image/tiff";
                break;
            case "pdf":
                contentType = "application/pdf";
                break;            
            case "txt":
                contentType = "text/plain";
                break;             
            case "csv":
                contentType = "text/csv";
                break;               
            case "html":
                contentType = "text/html";
                break;  
            default:
                break;  
        }
        return contentType;
    }

    public static void usage(){
        System.out.println("Run arguments:");
        System.out.print("-D" + INPUT_CSV + "=[csv_fake_data_file]");
        System.out.print(" -D" + PROJECT_ID + "=[project_id]");
        System.out.print(" -D" + BUCKET_NAME + "=[bucket_name]");
        System.out.println(" -D" + SAMPLE_FILE + "=[sample_file_path]");
        System.out.println("[optional - index of the header column used as file name. If not specified UUID name is generated -D" + OBJECT_NAME_HEADER + "=[unique_name_header_index] ] ");
    }

}
