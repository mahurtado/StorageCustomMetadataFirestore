# Storage Custom Metadata Firestore sync

Use Google Cloud Storage custom metadata and keep it synchronized with a Firestore database 

!["Solution architecture"](img/arch_gcs_firestore.jpg?raw=true)

## Installation

Deployment from CloudShell console.
Setup environment variables. Use your own:

```bash
REGION=europe-west3
GOOGLE_CLOUD_PROJECT=`gcloud config list --format 'value(core.project)'`
BUCKET_NAME=$GOOGLE_CLOUD_PROJECT
COLLECTION=content
```

Create a Firestore database in Native mode. Create a Cloud Storage bucket in the same region:

```bash
gcloud app create --region=$REGION
gcloud firestore databases create --region=$REGION
gsutil mb -l $REGION gs://$BUCKET_NAME 
```

Create and build Cloud Functions from this repo:

```bash
git clone https://github.com/mahurtado/StorageCustomMetadataFirestore
cd StorageCustomMetadataFirestore/CustomMetadataFirestoreCF
mvn compile
mvn package
```

Deploy the Cloud Functions:

```bash
gcloud services enable cloudbuild.googleapis.com
 
gcloud functions deploy content-gcs-insert \
    --set-env-vars COLLECTION=$COLLECTION \
    --region $REGION \
    --entry-point com.manolo.content.InsertFile \
    --runtime java11 \
    --memory 512MB \
    --trigger-resource $GOOGLE_CLOUD_PROJECT \
    --trigger-event google.storage.object.finalize \
    --source=target/deployment
 
gcloud functions deploy content-gcs-delete \
    --set-env-vars COLLECTION=$COLLECTION \
    --region $REGION \
    --entry-point com.manolo.content.InsertFile \
    --runtime java11 \
    --memory 512MB \
    --trigger-resource $GOOGLE_CLOUD_PROJECT \
    --trigger-event google.storage.object.delete \
    --source=target/deployment
 
gcloud functions deploy content-gcs-metadata-update \
    --set-env-vars COLLECTION=$COLLECTION \
    --region $REGION \
    --entry-point com.manolo.content.InsertFile \
    --runtime java11 \
    --memory 512MB \
    --trigger-resource $GOOGLE_CLOUD_PROJECT \
    --trigger-event google.storage.object.metadataUpdate \
    --source=target/deployment
 
gcloud functions deploy content-gcs-metadata-archive \
    --set-env-vars COLLECTION=$COLLECTION \
    --region $REGION \
    --entry-point com.manolo.content.InsertFile \
    --runtime java11 \
    --memory 512MB \
    --trigger-resource $GOOGLE_CLOUD_PROJECT \
    --trigger-event google.storage.object.archive \
    --source=target/deployment
```

## Usage

For testing, just uploada filess to the configured bucket, include new metadata, modify, etc.
Then check Firestore console and see the changes. Example for uploading a local file with custom metadata:

```bash
gsutil -h "x-goog-meta-key1:value1" cp [path_to_your_file] gs://$GOOGLE_CLOUD_PROJECT
```

## Contributing
Pull requests are welcome. 