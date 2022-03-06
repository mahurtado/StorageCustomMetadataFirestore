INPUT=./files/invoice_1.csv
PROJECT=mh-custom-metadata-firestore
BUCKET=mh-custom-metadata-firestore
SAMPLE_FILE=./files/invoice_sample.jpeg
HEADER_INDEX=0

java -classpath target/fake-invoices-1.0-SNAPSHOT.jar -DinputCsv=$INPUT -DprojectId=$PROJECT -DbucketName=$BUCKET -DsampleFile=$SAMPLE_FILE -DobjectNameHeaderIndex=$HEADER_INDEX com.manolo.content.fake.FakeGenerator 
