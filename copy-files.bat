echo 'copying jar files'
docker cp C:\Users\Akhil\Downloads\applications\universal-connectors\universal-debezium-connector\src\main\resources\jars\mysql-connector-java-8.0.30.jar 5ebfed7fb31b:/kafka/libs/
echo 'copied mysql-source-jdbc jar'
docker cp C:\Users\Akhil\Downloads\applications\universal-connectors\universal-debezium-connector\src\main\resources\jars\mongodb-kafka-connect-mongodb-1.8.1/ 5ebfed7fb31b:/kafka/connect/
echo 'copied mongodb-sink into kafka-connect'
@REM echo 'copied mysql-source-jdbc jar'
@REM docker cp C:\Users\Akhil\Downloads\applications\universal-connectors\universal-debezium-connector\src\main\resources\jars\init-kafka-connect-odp-1.3.3/ ec338a62ad6a:/kafka/connect/
echo 'copied sftp jar'
docker cp C:\Users\Akhil\Downloads\applications\universal-connectors\universal-debezium-connector\src\main\resources\jars\confluentinc-kafka-connect-sftp-3.1.11/ 5ebfed7fb31b:/kafka/connect/
docker restart 5ebfed7fb31b
echo 'restarted the kafka-connect'