package org.acme.resource.utili;

public class TemplatePath {
    public final static String ROOT_PATH_CDC = "src/main/resources/connectors/cdc/";
    public final static String ROOT_PATH_JDBC = "src/main/resources/connectors/jdbc/";
    public final static String MYSQL_SOURCE= ROOT_PATH_CDC +"source/MYSQL-CONNECTOR-SOURCE.json";
    public final static String POSTGRES_SOURCE= ROOT_PATH_CDC +"source/POSTGRES-CONNECTOR-SOURCE.json";
    public final static String MONGODB_SOURCE= ROOT_PATH_CDC +"source/MONGODB-CONNECTOR-SOURCE.json";

    public final static String MYSQL_SOURCE_JDBC= ROOT_PATH_JDBC +"source/MYSQL-CONNECTOR-SOURCE.json";
    public final static String POSTGRES_SOURCE_JDBC= ROOT_PATH_JDBC +"source/POSTGRES-CONNECTOR-SOURCE.json";
    public final static String MONGODB_SOURCE_JDBC= ROOT_PATH_JDBC +"source/MONGODB-CONNECTOR-SOURCE.json";
    public final static String MONGODB_SINK_JDBC= ROOT_PATH_JDBC +"sink/MONGODB-CONNECTOR-SINK.json";


    public final static String MYSQL_SINK= ROOT_PATH_CDC +"sink/MYSQL-CONNECTOR-SINK.json";
    public final static String POSTGRES_SINK= ROOT_PATH_CDC +"sink/POSTGRES-CONNECTOR-SINK.json";
    public final static String MONGODB_SINK= ROOT_PATH_CDC +"sink/MONGODB-CONNECTOR-SINK.json";
    public final static String ES_SINK= ROOT_PATH_CDC +"sink/ES-CONNECTOR-SINK.json";
    public final static String CASSANDRA_SINK= ROOT_PATH_CDC +"sink/CASSANDRA-CONNECTOR-SINK.json";

}
