package org.acme.service;

import io.smallrye.mutiny.Uni;
import org.acme.dto.DtoHelper;
import org.acme.dto.UserConnectorConfigDto;
import org.acme.model.DataModelDTO;
import org.acme.model.DataModelProperty;
import org.acme.model.DbConnectionDetails;
import org.acme.model.UserConnectorConfig;
import org.acme.repository.DbConnectionDetailsRepository;
import org.acme.repository.UserConnectorConfigRepository;
import org.acme.resource.utili.*;
import org.bson.types.ObjectId;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.resteasy.reactive.ClientWebApplicationException;
import org.jboss.resteasy.reactive.RestResponse;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.core.Response;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import static org.acme.resource.utili.ConnectionClass.ORACLE_URL;

@ApplicationScoped
public class UserConnectorService {


    private String POSTGRES_URL = "jdbc:postgresql://";
    private String MYSQL_URL = "jdbc:mysql://";

    private String DB2_URL = "jdbc:db2:";

    private String SQL_SERVER_URL = "jdbc:sqlserver://";

    private String ORACLE = "jdbc:oracle:thin:@";
    private String MONGO_URL_SOURCE = "rs0";
    private String MONGO_URL_SINK = "mongodb://";
    private String URL = "http://";
    private String NAME = "name";
    private String CONFIG = "config";
    private String DOUBLE_ORDINAL = "7";

    private String MYSQL_DRIVER_CLASS = "new com.mysql.jdbc.Driver()";

    Logger logger = LoggerFactory.getLogger("universal-connector");

    @Inject
    private UserConnectorConfigRepository userConnectorConfigRepository;

    @Inject
    private DbConnectionDetailsRepository dbConnectionDetailsRepository;

    @RestClient
    private ConnectorService connectorService;

    @RestClient
    private ModellerService modellerService;

    @Inject
    private EncryptionService encryptionService;


    public JSONObject createSourceConnectorJson(UserConnectorConfigDto userConnectorConfigDTO) {
        JSONObject response = null;
        if (userConnectorConfigDTO.getConnector().equals(ConnectorEnum.MYSQL.toString())) {
//            userConnectorConfigDTO.setHost("mysql");
            if(userConnectorConfigDTO.getConnectorFamily().equals(ConnectorEnum.CDC.toString()))
                response = createSourceMysqlJsonDebezium(userConnectorConfigDTO);
            else if(userConnectorConfigDTO.getConnectorFamily().equals(ConnectorEnum.JDBC.toString()))
                response = createSourceJsonJdbc(userConnectorConfigDTO);
        } else if (userConnectorConfigDTO.getConnector().equals(ConnectorEnum.POSTGRES.toString())) {
            userConnectorConfigDTO.setHost("postgres");
            if(userConnectorConfigDTO.getConnectorFamily().equals(ConnectorEnum.CDC.toString()))
                    response = createSourcePostgresDebezium(userConnectorConfigDTO);
            else if(userConnectorConfigDTO.getConnectorFamily().equals(ConnectorEnum.JDBC.toString()))
                response = createSourceJsonJdbc(userConnectorConfigDTO);
        } else if (userConnectorConfigDTO.getConnector().equals(ConnectorEnum.MONGODB.toString())) {
            if(userConnectorConfigDTO.getConnectorFamily().equals(ConnectorEnum.CDC.toString()))
            response = createSourceMongodbJsonDebezium(userConnectorConfigDTO);
            else if(userConnectorConfigDTO.getConnectorFamily().equals(ConnectorEnum.JDBC.toString()))
                response = createSourceJsonJdbc(userConnectorConfigDTO);

        } else if (userConnectorConfigDTO.getConnector().equals(ConnectorEnum.CASSANDRA.toString())) {
            if(userConnectorConfigDTO.getConnectorFamily().equals(ConnectorEnum.CDC.toString()))
                response = createSourceCassandraJsonDebezium(userConnectorConfigDTO);
            else if(userConnectorConfigDTO.getConnectorFamily().equals(ConnectorEnum.JDBC.toString()))
                response = createSourceJsonJdbc(userConnectorConfigDTO);

        } else if (userConnectorConfigDTO.getConnector().equals(ConnectorEnum.DB2.toString())) {
            if(userConnectorConfigDTO.getConnectorFamily().equals(ConnectorEnum.CDC.toString()))
                response = createSourceDB2JsonDebezium(userConnectorConfigDTO);
            else if(userConnectorConfigDTO.getConnectorFamily().equals(ConnectorEnum.JDBC.toString()))
                response = createSourceJsonJdbc(userConnectorConfigDTO);

        } else if (userConnectorConfigDTO.getConnector().equals(ConnectorEnum.SQLSERVER.toString())) {
            if(userConnectorConfigDTO.getConnectorFamily().equals(ConnectorEnum.CDC.toString()))
                response = createSourceSQLServerJsonDebezium(userConnectorConfigDTO);
            else if(userConnectorConfigDTO.getConnectorFamily().equals(ConnectorEnum.JDBC.toString()))
                response = createSourceJsonJdbc(userConnectorConfigDTO);

        } else if (userConnectorConfigDTO.getConnector().equals(ConnectorEnum.VITESS.toString())) {
            if(userConnectorConfigDTO.getConnectorFamily().equals(ConnectorEnum.CDC.toString()))
                response = createSourceVitessJsonDebezium(userConnectorConfigDTO);
            else if(userConnectorConfigDTO.getConnectorFamily().equals(ConnectorEnum.JDBC.toString()))
                response = createSourceJsonJdbc(userConnectorConfigDTO);
        }
        return response;
    }

    private JSONObject createSourceVitessJsonDebezium(UserConnectorConfigDto userConnectorConfigDTO) {
        return null;
    }

    private JSONObject createSourceSQLServerJsonDebezium(UserConnectorConfigDto userConnectorConfigDTO) {
        return null;
    }

    private JSONObject createSourceDB2JsonDebezium(UserConnectorConfigDto userConnectorConfigDTO) {
        return null;
    }

    private JSONObject createSourceCassandraJsonDebezium(UserConnectorConfigDto userConnectorConfigDTO) {
        return null;
    }

    private String createConnectionLinkPostgresJdbc(String host, String port, String dbName, String dbUser, String dbPassword) {
        StringBuilder connectionUrl = new StringBuilder(POSTGRES_URL);
        connectionUrl.append(host).append(":").append(port).append("/").append(dbName);
        return connectionUrl.toString();
    }

    private JSONObject createSourceJsonJdbc(UserConnectorConfigDto userConnectorConfigDto) {
        JSONParser parser = new JSONParser();
        JSONObject response = null;
        try {
            Object obj = parser.parse(new FileReader(TemplatePath.MYSQL_SOURCE_JDBC));
            JSONObject jsonObject = (JSONObject) obj;
            jsonObject.put(NAME, UUID.randomUUID().toString() + "-" + userConnectorConfigDto.getDbName() + "-" + userConnectorConfigDto.getConnectorType()+"-"+userConnectorConfigDto.getConnectorFamily());
            JSONObject configObject = (JSONObject) jsonObject.get(CONFIG);
            if(userConnectorConfigDto.getConnector().equals(ConnectorEnum.MYSQL.toString())) {
                configObject.put("connection.url", createConnectionLinkMysqlJdbc(userConnectorConfigDto.getHost(), userConnectorConfigDto.getPort(), userConnectorConfigDto.getDbName(), userConnectorConfigDto.getDbUser(), userConnectorConfigDto.getDbPassword()));
            }else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.POSTGRES.toString())) {
                configObject.put("connection.url", createConnectionLinkPostgresJdbc(userConnectorConfigDto.getHost(), userConnectorConfigDto.getPort(), userConnectorConfigDto.getDbName(), userConnectorConfigDto.getDbUser(), userConnectorConfigDto.getDbPassword()));
            } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.DB2.toString())) {
                configObject.put("connection.url", createConnectionLinkDB2Jdbc(userConnectorConfigDto.getHost(), userConnectorConfigDto.getPort(), userConnectorConfigDto.getDbName(), userConnectorConfigDto.getDbUser(), userConnectorConfigDto.getDbPassword()));
            } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.CASSANDRA.toString())) {
                configObject.put("connection.url", createConnectionLinkCassandraJdbc(userConnectorConfigDto.getHost(), userConnectorConfigDto.getPort(), userConnectorConfigDto.getDbName(), userConnectorConfigDto.getDbUser(), userConnectorConfigDto.getDbPassword()));
            } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.ORACLE.toString())) {
                configObject.put("connection.url", createConnectionLinkOracleJdbc(userConnectorConfigDto.getHost(), userConnectorConfigDto.getPort(), userConnectorConfigDto.getDbName(), userConnectorConfigDto.getDbUser(), userConnectorConfigDto.getDbPassword()));
            }else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.MONGODB.toString())) {
                configObject.put("connection.url", createConnectionLinkMongoDbJdbc(userConnectorConfigDto.getHost(), userConnectorConfigDto.getPort(), userConnectorConfigDto.getDbName(), userConnectorConfigDto.getDbUser(), userConnectorConfigDto.getDbPassword()));
            }
            configObject.put("connection.user", userConnectorConfigDto.getDbUser());
            configObject.put("connection.password", userConnectorConfigDto.getDbPassword());
            configObject.put("table.whitelist", addTableList(userConnectorConfigDto.getTables()));
            configObject.put("topic.prefix",userConnectorConfigDto.getTopicsPrefix());
            response = jsonObject;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return response;
    }

    private String createConnectionLinkMongoDbJdbc(String host, String port, String dbName, String dbUser, String dbPassword) {
        StringBuilder connectionUrl = new StringBuilder(MONGO_URL_SINK);
        connectionUrl.append(host).append(":").append(port).append("/").append(dbName);
        return connectionUrl.toString();
    }

    private String createConnectionLinkOracleJdbc(String host, String port, String dbName, String dbUser, String dbPassword) {
        StringBuilder connectionUrl = new StringBuilder(ORACLE_URL);
        return connectionUrl.append(ORACLE_URL).append(host).append(":").append(port).append(":").append(dbName).toString();
    }

    private String createConnectionLinkCassandraJdbc(String host, String port, String dbName, String dbUser, String dbPassword) {
        StringBuilder connectionUrl = new StringBuilder(ConnectionClass.CASSANDRA_URL);
        connectionUrl.append(host).append(":").append(port).append("/").append(dbName);
        return connectionUrl.toString();
    }

    private String createConnectionLinkDB2Jdbc(String host, String port, String dbName, String dbUser, String dbPassword) {
        StringBuilder connectionUrl = new StringBuilder(DB2_URL);
        connectionUrl.append(host).append(":").append(port).append("/").append(dbName);
        return connectionUrl.toString();
    }

    private String createConnectionLinkMysqlJdbc(String host, String port, String dbName, String dbUser, String dbPassword) {
        StringBuilder connectionUrl = new StringBuilder(MYSQL_URL);
        connectionUrl.append(host).append(":").append(port).append("/").append(dbName);
        return connectionUrl.toString();
    }

    private String addTableList(List<String> tables) {
        StringBuilder result = new StringBuilder();
        for (String table : tables) {
            result.append(table);
            result.append(",");
        }
        result.replace(result.length() - 1, result.length() - 1, "");
        return result.toString();

    }

    private JSONObject createSourceMongodbJsonDebezium(UserConnectorConfigDto userConnectorSourceConfigDTO) {
        JSONParser parser = new JSONParser();
        JSONObject response = null;
        try {
            Object obj = parser.parse(new FileReader(TemplatePath.MONGODB_SOURCE));
            JSONObject jsonObject = (JSONObject) obj;
            jsonObject.put(NAME, userConnectorSourceConfigDTO.getUserId() + "-" + userConnectorSourceConfigDTO.getConnectorType() + "-" + userConnectorSourceConfigDTO.getDbName() + "-" + jsonObject.get("name"));
            JSONObject configObject = (JSONObject) jsonObject.get(CONFIG);
            configObject.put("mongodb.hosts", createConnectionLinkMongodbSource(userConnectorSourceConfigDTO.getHost(), userConnectorSourceConfigDTO.getPort()));
//            configObject.put("mongodb.hosts","rs0/mongodb:27017");
            configObject.put("mongodb.user", userConnectorSourceConfigDTO.getDbUser());
            configObject.put("mongodb.password", userConnectorSourceConfigDTO.getDbPassword());
            configObject.put("database.include.list", userConnectorSourceConfigDTO.getDbName());
            System.out.println(jsonObject.toJSONString());
            response = jsonObject;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return response;
    }

    private String createConnectionLinkMongodbSource(String host, String port) {
        StringBuilder connectionUrl = new StringBuilder(MONGO_URL_SOURCE);
        return connectionUrl.append("/" + host).append(":").append(port).toString();
    }

    private JSONObject createSourcePostgresDebezium(UserConnectorConfigDto userConnectorConfigDto) {
        JSONParser parser = new JSONParser();
        JSONObject response = null;
        try {
            Object obj = parser.parse(new FileReader(TemplatePath.POSTGRES_SOURCE));
            JSONObject jsonObject = (JSONObject) obj;
            jsonObject.put(NAME, userConnectorConfigDto.getUserId() + "-" + userConnectorConfigDto.getConnectorType() + "-" + userConnectorConfigDto.getDbName() + "-" + jsonObject.get("name"));
            JSONObject configObject = (JSONObject) jsonObject.get(CONFIG);
            configObject.put("database.port", userConnectorConfigDto.getPort());
            configObject.put("database.hostname", userConnectorConfigDto.getHost());
            configObject.put("database.user", userConnectorConfigDto.getDbUser());
            configObject.put("database.password", userConnectorConfigDto.getDbPassword());
            configObject.put("database.server.name", userConnectorConfigDto.getDbName());
            System.out.println(jsonObject.toJSONString());
            response = jsonObject;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return response;
    }

    private JSONObject createSourceMysqlJsonDebezium(UserConnectorConfigDto userConnectorConfigDto) {
        JSONParser parser = new JSONParser();
        JSONObject response = null;
        try {
            Object obj = parser.parse(new FileReader(TemplatePath.MYSQL_SOURCE));
            JSONObject jsonObject = (JSONObject) obj;
            jsonObject.put(NAME, UUID.randomUUID().toString() + "-" + userConnectorConfigDto.getDbName() + "-" + userConnectorConfigDto.getConnectorType()+"-"+userConnectorConfigDto.getConnectorFamily());
            JSONObject configObject = (JSONObject) jsonObject.get(CONFIG);
            configObject.put("database.port", userConnectorConfigDto.getPort());
            configObject.put("database.hostname", userConnectorConfigDto.getHost());
            configObject.put("database.user", userConnectorConfigDto.getDbUser());
            configObject.put("database.password", userConnectorConfigDto.getDbPassword());
            configObject.put("database.include.list", userConnectorConfigDto.getDbName());
        //    configObject.put("topic.prefix",userConnectorConfigDto.getTopicsPrefix());
            if (userConnectorConfigDto.getTables() != null && !userConnectorConfigDto.getTables().isEmpty()) {
                configObject.put("table.include.list", createTableListString(userConnectorConfigDto.getDbName(), userConnectorConfigDto.getTables()));
                if (userConnectorConfigDto.getColumns() != null && !userConnectorConfigDto.getColumns().isEmpty())
                    configObject.put("column.include.list", createColumnListString(userConnectorConfigDto.getDbName(), userConnectorConfigDto.getColumns()));
            } else {
                configObject.put("database.whitelist", userConnectorConfigDto.getDbName());
            }
            response = jsonObject;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return response;
    }

    private String createColumnListString(String dbName, Map<String, List<String>> columns) {
        StringBuilder result = new StringBuilder();
        for (String table : columns.keySet()) {
            for (String column : columns.get(table)) {
                result.append(dbName).append(".").append(table).append(".").append(column).append(",");
            }
        }
        return result.toString();
    }

    private String createTableListString(String dbName, List<String> tables) {
        StringBuilder result = new StringBuilder();
        for (String table : tables) {
            result.append(dbName + "." + table);
            result.append(",");
        }
        result.replace(result.length() - 1, result.length() - 1, "");
        return result.toString();
    }


    public JSONObject createSinkConnectorJson(UserConnectorConfigDto userConnectorConfigDto) {
        JSONObject response = null;
        if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.POSTGRES.toString())) {
            response = createSinkPostgresJson(userConnectorConfigDto);
        } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.MONGODB.toString())) {
            if(userConnectorConfigDto.getConnectorFamily().equals(ConnectorEnum.CDC.toString()))
            response = createSinkMongodbJsonDebezium(userConnectorConfigDto);
            else if(userConnectorConfigDto.getConnectorFamily().equals(ConnectorEnum.JDBC.toString()))
                response = createSinkMongodbJsonJdbc(userConnectorConfigDto);
        } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.MYSQL.toString())) {
            response = createSinkMYSQLJson(userConnectorConfigDto);
        } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.ELASTIC_SEARCH.toString())) {
            response = createSinkESJson(userConnectorConfigDto);
        } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.DB2.toString())) {
            // to be implemented
        } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.SQLSERVER.toString())) {
            // to be implemented
        } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.ORACLE.toString())) {
            // to be implemented
        } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.CASSANDRA.toString())) {
            // to be implemented
        }
        return response;
    }

    private JSONObject createSinkMongodbJsonJdbc(UserConnectorConfigDto userConnectorConfigDto) {
        JSONParser parser = new JSONParser();
        JSONObject response = null;
        try {
            Object obj = parser.parse(new FileReader(TemplatePath.MONGODB_SINK_JDBC));
            JSONObject jsonObject = (JSONObject) obj;
            jsonObject.put(NAME, UUID.randomUUID().toString() + "-" + userConnectorConfigDto.getDbName() + "-" + userConnectorConfigDto.getConnectorType());
            JSONObject configObject = (JSONObject) jsonObject.get(CONFIG);
            configObject.put("database",userConnectorConfigDto.getDbName());
            if(userConnectorConfigDto.getTables()!=null && !userConnectorConfigDto.getTables().isEmpty())
                configObject.put("collection",userConnectorConfigDto.getTables().get(0));
            configObject.put("topics", userConnectorConfigDto.getTopics());
            configObject.put("connection.uri", createConnectionLinkMongoDB(userConnectorConfigDto.getHost(), userConnectorConfigDto.getPort(), userConnectorConfigDto.getDbName(), userConnectorConfigDto.getDbUser(), userConnectorConfigDto.getDbPassword()));
            System.out.println(jsonObject.toJSONString());
            response = jsonObject;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return response;
    }

    private JSONObject createSinkMYSQLJson(UserConnectorConfigDto userConnectorConfigDto) {
        JSONParser parser = new JSONParser();
        JSONObject response = null;
        try {
            Object obj = parser.parse(new FileReader(TemplatePath.MYSQL_SINK));
            JSONObject jsonObject = (JSONObject) obj;
            jsonObject.put(NAME, userConnectorConfigDto.getUserId() + "-" + userConnectorConfigDto.getDbName() + "-" + jsonObject.get("name"));
            JSONObject configObject = (JSONObject) jsonObject.get(CONFIG);
            configObject.put("topics", userConnectorConfigDto.getTopics());
            configObject.put("connection.url", createConnectionLinkMysql(userConnectorConfigDto.getHost(), userConnectorConfigDto.getPort(), userConnectorConfigDto.getDbName(), userConnectorConfigDto.getDbUser(), userConnectorConfigDto.getDbPassword()));
            System.out.println(jsonObject.toJSONString());
            response = jsonObject;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return response;
    }

    private String createConnectionLinkMysql(String host, String port, String dbName, String dbUser, String dbPassword) {
        StringBuilder connectionUrl = new StringBuilder(MYSQL_URL);
        connectionUrl.append(host).append(":").append(port).append("/").append(dbName).append("?user:").append(dbUser).append("&").append("password:").append(dbPassword);
        return connectionUrl.toString();
    }

    private JSONObject createSinkESJson(UserConnectorConfigDto userConnectorConfigDto) {
        JSONParser parser = new JSONParser();
        JSONObject response = null;
        try {
            Object obj = parser.parse(new FileReader(TemplatePath.ES_SINK));
            JSONObject jsonObject = (JSONObject) obj;
            jsonObject.put(NAME, userConnectorConfigDto.getUserId() + "-" + userConnectorConfigDto.getDbName() + "-" + jsonObject.get("name"));
            JSONObject configObject = (JSONObject) jsonObject.get(CONFIG);
            configObject.put("topics", userConnectorConfigDto.getTopics());
            configObject.put("connection.url", createConnectionLinkEs(userConnectorConfigDto.getHost(), userConnectorConfigDto.getPort(), userConnectorConfigDto.getDbName(), userConnectorConfigDto.getDbUser(), userConnectorConfigDto.getDbPassword()));
            System.out.println(jsonObject.toJSONString());
            response = jsonObject;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return response;
    }

    private String createConnectionLinkEs(String host, String port, String dbName, String dbUser, String dbPassword) {
        StringBuilder connectionUrl = new StringBuilder(URL);
        connectionUrl.append(host).append(":").append(port);
        return connectionUrl.toString();
    }

    private JSONObject createSinkMongodbJsonDebezium(UserConnectorConfigDto userConnectorConfigDto) {
        JSONParser parser = new JSONParser();
        JSONObject response = null;
        try {
            Object obj = parser.parse(new FileReader(TemplatePath.MONGODB_SINK));
            JSONObject jsonObject = (JSONObject) obj;
            jsonObject.put(NAME, UUID.randomUUID().toString() + "-" + userConnectorConfigDto.getDbName() + "-" + userConnectorConfigDto.getConnectorType());
            JSONObject configObject = (JSONObject) jsonObject.get(CONFIG);
            configObject.put("database",userConnectorConfigDto.getDbName());
            if(userConnectorConfigDto.getTables()!=null && !userConnectorConfigDto.getTables().isEmpty())
            configObject.put("collection",userConnectorConfigDto.getTables().get(0));
            configObject.put("topics", userConnectorConfigDto.getTopics());
            configObject.put("connection.uri", createConnectionLinkMongoDB(userConnectorConfigDto.getHost(), userConnectorConfigDto.getPort(), userConnectorConfigDto.getDbName(), userConnectorConfigDto.getDbUser(), userConnectorConfigDto.getDbPassword()));
            System.out.println(jsonObject.toJSONString());
            response = jsonObject;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return response;
    }

    private JSONObject createSinkPostgresJson(UserConnectorConfigDto userConnectorConfigDto) {
        JSONParser parser = new JSONParser();
        JSONObject response = null;
        try {
            Object obj = parser.parse(new FileReader(TemplatePath.POSTGRES_SINK));
            JSONObject jsonObject = (JSONObject) obj;
            jsonObject.put(NAME, userConnectorConfigDto.getUserId() + "-" + userConnectorConfigDto.getDbName() + "-" + userConnectorConfigDto.getConnectorType());
            JSONObject configObject = (JSONObject) jsonObject.get(CONFIG);
            configObject.put("pk.fields",userConnectorConfigDto.getPrimaryKeys());
            configObject.put("topics", userConnectorConfigDto.getTopics());
            configObject.put("connection.url", createConnectionLinkPostgres(userConnectorConfigDto.getHost(), userConnectorConfigDto.getPort(), userConnectorConfigDto.getDbName(), userConnectorConfigDto.getDbUser(), userConnectorConfigDto.getDbPassword()));
            System.out.println(jsonObject.toJSONString());
            response = jsonObject;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return response;
    }

    private String createConnectionLinkMongoDB(String host, String port, String dbName, String dbUser, String dbPassword) {
        StringBuilder connectionUrl = new StringBuilder(MONGO_URL_SINK);
        connectionUrl.append(dbUser).append(":").append(dbPassword).append("@").append(host).append(":").append(port).append("/").append(dbName).append("?authSource=admin&w=1&");
        return connectionUrl.toString();
    }

    private String createConnectionLinkPostgres(String host, String port, String dbName, String dbUser, String dbPassword) {
        StringBuilder connectionUrl = new StringBuilder(POSTGRES_URL);
        connectionUrl.append(host).append(":").append(port).append("/" + dbName).append("?user=").append(dbUser + "&").append("password=").append(dbPassword);
        return connectionUrl.toString();
    }

    public List<DataModelDTO> createDataModelMapping(UserConnectorConfigDto userConnectorConfigDto) throws SQLException, ClassNotFoundException {
        JSONObject result = new JSONObject();
        List<DataModelDTO> response = null;
        userConnectorConfigDto.setDbPassword(encryptionService.decrypt(userConnectorConfigDto.getDbPassword()));
        if (userConnectorConfigDto.getConnectorType().equals(ConnectorEnum.SOURCE.toString())) {
            if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.MYSQL.toString())) {
                response = createDataModelJson(userConnectorConfigDto, ConnectionClass.createMysqlConnectionUrl(userConnectorConfigDto), result);
            } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.POSTGRES.toString())) {
                response = createDataModelJson(userConnectorConfigDto, ConnectionClass.createPostgresConnectionUrl(userConnectorConfigDto), result);
            } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.MONGODB.toString())) {
                response = createDataModelJson(userConnectorConfigDto, ConnectionClass.createMONGODbConnectionUrl(userConnectorConfigDto), result);
            } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.ORACLE.toString())) {
                response = createDataModelJson(userConnectorConfigDto, ConnectionClass.createOracleConnectionUrl(userConnectorConfigDto), result);
            } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.SQLSERVER.toString())) {
                response = createDataModelJson(userConnectorConfigDto, ConnectionClass.createSqlServerConnectionUrl(userConnectorConfigDto), result);
            } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.VITESS.toString())) {
                // to be implemented

            } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.ELASTIC_SEARCH.toString())) {
            } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.CASSANDRA.toString())) {
                response = createDataModelJson(userConnectorConfigDto, ConnectionClass.createCassandraConnectionUrl(userConnectorConfigDto), result);
            } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.DB2.toString())) {
                response = createDataModelJson(userConnectorConfigDto, ConnectionClass.createDB2ConnectionUrl(userConnectorConfigDto), result);
            }
        }
        return response;
    }

    private List<DataModelDTO> createDataModelJson(UserConnectorConfigDto userConnectorConfigDto, Connection con, JSONObject result) throws SQLException {
        DatabaseMetaData metaData = con.getMetaData();
        List<DataModelDTO> dataModelDTOS = new ArrayList<>();
        if (userConnectorConfigDto.getColumns() != null && !userConnectorConfigDto.getColumns().isEmpty()) {
            for (String table : userConnectorConfigDto.getColumns().keySet()) {
                List<DataModelProperty> dataModelProperties = new ArrayList<>();
                DataModelDTO response = new DataModelDTO();
                response.setWorkspaceName(userConnectorConfigDto.getWorkSpaceName());
                response.setMiniAppName(userConnectorConfigDto.getMiniAppName());
                response.setFileType("datamodel");
                result.put("Entity", table);
                response.setFileName(table);
                HashMap<String, String> fieldMap = new HashMap<>();

                try (ResultSet columns = metaData.getColumns(null, null, table, null)) {
                    while (columns.next()) {
                        String columnName = columns.getString("COLUMN_NAME");
                        String datatype = columns.getString("DATA_TYPE");
                        List<String> columnsName = userConnectorConfigDto.getColumns().get(table);
                        if (columnsName != null && !columnsName.isEmpty()) {
                            if (columnsName.contains(columnName)) {
                                DataModelProperty dataModelProperty = new DataModelProperty();
                                dataModelProperty.setName(columnName);
                                if (datatype.equals(DOUBLE_ORDINAL)) {
                                    fieldMap.put(columnName, JDBCType.valueOf(Integer.valueOf(datatype) - 1).name());
                                    dataModelProperty.setType(JDBCType.valueOf(Integer.valueOf(datatype) - 1).name());
                                } else {
                                    fieldMap.put(columnName, JDBCType.valueOf(Integer.valueOf(datatype)).name());
                                    dataModelProperty.setType(JDBCType.valueOf(Integer.valueOf(datatype)).name());
                                }
                                dataModelProperties.add(dataModelProperty);
                            }
                        }
                    }
                }
                result.put("Value", fieldMap);
                response.setDataModelProperties(dataModelProperties);
                dataModelDTOS.add(response);
            }
        }else {
            for (String table : userConnectorConfigDto.getTables()) {
                List<DataModelProperty> dataModelProperties = new ArrayList<>();
                DataModelDTO response = new DataModelDTO();
                response.setWorkspaceName(userConnectorConfigDto.getWorkSpaceName());
                response.setMiniAppName(userConnectorConfigDto.getMiniAppName());
                response.setFileType("datamodel");
                result.put("Entity", table);
                response.setFileName(table);
                HashMap<String, String> fieldMap = new HashMap<>();

                try (ResultSet columns = metaData.getColumns(null, null, table, null)) {
                    while (columns.next()) {
                        String columnName = columns.getString("COLUMN_NAME");
                        String datatype = columns.getString("DATA_TYPE");
                                DataModelProperty dataModelProperty = new DataModelProperty();
                                dataModelProperty.setName(columnName);
                                if (datatype.equals(DOUBLE_ORDINAL)) {
                                    fieldMap.put(columnName, JDBCType.valueOf(Integer.valueOf(datatype) - 1).name());
                                    dataModelProperty.setType(JDBCType.valueOf(Integer.valueOf(datatype) - 1).name());
                                } else {
                                    fieldMap.put(columnName, JDBCType.valueOf(Integer.valueOf(datatype)).name());
                                    dataModelProperty.setType(JDBCType.valueOf(Integer.valueOf(datatype)).name());
                                }
                                dataModelProperties.add(dataModelProperty);
                    }
                }
                result.put("Value", fieldMap);
                response.setDataModelProperties(dataModelProperties);
                dataModelDTOS.add(response);
            }
        }
        return dataModelDTOS;
    }

    public void savingConfigurationInDB(Response response, JSONObject connectorJson, UserConnectorConfigDto userConnectorConfigDto) {
        userConnectorConfigDto.setName(connectorJson.get("name").toString());
        JSONObject obj = new JSONObject((Map) response.readEntity(JSONObject.class).get("config"));
        userConnectorConfigDto.setConfiguration(obj.toString());
        userConnectorConfigDto.setDbPassword(encryptionService.encrypt(userConnectorConfigDto.getDbPassword()));
        userConnectorConfigRepository.persist(DtoHelper.convertFromUserConnectorConfigDto(userConnectorConfigDto));
    }

    public UserConnectorConfigDto findConnectorConfigById(ObjectId id) {
        UserConnectorConfig userConnectorConfigDb = userConnectorConfigRepository.findById(id);
        if (userConnectorConfigDb != null)
            return DtoHelper.convertToUserConnectorConfigDTO(userConnectorConfigDb);
        else
            return null;
    }

    public UserConnectorConfigDto findConnectorConfigByName(String name) {
        UserConnectorConfig userConnectorConfigDb = userConnectorConfigRepository.findByName(name);
        if (userConnectorConfigDb != null)
            return DtoHelper.convertToUserConnectorConfigDTO(userConnectorConfigDb);
        return null;
    }

    public long deleteConnector(String connectorName) {
        return userConnectorConfigRepository.deleteByName(connectorName);
    }

    public void updateConfig(JSONObject connectorJson, Response response, UserConnectorConfigDto userConnectorConfigDto) {
        try {
            userConnectorConfigDto.setName(connectorJson.get("name").toString());
            JSONObject obj = new JSONObject((Map) response.readEntity(JSONObject.class).get("config"));
            userConnectorConfigDto.setConfiguration(obj.toString());
            UserConnectorConfig userConnectorConfig = DtoHelper.convertFromUserConnectorConfigDto(userConnectorConfigDto);
            userConnectorConfigRepository.update(userConnectorConfig);
//            userConnectorConfigRepository.persistOrUpdate(userConnectorConfig);
            List<DataModelDTO> dataModelMapping = this.createDataModelMapping(userConnectorConfigDto);
            logger.debug("added model class into generated folder...");
            for (DataModelDTO dataModelDTO : dataModelMapping) {
                dataModelDTO.setFileName(Utility.createFileName(dataModelDTO.getFileName()));
                modellerService.addModeller(dataModelDTO).subscribe().with(
                        item -> {
                            logger.debug("added model for the object = " + dataModelDTO.getFileName());
                        }
                );
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<UserConnectorConfigDto> findminiAppName(String miniAppName) {
        List<UserConnectorConfigDto> userConnectorConfigDtos = new ArrayList<>();
        for (UserConnectorConfig userConnectorConfig : userConnectorConfigRepository.findByMiniAppName(miniAppName)) {
            UserConnectorConfigDto userConnectorConfigDto = DtoHelper.convertToUserConnectorConfigDTO(userConnectorConfig);
            JSONObject status1 = connectorService.findStatus(userConnectorConfig.getName());
            HashMap connector = (HashMap) status1.get("connector");
            String state = (String) connector.get("state");
            userConnectorConfigDto.setStatus(state);
            userConnectorConfigDtos.add(userConnectorConfigDto);
        }
        return userConnectorConfigDtos;
    }

    public Uni<EventResponseModel> addSourceConnector(UserConnectorConfigDto userConnectorConfigDto) throws SQLException, ClassNotFoundException {
        JSONObject connectorJson = null;
        userConnectorConfigDto = helperFunctionToFillDetailsFromDbConfig(userConnectorConfigDto);
        EventResponseModel eventResponseModel = new EventResponseModel();
        connectorJson = this.createSourceConnectorJson(userConnectorConfigDto);
        System.out.println("config json = "+connectorJson.toJSONString());
        try {
            Response connectorResponse = connectorService.addConnector(connectorJson);

            if (connectorResponse.getStatus() == RestResponse.StatusCode.CREATED) {
                this.savingConfigurationInDB(connectorResponse, connectorJson, userConnectorConfigDto);
                // create data model mapping
               // userConnectorConfigDto.setHost("localhost");
                List<DataModelDTO> dataModelMapping = this.createDataModelMapping(userConnectorConfigDto);
                logger.debug("added model class into generated folder...");
                for (DataModelDTO dataModelDTO : dataModelMapping) {
                    dataModelDTO.setFileName(Utility.createFileName(dataModelDTO.getFileName()));
                    modellerService.addModeller(dataModelDTO).subscribe().with(
                            item -> {
                                logger.debug("added model for the object = " + dataModelDTO.getFileName());
                            }
                    );
                }
                eventResponseModel.setMessage("added connector and created model..");
                eventResponseModel.setData(RestResponse.StatusCode.OK);
            }
        } catch (ClientWebApplicationException exception) {
            if (exception.getResponse().getStatus() == RestResponse.StatusCode.CONFLICT) {
                logger.debug("connector with same name is present");
                eventResponseModel.setMessage("connector with same name is present");
                eventResponseModel.setData(RestResponse.StatusCode.CONFLICT);
                return Uni.createFrom().item(eventResponseModel);
            } else if (exception.getResponse().getStatus() == RestResponse.StatusCode.INTERNAL_SERVER_ERROR) {
                logger.debug("issue with connector api");
                eventResponseModel.setMessage("issue with connector api");
                eventResponseModel.setData(RestResponse.StatusCode.INTERNAL_SERVER_ERROR);
                return Uni.createFrom().item(eventResponseModel);
            }else{
                exception.printStackTrace();
                eventResponseModel.setMessage("api response error : "+exception.getMessage());
                eventResponseModel.setData(exception.getResponse().getStatus());
               return Uni.createFrom().item(eventResponseModel);
            }
        }
        return Uni.createFrom().item(eventResponseModel);
    }

    private UserConnectorConfigDto helperFunctionToFillDetailsFromDbConfig(UserConnectorConfigDto userConnectorConfigDto) {
        DbConnectionDetails byConnectionName = dbConnectionDetailsRepository.findByConnectionName(userConnectorConfigDto.getConnectionName());
        if(byConnectionName!=null){
            userConnectorConfigDto.setConnector(byConnectionName.getDb());
            userConnectorConfigDto.setDbName(byConnectionName.getDbName());
            userConnectorConfigDto.setDbUser(byConnectionName.getDbUser());
            userConnectorConfigDto.setDbPassword(encryptionService.decrypt(byConnectionName.getDbPassword()));
            userConnectorConfigDto.setHost(byConnectionName.getHost());
            userConnectorConfigDto.setPort(byConnectionName.getPort());
            userConnectorConfigDto.setMiniAppName(byConnectionName.getMiniAppName());
            userConnectorConfigDto.setWorkSpaceName(byConnectionName.getWorkSpaceName());
        }
        return userConnectorConfigDto;
    }

    public Uni<EventResponseModel> updateConnector(String name, UserConnectorConfigDto userConnectorConfigDto) {
        EventResponseModel responseModel = new EventResponseModel();
        Response response = null;
        JSONObject connectorJson = null;
        try {
            if (userConnectorConfigDto.getConnectorType().equals(ConnectorEnum.SOURCE.toString())) {
                connectorJson = this.createSourceConnectorJson(userConnectorConfigDto);
            } else if (userConnectorConfigDto.getConnectorType().equals(ConnectorEnum.SINK.toString())) {
                connectorJson = this.createSinkConnectorJson(userConnectorConfigDto);
            }
            response = connectorService.updateConnector(name,(JSONObject) connectorJson.get("config") );
            logger.debug("successfully updated connector " + response);
            if (response.getStatus() == RestResponse.StatusCode.OK) {
                this.updateConfig(connectorJson, response, userConnectorConfigDto);
                logger.debug("saved configuration into the db");
            } else if (response.getStatus() == RestResponse.StatusCode.CONFLICT) {
                logger.debug("connector with same name is present");
                responseModel.setMessage("connector with same name is present");
                return Uni.createFrom().item(responseModel);
            }

        } catch (Exception e) {
            e.printStackTrace();
            responseModel.setMessage("some problem occurred with the given input with error : " + e.getMessage());
            return Uni.createFrom().item(responseModel);
        }
        responseModel.setMessage("connector updated....");
        return Uni.createFrom().item(responseModel);
    }

    private JSONObject convertJson(JSONObject response) {
        JSONObject result = new JSONObject();

        result.remove("name");
        return (JSONObject) result.get("config");
    }

    public ResponseUtil getStatus(String connector) {
        ResponseUtil response = new ResponseUtil();
        JSONObject result = null;
        try {
            result = connectorService.findStatus(connector);
        } catch (Exception e) {
            e.printStackTrace();
            response.setMessage("some issue comes .." + e.getMessage());
            response.setStatusCode(Response.Status.BAD_REQUEST.getStatusCode());
            return response;
        }
        response.setData(result);
        return response;
    }

    public ResponseUtil pauseConnector(String connector) {
        ResponseUtil response = new ResponseUtil();
        try {
            connectorService.pauseConnector(connector);
        } catch (Exception e) {
            e.printStackTrace();
            response.setMessage("some issue came with the error : "+e.getMessage());
            response.setStatusCode(Response.Status.BAD_REQUEST.getStatusCode());
            return response;
        }
        response.setMessage("paused the connector...");
        response.setStatusCode(Response.Status.OK.getStatusCode());
        return response;
    }

    public ResponseUtil resumeConnector(String connector) {
        ResponseUtil response = new ResponseUtil();
        try {
            connectorService.resumeConnector(connector);
        } catch (Exception e) {
            e.printStackTrace();
            response.setMessage("Some issue came with the error : "+e.getMessage());
            response.setStatusCode(Response.Status.BAD_REQUEST.getStatusCode());
            return response;
        }
        response.setMessage("resumed the connector...");
        response.setStatusCode(Response.Status.OK.getStatusCode());
        return response;
    }

    public ResponseUtil deleteConfig(String connectorName) {
        ResponseUtil response = new ResponseUtil();
        Response connectorResponse = null;
        try {
            UserConnectorConfigDto connectorConfigDb = this.findConnectorConfigByName(connectorName);
            if (connectorConfigDb != null) {
                connectorResponse = connectorService.deleteConnector(connectorConfigDb.getName());
                if (connectorResponse.getStatus() == 204) {
                    long result = this.deleteConnector(connectorName);
                    if (result == 1) {
                        response.setMessage("Connector deleted successfully with name  : " + connectorName);
                        response.setStatusCode(Response.Status.OK.getStatusCode());
                    }
                }
            } else {
                response.setMessage("no such connector is present in db with name  : " + connectorName);
                response.setStatusCode(Response.Status.NOT_FOUND.getStatusCode());
            }

        } catch (ClientWebApplicationException clientWebApplicationException) {
            clientWebApplicationException.printStackTrace();
            if (clientWebApplicationException.getResponse().getStatus() == RestResponse.StatusCode.CONFLICT) {
                logger.debug("re-balance is in process");
                response.setStatusCode(Response.Status.BAD_REQUEST.getStatusCode());
                response.setMessage("re-balance is in process can not delete this connector for now..");
            }
        }
        return response;
    }

    public ResponseUtil getConnectorByName(String name) {
        ResponseUtil response = new ResponseUtil();
        try {
            UserConnectorConfigDto connectorDb = this.findConnectorConfigByName(name);
            if (connectorDb != null) {
                response.setStatusCode(Response.Status.OK.getStatusCode());
                response.setData(connectorDb);
            } else {
                response.setMessage("No data present...");
                response.setStatusCode(Response.Status.NO_CONTENT.getStatusCode());
            }
        } catch (Exception e) {
            e.printStackTrace();
            response.setStatusCode(Response.Status.BAD_REQUEST.getStatusCode());
            response.setMessage("Some error comes : " + e.getMessage());
        }
        return response;
    }

    public Response getConnectorById(ObjectId id) {
        Response response = null;
        try {
            UserConnectorConfigDto connectorDb = this.findConnectorConfigById(id);
            if (connectorDb != null) {
                logger.debug("found the connector config = " + connectorDb.toString());
                response = Response.ok(connectorDb).build();
            } else {
                response = Response.ok("No Such Connector present in db with this id = " + id).build();
                logger.debug("no connector config found....");
            }
        } catch (Exception e) {
            e.printStackTrace();
            response = Response.ok(e.getMessage()).build();
        }
        return response;
    }

    public Uni<EventResponseModel> addSinkConnector(UserConnectorConfigDto userConnectorConfigDto) throws SQLException, ClassNotFoundException {
        JSONObject connectorJson = null;
        EventResponseModel eventResponseModel = new EventResponseModel();
        userConnectorConfigDto = helperFunctionToFillDetailsFromDbConfig(userConnectorConfigDto);
        connectorJson = this.createSinkConnectorJson(userConnectorConfigDto);
        try {
            Response connectorResponse = connectorService.addConnector(connectorJson);
            if (connectorResponse.getStatus() == RestResponse.StatusCode.CREATED) {
                this.savingConfigurationInDB(connectorResponse, connectorJson, userConnectorConfigDto);
                eventResponseModel.setMessage("added sink connector...");
                eventResponseModel.setData(Response.Status.OK.getStatusCode());
            }
        } catch (ClientWebApplicationException clientWebApplicationException) {
            if (clientWebApplicationException.getResponse().getStatus() == RestResponse.StatusCode.CONFLICT) {
                logger.debug("connector with same name is present");
                eventResponseModel.setMessage("connector with same name is present");
                eventResponseModel.setData(Response.Status.CONFLICT.getStatusCode());
                return Uni.createFrom().item(eventResponseModel);
            } else if (clientWebApplicationException.getResponse().getStatus() == RestResponse.StatusCode.INTERNAL_SERVER_ERROR) {
                logger.debug("issue with connector api");
                eventResponseModel.setMessage("issue with connector api "+clientWebApplicationException.getLocalizedMessage());
                eventResponseModel.setData(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
                return Uni.createFrom().item(eventResponseModel);
            }
        } catch (NullPointerException nullPointerException) {
            logger.debug("issue with connector api");
            eventResponseModel.setMessage("issue with connector api with null pointer "+nullPointerException.getMessage());
            return Uni.createFrom().item(eventResponseModel);
        }
        return Uni.createFrom().item(eventResponseModel);
    }

    public List<DataModelDTO> getTableStructures(UserConnectorConfigDto userConnectorConfigDto) {
        List<DataModelDTO> result = null;
        DbConnectionDetails byConnectionName = dbConnectionDetailsRepository.findByConnectionName(userConnectorConfigDto.getConnectionName());


            try {
                if(byConnectionName!=null) {
                    byConnectionName.setDbPassword(encryptionService.decrypt(byConnectionName.getDbPassword()));
                    userConnectorConfigDto = helperConverter(userConnectorConfigDto,DtoHelper.convertDbDetailsDtoToUserConnectorConfigDto(byConnectionName));
//                    if (userConnectorConfigDto.getConnectorType().equals(ConnectorEnum.SOURCE.toString())) {
                        if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.MYSQL.toString())) {
                            result = getTableStructure(userConnectorConfigDto, ConnectionClass.createMysqlConnectionUrl(userConnectorConfigDto));
                        } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.POSTGRES.toString())) {
                            result = getTableStructure(userConnectorConfigDto, ConnectionClass.createPostgresConnectionUrl(userConnectorConfigDto));
                        } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.MONGODB.toString())) {
                            result = getTableStructure(userConnectorConfigDto, ConnectionClass.createMONGODbConnectionUrl(userConnectorConfigDto));
                        } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.ORACLE.toString())) {
                            result = getTableStructure(userConnectorConfigDto, ConnectionClass.createOracleConnectionUrl(userConnectorConfigDto));
                        } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.SQLSERVER.toString())) {
                            result = getTableStructure(userConnectorConfigDto, ConnectionClass.createSqlServerConnectionUrl(userConnectorConfigDto));
                        } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.VITESS.toString())) {
                            // to be implemented

                        } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.ELASTIC_SEARCH.toString())) {
                        } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.CASSANDRA.toString())) {
                            result = getTableStructure(userConnectorConfigDto, ConnectionClass.createCassandraConnectionUrl(userConnectorConfigDto));
                        } else if (userConnectorConfigDto.getConnector().equals(ConnectorEnum.DB2.toString())) {
                            result = getTableStructure(userConnectorConfigDto, ConnectionClass.createDB2ConnectionUrl(userConnectorConfigDto));
                        }
//                    }
                }
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
                Response.ok(sqlException.getErrorCode(), "connection failed with below error : " + sqlException.getMessage()).build();
            }
            return result;
        }

    private UserConnectorConfigDto helperConverter(UserConnectorConfigDto older, UserConnectorConfigDto fromDb) {
        older.setConnector(fromDb.getConnector());
        older.setConnectionName(fromDb.getConnectionName());
        older.setId(fromDb.getId());
        older.setDbUser(fromDb.getDbUser());
        older.setDbPassword(fromDb.getDbPassword());
        older.setHost(fromDb.getHost());
        older.setPort(fromDb.getPort());
        older.setWorkSpaceName(fromDb.getWorkSpaceName());
        older.setMiniAppName(fromDb.getMiniAppName());
        older.setDbName(fromDb.getDbName());

        return older;
    }

    private List<DataModelDTO> getTableStructure(UserConnectorConfigDto userConnectorConfigDto, Connection con) throws SQLException {
        DatabaseMetaData metaData = con.getMetaData();
        List<DataModelDTO> dataModelDTOS = new ArrayList<>();
        if (userConnectorConfigDto.getTables() != null && !userConnectorConfigDto.getTables().isEmpty()) {
            for (String table : userConnectorConfigDto.getTables()) {
                List<DataModelProperty> dataModelProperties = new ArrayList<>();
                DataModelDTO response = new DataModelDTO();
                response.setWorkspaceName(userConnectorConfigDto.getWorkSpaceName());
                response.setMiniAppName(userConnectorConfigDto.getMiniAppName());
                response.setFileType("datamodel");
                response.setFileName(table);
                HashMap<String, String> fieldMap = new HashMap<>();

                try (ResultSet columns = metaData.getColumns(null, null, table, null)) {
                    while (columns.next()) {
                        String columnName = columns.getString("COLUMN_NAME");
                        String datatype = columns.getString("DATA_TYPE");
                        DataModelProperty dataModelProperty = new DataModelProperty();
                        dataModelProperty.setName(columnName);
                        if (datatype.equals(DOUBLE_ORDINAL)) {
                            fieldMap.put(columnName, JDBCType.valueOf(Integer.valueOf(datatype) - 1).name());
                            dataModelProperty.setType(JDBCType.valueOf(Integer.valueOf(datatype) - 1).name());
                        } else {
                            fieldMap.put(columnName, JDBCType.valueOf(Integer.valueOf(datatype)).name());
                            dataModelProperty.setType(JDBCType.valueOf(Integer.valueOf(datatype)).name());
                        }
                        dataModelProperties.add(dataModelProperty);
                    }
                }
                response.setDataModelProperties(dataModelProperties);
                dataModelDTOS.add(response);
            }
        }
        return dataModelDTOS;
    }

    public ResponseUtil getTopics(String connector) {
        ResponseUtil responseUtil = new ResponseUtil();
        JSONObject topics = connectorService.getTopics(connector);
        System.out.println(topics);
        responseUtil.setStatusCode(Response.Status.OK.getStatusCode());
        responseUtil.setData(topics);
        return responseUtil;
    }

    public ResponseUtil resetTopics(String connector) {
        ResponseUtil responseUtil = new ResponseUtil();
        JSONObject topics = connectorService.resetTopics(connector);
        System.out.println(topics);
        responseUtil.setStatusCode(Response.Status.OK.getStatusCode());
        responseUtil.setData(topics);
        return responseUtil;
    }

    public List<String> getTables(String connectionName) {
        List<String> tables = new ArrayList<>();
        try {
            DbConnectionDetails connectionDetailsByConnectionName = dbConnectionDetailsRepository.findByConnectionName(connectionName);
            if(connectionDetailsByConnectionName!=null) connectionDetailsByConnectionName.setDbPassword(encryptionService.decrypt(connectionDetailsByConnectionName.getDbPassword()));
            if(connectionDetailsByConnectionName!=null && connectionDetailsByConnectionName.getDb().equals(ConnectorEnum.MYSQL.toString())) {
                Connection mysqlConnectionUrl = ConnectionClass.createMysqlConnectionUrl(DtoHelper.convertDbDetailsDtoToUserConnectorConfigDto(connectionDetailsByConnectionName));
                DatabaseMetaData metaData = mysqlConnectionUrl.getMetaData();
                String[] tableTypes = {"TABLE"};
                ResultSet resultSet = metaData.getTables(null, null, null, tableTypes);

                while (resultSet.next()) {
                    tables.add(resultSet.getString("TABLE_NAME"));
                }
            }else if(connectionDetailsByConnectionName!=null && connectionDetailsByConnectionName.getDb().equals(ConnectorEnum.POSTGRES.toString())){
                Connection postgresConnectionUrl = ConnectionClass.createPostgresConnectionUrl(DtoHelper.convertDbDetailsDtoToUserConnectorConfigDto(connectionDetailsByConnectionName));
                DatabaseMetaData metaData = postgresConnectionUrl.getMetaData();
                String[] tableTypes = {"TABLE"};
                ResultSet resultSet = metaData.getTables(null, null, null, tableTypes);

                while (resultSet.next()) {
                    tables.add(resultSet.getString("TABLE_NAME"));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return tables;
    }

    public Map<String, String> findStatus(String miniAppName) {
        Map<String,String> status = new HashMap<>();
        List<UserConnectorConfigDto> userConnectorConfigDtos = new ArrayList<>();
        for (UserConnectorConfig userConnectorConfig : userConnectorConfigRepository.findByMiniAppName(miniAppName))
            userConnectorConfigDtos.add(DtoHelper.convertToUserConnectorConfigDTO(userConnectorConfig));
        for(UserConnectorConfigDto userConnectorConfigDto:userConnectorConfigDtos){
            JSONObject status1 = connectorService.findStatus(userConnectorConfigDto.getName());
            HashMap connector = (HashMap)status1.get("connector");
            String state = (String) connector.get("state");
            status.put(userConnectorConfigDto.getName(),state);
        }
        return status;
    }
}
