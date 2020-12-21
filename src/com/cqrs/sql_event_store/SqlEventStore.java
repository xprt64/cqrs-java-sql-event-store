package com.cqrs.sql_event_store;

import com.cqrs.aggregates.AggregateDescriptor;
import com.cqrs.base.Event;
import com.cqrs.base.EventStore;
import com.cqrs.event_store.exceptions.StorageException;
import com.cqrs.events.EventWithMetaData;
import com.cqrs.events.MetaData;
import com.cqrs.util.Guid;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SqlEventStore implements EventStore {

    private final DataSource dataSource;
    private final String tableName;
    private final CreateTableSqlFactory sqlFactory;
    private final HashMap<String, Class<?>> classCache = new HashMap<>();

    ObjectMapper mapper = new ObjectMapper();

    public SqlEventStore(
        DataSource dataSource,
        String tableName,
        CreateTableSqlFactory sqlFactory
    ) {
        this.dataSource = dataSource;
        this.tableName = tableName;
        if (null == sqlFactory) {
            sqlFactory = defaultFactory();
        }
        this.sqlFactory = sqlFactory;
        mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
        mapper.registerModule(new ParameterNamesModule(JsonCreator.Mode.PROPERTIES));
        mapper.findAndRegisterModules();
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.EVERYTHING, JsonTypeInfo.As.PROPERTY);
    }

    public SqlEventStore(
        DataSource dataSource,
        String tableName
    ) {
        this(dataSource, tableName, null);
    }

    public static String makeStreamId(AggregateDescriptor aggregateDescriptor) throws NoSuchAlgorithmException {
        MessageDigest crypt = MessageDigest.getInstance("SHA-1");
        crypt.reset();
        crypt.update(aggregateDescriptor.toString().getBytes(StandardCharsets.UTF_8));
        return Guid.bin2hex(crypt.digest());
    }

    public void createStore() throws SQLException, NoSuchAlgorithmException {
        try (Connection connection = dataSource.getConnection()) {
            try (Statement stmt = connection.createStatement()) {
                int aggregateIdLength = Guid.generate().length();
                int aggregateStreamLength = makeStreamId(new AggregateDescriptor("1", "2")).length();
                String sql = sqlFactory.generateSql(tableName, aggregateIdLength, aggregateStreamLength);
                stmt.executeUpdate(sql);
            }
        }
    }

    private CreateTableSqlFactory defaultFactory() {
        return (tableName, aggregateIdLength, aggregateStreamLength) ->
            "CREATE TABLE IF NOT EXISTS " + tableName + "  (" +
                " id BIGINT NOT NULL AUTO_INCREMENT, " +
                " aggregateStream VARCHAR(" + aggregateStreamLength + ")  NOT NULL, " +
                " aggregateType VARCHAR(255)  NOT NULL, " +
                " aggregateId VARCHAR(" + aggregateIdLength + ") NOT NULL, " +
                " version INTEGER (255) NOT NULL, " +
                " eventType VARCHAR(255) NOT NULL, " +
                " payload MEDIUMBLOB NOT NULL, " +
                " dateCreated DATETIME  NOT NULL, " +
                " PRIMARY KEY ( id )," +
                "  INDEX aggregateStream ( aggregateStream )," +
                "  INDEX eventType ( eventType )," +
                " UNIQUE KEY ( aggregateStream, version )" +
                ") ENGINE=InnoDB DEFAULT CHARSET = utf8" +
                "";
    }

    public interface CreateTableSqlFactory {
        String generateSql(String tableName, int aggregateIdLength, int aggregateStreamLength);
    }

    public void dropStore() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
            }
        }
    }

    @Override
    public int loadEventsForAggregate(AggregateDescriptor aggregateDescriptor, Predicate<EventWithMetaData> consumer) throws StorageException {
        int version = -1;
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement stm = connection.prepareStatement("" +
                                                                         "SELECT *  " +
                                                                         "FROM " + tableName + " " +
                                                                         "WHERE aggregateStream = ? " +
                                                                         "ORDER BY id ASC ")) {
                stm.setString(1, SqlEventStore.makeStreamId(aggregateDescriptor));
                try (ResultSet result = stm.executeQuery()) {
                    while (result.next()) {
                        version = result.getInt("version");
                        if (!consumer.test(rehydrateEvent(result))) {
                            return version;
                        }
                    }
                }

            }

        } catch (Throwable exception) {
            exception.printStackTrace();
            throw new StorageException(exception.getClass().getCanonicalName() + ":" + exception.getMessage(), exception);
        }
        return version;
    }

    @Override
    public void appendEventsForAggregate(AggregateDescriptor aggregateDescriptor, List<EventWithMetaData> eventsWithMetaData, int expectedVersion) throws ConcurrentModificationException, StorageException {
        boolean autocommit = false;

        try (Connection connection = dataSource.getConnection()) {
            final String streamId = SqlEventStore.makeStreamId(aggregateDescriptor);
            autocommit = connection.getAutoCommit();
            if (autocommit) {
                connection.setAutoCommit(false);
            }
            try {
                for (EventWithMetaData eventWithMetaData : eventsWithMetaData) {
                    int index = 0;
                    try (PreparedStatement stm = connection.prepareStatement("INSERT INTO " + tableName +
                                                                                 " (aggregateStream,aggregateType,aggregateId,version,eventType,payload,dateCreated) " +
                                                                                 " VALUES(?,?,?,?,?,?,?)" +
                                                                                 "")) {
                        stm.setString(++index, streamId);
                        stm.setString(++index, aggregateDescriptor.aggregateClass);
                        stm.setString(++index, aggregateDescriptor.aggregateId);
                        stm.setInt(++index, ++expectedVersion);
                        stm.setString(++index, eventWithMetaData.event.getClass().getCanonicalName());
                        stm.setString(++index, mapper.writeValueAsString(eventWithMetaData.event));
                        stm.setObject(++index, LocalDateTime.now());
                        //System.out.println(stm.toString());
                        stm.executeUpdate();
                    }
                }
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                if (1062 == e.getErrorCode()) {
                    throw new ConcurrentModificationException();
                }
            } finally {
                if (autocommit) {
                    try {
                        connection.setAutoCommit(autocommit);
                    } catch (SQLException e) {
                    }
                }
            }
        } catch (SQLException | NoSuchAlgorithmException | JsonProcessingException exception) {
            throw new StorageException(exception.getClass().getCanonicalName() + ":" + exception.getMessage(), exception);
        }
    }

    @Override
    public void loadEventsByClassNames(List<String> eventClasses, Predicate<EventWithMetaData> consumer) throws StorageException {
        String eventClassesStr = eventClasses.stream().map(eventClass -> "'" + eventClass + "'").collect(Collectors.joining(","));
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement stm = connection.prepareStatement("" +
                                                                         "SELECT *  " +
                                                                         "FROM " + tableName + " " +
                                                                         "WHERE eventType IN (" + eventClassesStr + ") " +
                                                                         "ORDER BY id ASC ")) {
                stm.setFetchSize(100);
                try (ResultSet result = stm.executeQuery()) {
                    while (result.next()) {
                        if (!consumer.test(rehydrateEvent(result))) {
                            return;
                        }
                    }
                }
            }

        } catch (Throwable exception) {
            throw new StorageException(exception.getMessage(), exception);
        }
    }

    @Override
    public int countEventsByClassNames(List<String> eventClasses) throws StorageException {
        String eventClassesStr = eventClasses.stream().map(eventClass -> "'" + eventClass + "'").collect(Collectors.joining(","));
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement stm = connection.prepareStatement(
                "SELECT COUNT(id) as cnt  FROM " + tableName + " WHERE eventType IN (" + eventClassesStr + ") ORDER BY id ASC ");) {
                try (ResultSet result = stm.executeQuery();) {
                    while (result.next()) {
                        return result.getInt("cnt");
                    }
                }
            }

        } catch (Throwable exception) {
            throw new StorageException(exception.getMessage(), exception);
        }
        return 0;
    }

    private EventWithMetaData rehydrateEvent(ResultSet result) throws SQLException, ClassNotFoundException, JsonProcessingException {
//        long startTime = System.nanoTime();

//        System.out.println( result.getString(FieldPostions.EVENT_TYPE.value));
        String eventType = result.getString(FieldPostions.EVENT_TYPE.value);
//        System.out.println("    getString eventType took " + (System.nanoTime() - startTime)/1000 + " ms");
//        long startTimeBeforeReadValue = System.nanoTime();
        Event event = (Event) mapper.readValue(result.getString(FieldPostions.PAYLOAD.value), loadClass(eventType));
//        System.out.println("    mapper.readValue took " + (System.nanoTime() - startTimeBeforeReadValue)/1000 + " ms");
//        long startTimeBeforeEventWithMetaData = System.nanoTime();
        EventWithMetaData eventWithMetaData = new EventWithMetaData(
            event,
            new MetaData(
                result.getTimestamp(FieldPostions.DATE_CREATED.value).toLocalDateTime(),
                result.getString(FieldPostions.AGGREGATE_ID.value),
                result.getString(FieldPostions.AGGREGATE_TYPE.value)
            )
                .withEventId(result.getString(FieldPostions.ID.value))
                .withVersion(result.getInt(FieldPostions.VERSION.value))
        );
//        System.out.println("    new EventWithMetaData " + eventType + " took " + (System.nanoTime() - startTimeBeforeEventWithMetaData)/1000 + " ms");
//        System.out.println("rehydrateEvent " + eventType + " took " + (System.nanoTime() - startTime)/1000 + " ms");
        return eventWithMetaData;
    }

    private Class<?> loadClass(String eventType) throws ClassNotFoundException {
        if (!classCache.containsKey(eventType)) {
            classCache.put(eventType, Class.forName(eventType));
        }
        return classCache.get(eventType);
    }


    @Override
    public EventWithMetaData findEventById(String id) throws StorageException {
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement stm = connection.prepareStatement("SELECT *  FROM ? WHERE id = ? ");
            stm.setString(1, tableName);
            stm.setString(2, id);
            ResultSet result = stm.executeQuery();
            while (result.next()) {
                return rehydrateEvent(result);
            }
        } catch (Throwable exception) {
            throw new StorageException(exception.getMessage(), exception);
        }
        return null;
    }

    public enum FieldPostions {
        ID(1),
        AGGREGATE_TYPE(3),
        AGGREGATE_ID(4),
        DATE_CREATED(8),
        VERSION(5),
        EVENT_TYPE(6),
        PAYLOAD(7);

        private final int value;

        FieldPostions(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }
}
