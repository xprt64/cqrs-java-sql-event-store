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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SqlEventStore implements EventStore {

    private final Connection connection;
    private final String tableName;
    ObjectMapper mapper = new ObjectMapper();

    public SqlEventStore(
        Connection connection,
        String tableName
    ) {
        this.connection = connection;
        this.tableName = tableName;
        mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
        mapper.registerModule(new ParameterNamesModule(JsonCreator.Mode.PROPERTIES));
        mapper.findAndRegisterModules();
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.EVERYTHING, JsonTypeInfo.As.PROPERTY);
    }

    public static String makeStreamId(AggregateDescriptor aggregateDescriptor) throws NoSuchAlgorithmException {
        MessageDigest crypt = MessageDigest.getInstance("SHA-1");
        crypt.reset();
        crypt.update(aggregateDescriptor.toString().getBytes(StandardCharsets.UTF_8));
        return Guid.bin2hex(crypt.digest());
    }

    public void createStore() throws SQLException, NoSuchAlgorithmException {
        Statement stmt = connection.createStatement();

        int aggregateIdLength = Guid.generate().length();
        int aggregateStreamLength = makeStreamId(new AggregateDescriptor("1", "2")).length();

        String sql = "CREATE TABLE IF NOT EXISTS " + tableName + "  (" +
            " id BIGINT NOT NULL AUTO_INCREMENT, " +
            " aggregateStream VARCHAR(" + aggregateStreamLength + ")  NOT NULL, " +
            " aggregateType VARCHAR(255)  NOT NULL, " +
            " aggregateId VARCHAR(" + aggregateIdLength + ") NOT NULL, " +
            " version INTEGER (255) NOT NULL, " +
            " eventType VARCHAR(255) NOT NULL, " +
            " payload JSON NOT NULL, " +
            " dateCreated DATETIME  NOT NULL, " +
            " PRIMARY KEY ( id )," +
            "  INDEX aggregateStream ( aggregateStream )," +
            "  INDEX eventType ( eventType )," +
            " UNIQUE KEY ( aggregateStream, version )" +
            ") ENGINE=InnoDB DEFAULT CHARSET = utf8" +
            "";
        stmt.executeUpdate(sql);
    }

    public void dropStore() throws SQLException {
        Statement stmt = connection.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
    }

    @Override
    public int loadEventsForAggregate(AggregateDescriptor aggregateDescriptor, Predicate<EventWithMetaData> consumer) throws StorageException {
        int version = -1;
        try {
            PreparedStatement stm = connection.prepareStatement("SELECT *  FROM " + tableName + " WHERE aggregateStream = ? ORDER BY id ASC ");
            stm.setString(1, SqlEventStore.makeStreamId(aggregateDescriptor));
            ResultSet result = stm.executeQuery();
            while (result.next()) {
                version = result.getInt("version");
                if (!consumer.test(rehydrateEvent(result))) {
                    return version;
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

        try {
            final String streamId = SqlEventStore.makeStreamId(aggregateDescriptor);
            autocommit = connection.getAutoCommit();
            if (autocommit) {
                connection.setAutoCommit(false);
            }
            try {
                for (EventWithMetaData eventWithMetaData : eventsWithMetaData) {
                    int index = 0;
                    PreparedStatement stm = connection.prepareStatement("INSERT INTO " + tableName +
                        " (aggregateStream,aggregateType,aggregateId,version,eventType,payload,dateCreated) " +
                        " VALUES(?,?,?,?,?,?,?)" +
                        "");
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
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                if (1062 == e.getErrorCode()) {
                    throw new ConcurrentModificationException();
                }
            }
        } catch (SQLException | NoSuchAlgorithmException | JsonProcessingException exception) {
            throw new StorageException(exception.getClass().getCanonicalName() + ":" + exception.getMessage(), exception);
        } finally {
            if (autocommit) {
                try {
                    connection.setAutoCommit(autocommit);
                } catch (SQLException e) {
                }
            }
        }
    }

    @Override
    public void loadEventsByClassNames(List<String> eventClasses, Predicate<EventWithMetaData> consumer) throws StorageException {
        String eventClassesStr = eventClasses.stream().map(eventClass -> "'" + eventClass + "'").collect(Collectors.joining(","));
        try {
            PreparedStatement stm = connection.prepareStatement("SELECT *  FROM " + tableName + " WHERE eventType IN (" + eventClassesStr + ") ORDER BY id ASC ");
            ResultSet result = stm.executeQuery();
            while (result.next()) {
                if (!consumer.test(rehydrateEvent(result))) {
                    return;
                }
            }
        } catch (Throwable exception) {
            throw new StorageException(exception.getMessage(), exception);
        }
    }

    @Override
    public int countEventsByClassNames(List<String> eventClasses) throws StorageException {
        String eventClassesStr = eventClasses.stream().map(eventClass -> "'" + eventClass + "'").collect(Collectors.joining(","));
        try {
            PreparedStatement stm = connection.prepareStatement("SELECT COUNT(id) as cnt  FROM " + tableName + " WHERE eventType IN (" + eventClassesStr + ") ORDER BY id ASC ");
            ResultSet result = stm.executeQuery();
            while (result.next()) {
                return result.getInt("cnt");
            }
        } catch (Throwable exception) {
            throw new StorageException(exception.getMessage(), exception);
        }
        return 0;
    }

    private EventWithMetaData rehydrateEvent(ResultSet result) throws SQLException, ClassNotFoundException, JsonProcessingException {
        String eventType = result.getString("eventType");
        Event event = (Event) mapper.readValue(result.getString("payload"), Class.forName(eventType));
        return new EventWithMetaData(
            event,
            new MetaData(
                result.getTimestamp("dateCreated").toLocalDateTime(),
                result.getString("aggregateId"),
                result.getString("aggregateType")
            )
                .withEventId(result.getString("id"))
                .withVersion(result.getInt("version"))
        );
    }


    @Override
    public EventWithMetaData findEventById(String id) throws StorageException {
        try {
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
}
