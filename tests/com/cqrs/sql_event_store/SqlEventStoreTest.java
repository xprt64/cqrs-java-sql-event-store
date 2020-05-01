package com.cqrs.sql_event_store;

import com.cqrs.aggregates.AggregateDescriptor;
import com.cqrs.base.Event;
import com.cqrs.event_store.exceptions.StorageException;
import com.cqrs.events.EventWithMetaData;
import com.cqrs.events.MetaData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlEventStoreTest {

    private Connection connection;

    @BeforeEach
    void setUp() throws ClassNotFoundException, SQLException {
        connection = DriverManager.getConnection(
            "jdbc:mysql://localhost:3306/eventstore", "root", "pw");
    }

    @Test
    public void normalUse() throws SQLException, NoSuchAlgorithmException, StorageException {
        SqlEventStore sut = new SqlEventStore(connection, "test1");

        sut.dropStore();
        sut.createStore();

        final AggregateDescriptor aggDsc123 = new AggregateDescriptor("123", "aggregateClass");

        int oldVersion = sut.loadEventsForAggregate(aggDsc123, eventWithMetaData -> true);
        assertEquals(-1, oldVersion);

        ArrayList<EventWithMetaData> someEvents = new ArrayList<EventWithMetaData>() {{
            add(new EventWithMetaData(factoryEvent(), new MetaData(LocalDateTime.now(), aggDsc123.aggregateId, aggDsc123.aggregateClass)));
        }};

        sut.appendEventsForAggregate(aggDsc123, someEvents, oldVersion);

        int version = sut.loadEventsForAggregate(aggDsc123, eventWithMetaData -> true);
        assertEquals(oldVersion + 1, version);
    }

    @Test
    public void loadEventsByClassNames() throws SQLException, NoSuchAlgorithmException, StorageException {
        SqlEventStore sut = new SqlEventStore(connection, "test1");

        sut.dropStore();
        sut.createStore();

        final AggregateDescriptor aggDsc1 = new AggregateDescriptor("1", "aggregateClass");
        final AggregateDescriptor aggDsc2 = new AggregateDescriptor("2", "aggregateClass");

        int oldVersion = sut.loadEventsForAggregate(aggDsc1, eventWithMetaData -> true);
        assertEquals(-1, oldVersion);

        sut.appendEventsForAggregate(
            aggDsc1,
            new ArrayList<EventWithMetaData>() {{
                add(new EventWithMetaData(factoryEvent(), new MetaData(LocalDateTime.now(), aggDsc1.aggregateId, aggDsc1.aggregateClass)));
                add(new EventWithMetaData(new WithNestedEvent("aa", factoryEvent()), new MetaData(LocalDateTime.now(), aggDsc1.aggregateId, aggDsc1.aggregateClass)));
            }},
            oldVersion
        );
        oldVersion = sut.loadEventsForAggregate(aggDsc1, eventWithMetaData -> true);

        sut.appendEventsForAggregate(
            aggDsc2,
            new ArrayList<EventWithMetaData>() {{
                add(new EventWithMetaData(factoryEvent(), new MetaData(LocalDateTime.now(), aggDsc2.aggregateId, aggDsc2.aggregateClass)));
            }},
            oldVersion
        );

        List<String> classes = new ArrayList<>();
        classes.add(MyEvent.class.getCanonicalName());
        List<String> loadedClasses = new ArrayList<>();

        sut.loadEventsByClassNames(classes, eventWithMetaData -> {
            assertEquals(MyEvent.class.getCanonicalName(), eventWithMetaData.event.getClass().getCanonicalName());
            loadedClasses.add(eventWithMetaData.event.getClass().getCanonicalName());
            return true;
        });
        assertEquals(2, loadedClasses.size());
        assertEquals(2, sut.countEventsByClassNames(classes));
    }

    @Test
    public void withNestedEvent() throws SQLException, NoSuchAlgorithmException, StorageException {
        SqlEventStore sut = new SqlEventStore(connection, "test1");

        sut.dropStore();
        sut.createStore();

        final AggregateDescriptor aggDsc123 = new AggregateDescriptor("123", "aggregateClass");
        int oldVersion = sut.loadEventsForAggregate(aggDsc123, eventWithMetaData -> true);

        ArrayList<EventWithMetaData> someEvents = new ArrayList<EventWithMetaData>() {{
            add(new EventWithMetaData(new WithNestedEvent("aa", factoryEvent()), new MetaData(LocalDateTime.now(), aggDsc123.aggregateId, aggDsc123.aggregateClass)));
        }};
        sut.appendEventsForAggregate(aggDsc123, someEvents, oldVersion);

        sut.loadEventsForAggregate(aggDsc123, eventWithMetaData -> {
            assertEquals(WithNestedEvent.class.getCanonicalName(), eventWithMetaData.event.getClass().getCanonicalName());
            return true;
        });
    }


    @Test
    public void appendEventsForAggregateShouldFailOnConcurrent() throws SQLException, NoSuchAlgorithmException, StorageException {
        SqlEventStore sut = new SqlEventStore(connection, "test1");

        sut.dropStore();
        sut.createStore();

        assertThrows(ConcurrentModificationException.class, () -> {
            final AggregateDescriptor aggDsc123 = new AggregateDescriptor("123", "aggregateClass");

            int oldVersion = sut.loadEventsForAggregate(aggDsc123, eventWithMetaData -> true);
            assertEquals(-1, oldVersion);

            ArrayList<EventWithMetaData> someEvents = new ArrayList<EventWithMetaData>() {{
                add(new EventWithMetaData(factoryEvent(), new MetaData(LocalDateTime.now(), aggDsc123.aggregateId, aggDsc123.aggregateClass)));
            }};

            sut.appendEventsForAggregate(aggDsc123, someEvents, oldVersion);
            sut.appendEventsForAggregate(aggDsc123, someEvents, oldVersion);
        });
    }


    private MyEvent factoryEvent() {
        return new MyEvent("aa", "bb", 12);
    }

}

class MyEvent implements Event {
    final public String a;
    final private String b;
    final private int c;

    public MyEvent(String a, String b, int c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    public String getB() {
        return b;
    }

    public int getC() {
        return c;
    }
}

class WithNestedEvent implements Event {
    final public String a;
    final private MyEvent nested;

    WithNestedEvent(String a, MyEvent nested) {
        this.a = a;
        this.nested = nested;
    }
}