package org.umlg.sqlg;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Test appender to capture some explicit messages
 *
 * @author JP Moresmau
 */
public class TestAppender extends AppenderSkeleton {
    private static final LinkedList<LoggingEvent> eventsList = new LinkedList<>();

    public static LinkedList<LoggingEvent> getEventsList() {
        return eventsList;
    }

    /**
     * get last event of the given name AND CLEARS THE LIST
     *
     * @param name the event name (class where event was logged)
     * @return the event or null if none
     */
    public static LoggingEvent getLast(String name) {
        synchronized (eventsList) {
            if (eventsList.isEmpty()) {
                return null;
            }
            LoggingEvent evt = eventsList.removeLast();
            while (evt != null && !evt.getLoggerName().equals(name)) {
                evt = eventsList.removeLast();
            }
            eventsList.clear();
            return evt;
        }
    }

    /**
     * get all events of the given name AND CLEARS THE LIST
     *
     * @param name the event name (class where event was logged)
     * @return the event or null if none
     */
    public static List<LoggingEvent> getAll(String name) {
        List<LoggingEvent> result = new ArrayList<>();
        synchronized (eventsList) {
            if (eventsList.isEmpty()) {
                return null;
            }
            for (LoggingEvent evt : eventsList) {
                if (evt.getLoggerName().equals(name)) {
                    result.add(evt);
                }
            }
            eventsList.clear();
            return result;
        }
    }

    public TestAppender() {

    }

    public TestAppender(boolean isActive) {
        super(isActive);
    }

    @Override
    public void close() {

    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    @Override
    protected void append(LoggingEvent event) {
        synchronized (eventsList) {
            eventsList.add(event);
            // keep memory low, since we want the last event usually anyway
            if (eventsList.size() > 10) {
                eventsList.removeFirst();
            }
        }
    }

}
