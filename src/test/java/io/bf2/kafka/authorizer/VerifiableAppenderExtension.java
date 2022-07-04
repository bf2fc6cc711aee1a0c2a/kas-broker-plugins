package io.bf2.kafka.authorizer;

import com.google.common.collect.Lists;
import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

//based on/inspired by https://www.dontpanicblog.co.uk/2018/01/15/test-log4j-with-junit/
// Which is in turn based on the MIT licensed https://github.com/hotblac/voicexmlriot/blob/voicexmlriot-0.1.0/src/test/java/org/vxmlriot/jvoicexml/junit/LogAppenderResource.java
public class VerifiableAppenderExtension implements BeforeEachCallback, AfterEachCallback, ParameterResolver {
    public static final String LOG_EVENTS_STORE_KEY = "logEvents";
    public  final String appenderName;
    private final ExtensionContext.Namespace namespace;

    public VerifiableAppenderExtension() {
        namespace = ExtensionContext.Namespace.create(VerifiableAppenderExtension.class);
        appenderName = "VerifiableAppender-" + Instant.now();
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) {
        extensionContext.getStore(namespace).remove(LOG_EVENTS_STORE_KEY, List.class);
        Logger.getRootLogger().removeAppender(appenderName);
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) {
        final ArrayList<LoggingEvent> logEvents = Lists.newArrayList();
        Appender appender = new CollectingAppender(logEvents, appenderName);
        Logger.getRootLogger().addAppender(appender);
        extensionContext.getStore(namespace).put(LOG_EVENTS_STORE_KEY, logEvents);
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.findAnnotation(LoggedEvents.class).isPresent();
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return extensionContext.getStore(namespace).get(LOG_EVENTS_STORE_KEY, List.class);
    }


    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.PARAMETER)
    public @interface LoggedEvents {
    }

    private static class CollectingAppender implements Appender {
        private final ArrayList<LoggingEvent> logEvents;
        private String appenderName;

        public CollectingAppender(ArrayList<LoggingEvent> logEvents, String appenderName) {
            this.logEvents = logEvents;
            this.appenderName = appenderName;
        }

        @Override
        public void addFilter(Filter filter) {

        }

        @Override
        public Filter getFilter() {
            return null;
        }

        @Override
        public void clearFilters() {

        }

        @Override
        public void close() {

        }

        @Override
        public void doAppend(LoggingEvent loggingEvent) {
            logEvents.add(loggingEvent);
        }

        @Override
        public String getName() {
            return appenderName;
        }

        @Override
        public void setErrorHandler(ErrorHandler errorHandler) {

        }

        @Override
        public ErrorHandler getErrorHandler() {
            return null;
        }

        @Override
        public void setLayout(Layout layout) {

        }

        @Override
        public Layout getLayout() {
            return null;
        }

        @Override
        public void setName(String s) {
            appenderName = s;
        }

        @Override
        public boolean requiresLayout() {
            return false;
        }
    }
}
