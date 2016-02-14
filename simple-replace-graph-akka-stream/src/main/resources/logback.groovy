import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.core.rolling.FixedWindowRollingPolicy
import ch.qos.logback.core.rolling.RollingFileAppender
import ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy

import static ch.qos.logback.classic.Level.INFO

appender("RootFileAppender", RollingFileAppender) {
  file = "logs/application.log"
  append = true
  rollingPolicy(FixedWindowRollingPolicy) {
    fileNamePattern = "logs/application.log.%i"
    maxIndex = 21
  }
  triggeringPolicy(SizeBasedTriggeringPolicy) {
    maxFileSize = 1000000
  }
  encoder(PatternLayoutEncoder) {
    pattern = "%d{yyyy-MM-dd HH:mm:ss}, %p, %c, %t, %L, %C{1}, %M %m%n"
  }
}
root(INFO, ["RootFileAppender"])
