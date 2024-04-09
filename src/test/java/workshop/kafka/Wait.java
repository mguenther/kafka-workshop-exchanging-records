package workshop.kafka;

import java.time.Duration;

final class Wait {

    static void delay(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new AssertionError(e);
        }
    }
}
