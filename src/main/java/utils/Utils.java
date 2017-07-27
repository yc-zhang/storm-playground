package utils;

public class Utils {
    public static void waitForSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException ignored) {
        }
    }

    public static void waitForMillis(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException ignored) {
        }
    }
}
