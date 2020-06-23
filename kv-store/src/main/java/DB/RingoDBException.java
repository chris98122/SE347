package DB;

public class RingoDBException extends Exception {
    public RingoDBException() {
        super();
    }

    public RingoDBException(String detailMessage) {
        super(detailMessage);
    }

    public static class NoSnapshot extends RingoDBException {
        public NoSnapshot() {
            super();
        }
    }
}

