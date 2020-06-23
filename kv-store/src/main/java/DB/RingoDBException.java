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

    public static class MapNotEmpty extends RingoDBException {
        public MapNotEmpty() {
            super();
        }
    }

    public static class KeyEmpty extends RingoDBException {
        public KeyEmpty() {
            super();
        }
    }

    public static class KeyNotExists extends RingoDBException {
        public KeyNotExists() {
            super();
        }
    }
}

