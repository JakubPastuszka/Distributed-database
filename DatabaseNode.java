import java.net.*;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.CountDownLatch;

public class DatabaseNode extends Thread{

    private HashMap<Integer, Integer> database;
    private ArrayList<Integer> connectedPorts;
    private int portNumber;

    public DatabaseNode(int port, Map<Integer, Integer> initialRecords) {
        this.portNumber = port;
        this.database = new HashMap<>(initialRecords);
        this.connectedPorts = new ArrayList<>();
    }



    public void addConnection(int port) {
        connectedPorts.add(port);
    }

    private void establishConnection(String address, int port) {
        try (Socket socket = new Socket(address, port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            out.println("connected serwer " + this.portNumber);
        } catch (IOException e) {
            System.err.println("Could not establish connection to " + address + ":" + port);
        }
    }

    public void startServer2() {
        for (int port : connectedPorts) {
            try (Socket socket = new Socket("localhost", port);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                out.println("addPort " + this.portNumber);
            } catch (IOException e) {
                System.err.println("Could not connect to server at port: " + port);
            }
        }

        try (ServerSocket serverSocket = new ServerSocket(this.portNumber)) {
            System.out.println("DatabaseNode is running on port " + this.portNumber);

            while (true) {
                try (Socket clientSocket = serverSocket.accept();
                     PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                     BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {

                    System.out.println("Client connected");

                    String inputLine;
                    while ((inputLine = in.readLine()) != null) {
                        String outputLine = processCommand(inputLine);
                        out.println(outputLine);
                    }
                } catch (IOException e) {
                    System.err.println("Exception in communication with a client: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Could not listen on port " + this.portNumber);
        }
    }

    public class MaxValueThread extends Thread {

        private volatile int resultMaxKey;
        private volatile int resultMaxValue;
        private final int maxKey;
        private final int maxValue;
        private final int nextServerPort;
        private final CountDownLatch latch;
        private final int senderPort;

        public MaxValueThread(int maxKey, int maxValue, int nextServerPort, CountDownLatch latch, int senderPort) {
            this.maxKey = maxKey;
            this.maxValue = maxValue;
            this.nextServerPort = nextServerPort;
            this.latch = latch;
            this.senderPort = senderPort;
        }
        public int getResultMaxKey() {
            return resultMaxKey;
        }

        public int getResultMaxValue() {
            return resultMaxValue;
        }



        @Override
        public void run() {
            System.out.println("MaxValueThread started for port " + nextServerPort);
            try (Socket socket = new Socket("localhost", nextServerPort);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                // Send request to next server with updated visited ports
                out.println("get-max " + maxKey + " " + maxValue + " " + this.senderPort);

                // Read response from the next server
                String response = in.readLine();
                System.out.println("Response from server " + nextServerPort + ": " + response);

                String[] responseParts = response.split(":");
                if (responseParts.length == 2) {
                    this.resultMaxKey = Integer.parseInt(responseParts[0]);
                    this.resultMaxValue = Integer.parseInt(responseParts[1]);
                }
            } catch (IOException e) {
                System.err.println("Error in MaxValueThread: " + e.getMessage());
            } finally {
                latch.countDown();
            }
            System.out.println("MaxValueThread finished for port " + nextServerPort);
        }
    }
    public class MinValueThread extends Thread {

        private volatile int resultMinKey;
        private volatile int resultMinValue;
        private final int minKey;
        private final int minValue;
        private final int nextServerPort;
        private final CountDownLatch latch;
        private final int senderPort;

        public MinValueThread(int minKey, int minValue, int nextServerPort, CountDownLatch latch, int senderPort) {
            this.minKey = minKey;
            this.minValue = minValue;
            this.nextServerPort = nextServerPort;
            this.latch = latch;
            this.senderPort = senderPort;
        }
        public int getResultMinKey() {
            return resultMinKey;
        }

        public int getResultMinValue() {
            return resultMinValue;
        }

        @Override
        public void run() {
            System.out.println("MinValueThread started for port " + nextServerPort);
            try (Socket socket = new Socket("localhost", nextServerPort);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                // Send request to next server with updated visited ports
                out.println("get-min " + minKey + " " + minValue + " " + this.senderPort);

                // Read response from the next server
                String response = in.readLine();
                System.out.println("Response from server " + nextServerPort + ": " + response);

                String[] responseParts = response.split(":");
                if (responseParts.length == 2) {
                    this.resultMinKey = Integer.parseInt(responseParts[0]);
                    this.resultMinValue = Integer.parseInt(responseParts[1]);
                }
            } catch (IOException e) {
                System.err.println("Error in MinValueThread: " + e.getMessage());
            } finally {
                latch.countDown();
            }
            System.out.println("MinValueThread finished for port " + nextServerPort);
        }
    }
    public class GetValueThread extends Thread {
        private volatile int resultKey;
        private volatile int resultValue;
        private final int searchKey;
        private final int nextServerPort;
        private final CountDownLatch latch;
        private final int senderPort;

        public GetValueThread(int searchKey, int nextServerPort, CountDownLatch latch, int senderPort) {
            this.searchKey = searchKey;
            this.nextServerPort = nextServerPort;
            this.latch = latch;
            this.senderPort = senderPort;
        }

        public int getResultKey() {
            return resultKey;
        }

        public int getResultValue() {
            return resultValue;
        }

        @Override
        public void run() {
            System.out.println("GetValueThread started for port " + nextServerPort);
            try (Socket socket = new Socket("localhost", nextServerPort);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                // Send request to next server
                out.println("get-value " + searchKey + " " + this.senderPort);

                // Read response from the next server
                String response = in.readLine();
                System.out.println("Response from server " + nextServerPort + ": " + response);

                String[] responseParts = response.split(":");
                if (responseParts.length == 2) {
                    this.resultKey = Integer.parseInt(responseParts[0]);
                    this.resultValue = Integer.parseInt(responseParts[1]);
                }
            } catch (IOException e) {
                System.err.println("Error in GetValueThread: " + e.getMessage());
            } finally {
                latch.countDown();
            }
            System.out.println("GetValueThread finished for port " + nextServerPort);
        }
    }

    public class FindKeyThread extends Thread {
        private volatile int resultPort;
        private final int searchKey;
        private final int nextServerPort;
        private final CountDownLatch latch;
        private final int senderPort;

        public FindKeyThread(int searchKey, int nextServerPort, CountDownLatch latch, int senderPort) {
            this.searchKey = searchKey;
            this.nextServerPort = nextServerPort;
            this.latch = latch;
            this.senderPort = senderPort;
            this.resultPort = -1; // Zainicjowanie resultPort
        }

        public int getResultPort() {
            return resultPort;
        }

        @Override
        public void run() {
            System.out.println("FindKeyThread started for port " + nextServerPort);
            try (Socket socket = new Socket("localhost", nextServerPort);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                out.println("find-key " + searchKey + " " + this.senderPort);

                String response = in.readLine();
                System.out.println("Response from server " + nextServerPort + ": " + response);
                if (response.startsWith("localhost:")) {
                    try {
                        this.resultPort = Integer.parseInt(response.split(":")[1]);
                    } catch (NumberFormatException e) {
                        this.resultPort = -1; // W przypadku błędu formatowania
                    }
                } else {
                    this.resultPort = -1; // W przypadku braku prefiksu 'localhost:'
                }
            } catch (IOException e) {
                System.err.println("Error in FindKeyThread: " + e.getMessage());
                this.resultPort = -1; // Ustawienie -1 w przypadku błędu IO
            } finally {
                latch.countDown();
            }
            System.out.println("FindKeyThread Finished for port " + nextServerPort);
        }
    }

    public class SetValueThread extends Thread {
        private volatile boolean success;
        private final int searchKey;
        private final int newValue;
        private final int nextServerPort;
        private final CountDownLatch latch;
        private final int senderPort;

        public SetValueThread(int searchKey, int newValue, int nextServerPort, CountDownLatch latch, int senderPort) {
            this.searchKey = searchKey;
            this.newValue = newValue;
            this.nextServerPort = nextServerPort;
            this.latch = latch;
            this.senderPort = senderPort;
        }

        public boolean isSuccess() {
            return success;
        }

        @Override
        public void run() {
            System.out.println("SetValueThread started for port " + nextServerPort);
            try (Socket socket = new Socket("localhost", nextServerPort);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                out.println("set-value " + searchKey + ":" + newValue + " " + this.senderPort);

                String response = in.readLine();
                System.out.println("Response from server " + nextServerPort + ": " + response);

                success = "OK".equals(response);
            } catch (IOException e) {
                System.err.println("Error in SetValueThread: " + e.getMessage());
            } finally {
                latch.countDown();
            }
            System.out.println("SetValueThread finished for port " + nextServerPort);
        }
    }
    private void informNeighborsAboutTermination() {
        for (int port : connectedPorts) {
            try (Socket socket = new Socket("localhost", port);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
                out.println("removePort " + this.portNumber);
            } catch (IOException e) {
                System.err.println("Could not inform neighbor " + port + " about termination");
            }
        }
    }

    private String processCommand(String command) {
        String[] parts = command.split(" ");
        switch (parts[0]) {

            case "set-value":
                if (parts.length < 2) return "invalid operation argument";
                String[] keyValue = parts[1].split(":");
                if (keyValue.length != 2) return "invalid operation argument";

                int keyToSet, newValue;
                try {
                    keyToSet = Integer.parseInt(keyValue[0]);
                    newValue = Integer.parseInt(keyValue[1]);
                } catch (NumberFormatException e) {
                    return "invalid operation argument";
                }

                int senderPortSetValue = -1;
                try {
                    if (parts.length > 2) {
                        senderPortSetValue = Integer.parseInt(parts[2]);
                    }
                } catch (NumberFormatException e) {
                    return "invalid operation argument";
                }

                if (database.containsKey(keyToSet)) {
                    database.put(keyToSet, newValue);
                    return "OK";
                }

                if (!connectedPorts.isEmpty()) {
                    CountDownLatch latch = new CountDownLatch(connectedPorts.size());
                    List<SetValueThread> threads = new ArrayList<>();

                    for (int port : connectedPorts) {
                        if (port != this.portNumber && port != senderPortSetValue) {
                            SetValueThread setValueThread = new SetValueThread(keyToSet, newValue, port, latch, this.portNumber);
                            threads.add(setValueThread);
                            setValueThread.start();
                        } else {
                            latch.countDown();
                        }
                    }

                    try {
                        latch.await();
                        for (SetValueThread thread : threads) {
                            if (thread.isSuccess()) {
                                return "OK";
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return "Interrupted while waiting for response";
                    }
                }
                return "ERROR";

            case "get-value":
                if (parts.length < 2) return "invalid operation argument";
                int searchKey;
                int senderPortValue = -1;

                try {
                    searchKey = Integer.parseInt(parts[1]);
                    if (parts.length > 2) {
                        senderPortValue = Integer.parseInt(parts[2]);
                    }
                } catch (NumberFormatException e) {
                    return "invalid operation argument";
                }

                int value = database.getOrDefault(searchKey, -1);

                if (value != -1) {
                    return searchKey + ":" + value;
                }

                if (!connectedPorts.isEmpty()) {
                    CountDownLatch latch = new CountDownLatch(connectedPorts.size());
                    List<GetValueThread> threads = new ArrayList<>();

                    for (int port : connectedPorts) {
                        if (port != this.portNumber && port != senderPortValue) {
                            GetValueThread getValueThread = new GetValueThread(searchKey, port, latch, this.portNumber);
                            threads.add(getValueThread);
                            getValueThread.start();
                        } else {
                            latch.countDown();
                        }
                    }

                    try {
                        latch.await();
                        for (GetValueThread thread : threads) {
                            if (thread.getResultKey() == searchKey) {
                                return searchKey + ":" + thread.getResultValue();
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return "Interrupted while waiting for response";
                    }
                }
                return "ERROR";

            case "find-key":
                if (parts.length < 2) return "ERROR";
                int searchKeyForFind;
                try {
                    searchKeyForFind = Integer.parseInt(parts[1]);
                } catch (NumberFormatException e) {
                    return "invalid operation argument";
                }

                int senderPortForKeyFind = -1;
                try {
                    if (parts.length > 2) {
                        senderPortForKeyFind = Integer.parseInt(parts[2]);
                    }
                } catch (NumberFormatException e) {
                    return "invalid operation argument"; // Zwróć błąd, gdy port nie jest liczbą
                }

                if (database.containsKey(searchKeyForFind)) {
                    return "localhost:" + this.portNumber; // Dodanie 'localhost:'
                }

                if (!connectedPorts.isEmpty()) {
                    CountDownLatch latch = new CountDownLatch(connectedPorts.size());
                    List<FindKeyThread> threads = new ArrayList<>();

                    for (int port : connectedPorts) {
                        if (port != this.portNumber && port != senderPortForKeyFind) {
                            FindKeyThread findKeyThread = new FindKeyThread(searchKeyForFind, port, latch, this.portNumber);
                            threads.add(findKeyThread);
                            findKeyThread.start();
                        } else {
                            latch.countDown();
                        }
                    }

                    try {
                        latch.await();
                        for (FindKeyThread thread : threads) {
                            if (thread.getResultPort() != -1) {
                                return "localhost:" + thread.getResultPort(); // Dodanie 'localhost:'
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return "Interrupted while waiting for response";
                    }
                }
                return "Key not found";

            case "get-max":
                int maxKey = -1;
                int maxValue = Integer.MIN_VALUE;
                int senderPortMax = -1;

                try {
                    if (parts.length > 3) {
                        senderPortMax = Integer.parseInt(parts[3]);
                    }
                } catch (NumberFormatException e) {
                    return "invalid operation argument";
                }

                if (!database.isEmpty()) {
                    maxKey = Collections.max(database.entrySet(), Map.Entry.comparingByValue()).getKey();
                    maxValue = database.get(maxKey);
                }

                if (!connectedPorts.isEmpty()) {
                    CountDownLatch latch = new CountDownLatch(connectedPorts.size());
                    List<MaxValueThread> threads = new ArrayList<>();

                    for (int port : connectedPorts) {
                        if (port != this.portNumber && port != senderPortMax) {
                            MaxValueThread maxValueThread = new MaxValueThread(maxKey, maxValue, port, latch, this.portNumber);
                            threads.add(maxValueThread);
                            maxValueThread.start();
                        } else {
                            latch.countDown(); // Zmniejszamy licznik, jeśli port jest pomijany
                        }
                    }

                    try {
                        latch.await(); // Oczekiwanie na zakończenie wszystkich wątków
                        for (MaxValueThread thread : threads) {
                            if (thread.getResultMaxValue() > maxValue) {
                                maxKey = thread.getResultMaxKey();
                                maxValue = thread.getResultMaxValue();
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return "Interrupted while waiting for response";
                    }
                }
                return maxKey + ":" + maxValue;
            case "get-min":
                int minKey = -1;
                int minValue = Integer.MAX_VALUE;
                int senderPortMIN = -1;

                try {
                    if (parts.length > 3) {
                        senderPortMIN = Integer.parseInt(parts[3]);
                    }
                } catch (NumberFormatException e) {
                    return "invalid operation argument";
                }

                if (!database.isEmpty()) {
                    minKey = Collections.min(database.entrySet(), Map.Entry.comparingByValue()).getKey();
                    minValue = database.get(minKey);
                }

                if (!connectedPorts.isEmpty()) {
                    CountDownLatch latch = new CountDownLatch(connectedPorts.size());
                    List<MinValueThread> threads = new ArrayList<>();

                    for (int port : connectedPorts) {
                        if (port != this.portNumber && port != senderPortMIN) {
                            MinValueThread minValueThread = new MinValueThread(minKey, minValue, port, latch, this.portNumber);
                            threads.add(minValueThread);
                            minValueThread.start();
                        } else {
                            latch.countDown();
                        }
                    }

                    try {
                        latch.await();
                        for (MinValueThread thread : threads) {
                            if (thread.getResultMinValue() < minValue) {
                                minKey = thread.getResultMinKey();
                                minValue = thread.getResultMinValue();
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return "Interrupted while waiting for response";
                    }
                }
                return minKey + ":" + minValue;


            case "new-record":
                if (parts.length != 2) return "ERROR";
                String[] keyValue2 = parts[1].split(":");
                if (keyValue2.length != 2) return "ERROR";
                try {
                    Integer newKeyRecord = Integer.parseInt(keyValue2[0]);
                    Integer newValueRecord = Integer.parseInt(keyValue2[1]);


                    if (!database.containsKey(newKeyRecord)) {

                        database.put(newKeyRecord, newValueRecord);
                        return "OK";
                    } else {

                        return "ERROR";
                    }
                } catch (NumberFormatException e) {
                    return "ERROR";
                }

            case "showDataBase":
                if (database.isEmpty()) return "Database is empty";
                return database.entrySet().stream()
                        .map(entry -> entry.getKey() + ":" + entry.getValue())
                        .collect(Collectors.joining(", "));

            case "showConnectedPorts":
                if (connectedPorts.isEmpty()) return "No connected ports";
                return "Connected ports: " + connectedPorts.stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(", "));

            case "terminate":

                informNeighborsAboutTermination(); // Informowanie sąsiednich węzłów
                // Zakończenie działania serwera
                new Thread(() -> {
                    try {
                        Thread.sleep(500); // Krótkie opóźnienie
                        System.out.println("Node " + this.portNumber + " is terminating.");
                        System.exit(0);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }).start();
                return "OK";
            default:
                return "Invalid command";

            case "removePort":
                if (parts.length != 2) return "ERROR";
                try {
                    int portToRemove = Integer.parseInt(parts[1]);
                    connectedPorts.remove(Integer.valueOf(portToRemove));
                    return "Port " + portToRemove + " removed";
                } catch (NumberFormatException e) {
                    return "ERROR";
                }

            case "addPort":
                if (parts.length != 2) return "ERROR";
                try {
                    int portToAdd = Integer.parseInt(parts[1]);
                    connectedPorts.add(portToAdd);
                    return "Port " + portToAdd + " added";
                } catch (NumberFormatException e) {
                    return "ERROR";
                }


        }
    }

    public static void main(String[] args) {
        int portNumber = 0;
        ArrayList<Integer> connections = new ArrayList<>();
        Map<Integer, Integer> initialRecords = new HashMap<>();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-tcpport":
                    if (i + 1 < args.length) portNumber = Integer.parseInt(args[++i]);
                    break;
                case "-connect":
                    if (i + 1 < args.length) {
                        String[] hostPort = args[++i].split(":");
                        if (hostPort.length == 2) {
                            connections.add(Integer.parseInt(hostPort[1]));
                        }
                    }
                    break;
                case "-record":
                    if (i + 1 < args.length) {
                        String[] keyValue = args[++i].split(":");
                        if (keyValue.length == 2) {
                            int key = Integer.parseInt(keyValue[0]);
                            int value = Integer.parseInt(keyValue[1]);
                            initialRecords.put(key, value);
                        }
                    }
                    break;
            }
        }

        if (portNumber == 0) {
            System.err.println("Usage: java DatabaseNode -tcpport <port number> [-connect <host:port> ...] [-record <key:value> ...]");
            System.exit(1);
        }

        DatabaseNode node = new DatabaseNode(portNumber, initialRecords);

        for (int port : connections) {
            node.establishConnection("localhost", port);
            node.addConnection(port);
        }
        node.startServer2();
    }
}