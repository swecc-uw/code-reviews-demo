package networking;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import datastructures.LSMTree;

public class LSMServer {
    private static final int PORT = 8080;
    private final LSMTree<String, String> lsmTree;
    private final HttpServer server;

    public LSMServer(int maxMemTableSize, int maxLevels) throws IOException {
        this.lsmTree = new LSMTree<>(maxMemTableSize, maxLevels);
        this.server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext("/put", new PutHandler());
        server.createContext("/get", new GetHandler());
        server.setExecutor(Executors.newFixedThreadPool(10));
    }

    public void start() {
        server.start();
        System.out.println("LSMServer started on port " + PORT);
    }

    private class PutHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "Method Not Allowed");
                return;
            }

            try {
                String body = new String(exchange.getRequestBody().readAllBytes());
                String[] parts = body.split("=", 2);
                if (parts.length != 2) {
                    sendResponse(exchange, 400, "Bad Request");
                    return;
                }

                String key = parts[0];
                String value = parts[1];
                lsmTree.put(key, value);
                sendResponse(exchange, 200, "OK");
            } catch (Exception e) {
                System.out.printf("Error: %s\n", e.getMessage());
                sendResponse(exchange, 500, "Internal Server Error");
            }
        }
    }

    private class GetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "Method Not Allowed");
                return;
            }

            try {
                String query = exchange.getRequestURI().getQuery();

                if (query == null) {
                    sendResponse(exchange, 400, "Bad Request");
                    return;
                }

                String[] parts = query.split("=", 2);
                if (parts.length != 2 || !parts[0].equals("key")) {
                    sendResponse(exchange, 400, "Bad Request");
                    return;
                }

                String key = parts[1];
                String value = lsmTree.get(key);
                if (value != null) {
                    sendResponse(exchange, 200, value);
                } else {
                    sendResponse(exchange, 404, "Not Found");
                }
            } catch (Exception e) {
                System.out.printf("Error: %s\n", e.getMessage());
                sendResponse(exchange, 500, "Internal Server Error");
            }
        }
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        exchange.sendResponseHeaders(statusCode, response.length());
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes());
        }
    }

    /**
     *
     * # Insert a key-value pair
     *  curl -X POST -d "mykey=myvalue" http://localhost:8080/put
     *
     *  # Retrieve a value
     *  curl "http://localhost:8080/get?key=mykey"
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        LSMServer server = new LSMServer(1000, 5);
        server.start();
    }
}