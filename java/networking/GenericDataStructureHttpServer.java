package networking;

import com.sun.net.httpserver.*;

import java.io.*;
import java.lang.reflect.*;
import java.net.*;
import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.*;

public class GenericDataStructureHttpServer<T> {
    private final HttpServer server;
    private final T wrappedObject;
    private final Map<String, String> methods;

    public GenericDataStructureHttpServer(T wrappedObject, int port, Map<String, String> methods) throws IOException {
        this.wrappedObject = wrappedObject;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(10));
        this.methods = methods;

        createEndpoints();

    }

    public GenericDataStructureHttpServer(T wrappedObject, int port) throws IOException {
        this(wrappedObject, port, null);
    }

    private void createEndpoints() {
        Method[] methods = wrappedObject.getClass().getDeclaredMethods();

        for (Method method : methods) {
            if (method.getDeclaringClass() != Object.class) {
                // skip Object methods
                if (isObjectMethod(method))
                    continue;
                // skip static methods
                if (Set.of(method.getModifiers()).contains(9))
                    continue;

                // skip lambda methods
                if (method.getName().contains("lambda$"))
                    continue;

                String path = "/" + method.getName();
                server.createContext(path, new MethodHandler(method));
                System.out.println("Created endpoint: " + path);
            }
        }
        CurlCommandGenerator.generateBashFile(wrappedObject.getClass(), server.getAddress().getPort(), this.methods);
    }

    public void start() {
        server.start();
        System.out.println("Server started on port " + server.getAddress().getPort());
    }

    private class MethodHandler implements HttpHandler {
        private final Method method;

        public MethodHandler(Method method) {
            this.method = method;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                Map<String, String> params = parseQuery(exchange.getRequestURI().getQuery());
                Object[] args = matchParameters(params);
                Object result = method.invoke(wrappedObject, args);
                sendResponse(exchange, 200, result != null ? result.toString() : "null");
            } catch (IllegalAccessException | InvocationTargetException e) {
                sendResponse(exchange, 500, "Error invoking method: " + e.getMessage());
            } catch (IllegalArgumentException e) {
                sendResponse(exchange, 400, "Invalid arguments: " + e.getMessage());
            }
        }

        private Object[] matchParameters(Map<String, String> params) {
            Class<?>[] parameterTypes = method.getParameterTypes();
            Object[] args = new Object[parameterTypes.length];

            for (int i = 0; i < parameterTypes.length; i++) {
                String paramName = "arg" + (i + 1);
                String paramValue = params.get(paramName);
                if (paramValue == null) {
                    throw new IllegalArgumentException("Missing parameter: " + paramName);
                }

                args[i] = convertParameter(paramValue, parameterTypes[i]);
            }

            return args;
        }

        private Object convertParameter(String value, Class<?> type) {
            if (type == String.class)
                return value;
            else if (type == int.class || type == Integer.class)
                return Integer.parseInt(value);
            else if (type == long.class || type == Long.class)
                return Long.parseLong(value);
            else if (type == double.class || type == Double.class)
                return Double.parseDouble(value);
            else if (type == boolean.class || type == Boolean.class)
                return Boolean.parseBoolean(value);

            throw new IllegalArgumentException("Unsupported parameter type: " + type.getSimpleName());
        }

    }

    private Map<String, String> parseQuery(String query) {
        Map<String, String> result = new HashMap<>();

        if (query != null)
            for (String param : query.split("&")) {
                String[] entry = param.split("=");
                if (entry.length > 1)
                    result.put(entry[0], URLDecoder.decode(entry[1], StandardCharsets.UTF_8));
                else
                    result.put(entry[0], "");
            }

        return result;
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        exchange.sendResponseHeaders(statusCode, response.length());
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response.getBytes());
        }
    }

    // WIP, need to filter out Object methods
    private static class CurlCommandGenerator {
        public static void generateBashFile(Class<?> clazz, int port, Map<String, String> methods) {
            String filename = "./bin/" + clazz.getSimpleName() + "_curl_commands.sh";
            try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
                writer.println("#!/bin/bash");
                writer.println();

                Method[] declared = clazz.getDeclaredMethods();
                for (Method method : declared) {
                    // Skip Object methods
                    if (isObjectMethod(method))
                        continue;
                    // skip static methods
                    if (Set.of(method.getModifiers()).contains(9))
                        continue;

                    // skip lambda methods
                    if (method.getName().contains("lambda$"))
                        continue;

                    if (method.getDeclaringClass() != Object.class) {
                        String curlCommand = generateCurlCommand(method, port, methods);
                        writer.println("# " + method.getName());
                        writer.println(curlCommand);
                        writer.println();
                    }
                }

                System.out.println("Curl commands bash file generated: " + filename);
            } catch (IOException e) {
                System.err.println("Error generating curl commands file: " + e.getMessage());
            }
        }

        private static String generateCurlCommand(Method method, int port, Map<String, String> methods) {
            StringBuilder command;

            if (methods != null && methods.containsKey(method.getName())) {
                // default to string data type for post requests
                switch (methods.get(method.getName())) {
                    case "POST":
                        command = new StringBuilder("curl -X POST -d \"");
                        break;
                    case "PUT":
                        command = new StringBuilder("curl -X PUT -d \"");
                        break;
                    default:
                        command = new StringBuilder("curl -X GET \"");
                        break;
                }
            } else command = new StringBuilder("curl -X GET \"");

            command.append(port).append("/").append(method.getName());

            Parameter[] parameters = method.getParameters();
            if (parameters.length > 0) {
                command.append("?");
                for (int i = 0; i < parameters.length; i++) {
                    if (i > 0) command.append("&");
                    command.append("arg")
                            .append(i + 1)
                            .append("=<")
                            .append(parameters[i].getType().getSimpleName())
                            .append(">");
                }
            }

            command.append("\"");
            return command.toString();
        }
    }

    private static boolean isObjectMethod(Method method) {
        for (Method objectMethod : Object.class.getDeclaredMethods())
            if (objectMethod.getName().equals(method.getName()))
                return true;
        return false;
    }
}