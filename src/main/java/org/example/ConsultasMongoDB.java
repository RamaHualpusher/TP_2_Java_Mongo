package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.*;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Scanner;
import java.util.regex.Pattern;

public class ConsultasMongoDB {
    public static void main(String[] args) {
        MongoClient mongoClient = null;
        try {
            mongoClient = conectarMongoDB();
            MongoCollection<Document> collection = getCollection(mongoClient);

            Scanner scanner = new Scanner(System.in);
            boolean continuar = true;

            while (continuar) {
                mostrarMenu();
                String opcion = scanner.nextLine().trim();
                Pattern patronOpcion = Pattern.compile("^[1-9]$");
                if (patronOpcion.matcher(opcion).matches()) {
                    int opcionNumerica = Integer.parseInt(opcion);
                    switch (opcionNumerica) {
                        case 1:
                            consultaRegionAmericas(collection);
                            break;
                        case 2:
                            consultaRegionAmericasPoblacionMayorA100Millones(collection);
                            break;
                        case 3:
                            consultaRegionDistintaDeAfrica(collection);
                            break;
                        case 4:
                            actualizarEgypt(collection);
                            break;
                        case 5:
                            System.out.println("Ingrese el código del país a eliminar:");
                            String codigoStr = scanner.nextLine().trim();
                            Pattern patronCodigo = Pattern.compile("^[a-zA-Z]{3}$");
                            if (patronCodigo.matcher(codigoStr).matches()) {
                                String codigo = codigoStr.toUpperCase();
                                eliminarPaisPorCodigo(collection, codigo);
                            } else {
                                System.out.println("Código de país no válido. Intente de nuevo.");
                            }
                            break;
                        case 6:
                            consultaPoblacionEntreRango(collection);
                            break;
                        case 7:
                            consultaOrdenadaPorNombre(collection);
                            break;
                        case 8:
                            System.out.println("Ingrese la expresión regular para la búsqueda:");
                            String regex = scanner.nextLine().trim();
                            consultaPorNombreUsandoRegex(collection, regex);
                            break;
                        case 9:
                            continuar = false;
                            System.out.println("Saliendo...");
                            break;
                    }
                } else {
                    System.out.println("Opción no válida. Intente de nuevo.");
                }
            }

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            System.out.println("Fin del programa");
            if (mongoClient != null) {
                mongoClient.close();
            }
        }
    }

    public static void mostrarMenu() {
        System.out.println("\nMenú de opciones:");
        System.out.println("1. Seleccionar documentos donde la región sea Americas");
        System.out.println("2. Seleccionar documentos donde la región sea Americas y la población sea mayor a 100000000");
        System.out.println("3. Seleccionar documentos donde la región sea distinta de Africa");
        System.out.println("4. Actualizar el documento donde el nombre sea Egypt");
        System.out.println("5. Eliminar el documento donde el código del país sea ingresado");
        System.out.println("6. Seleccionar documentos donde la población sea mayor a 5000000000 y menor a 150000000");
        System.out.println("7. Seleccionar documentos ordenados por nombre en forma ascendente");
        System.out.println("8. Seleccionar documentos por nombre usando expresiones regulares");
        System.out.println("9. Salir");
        System.out.print("Ingrese el número de la opción que desea ejecutar: ");
    }


    public static MongoClient conectarMongoDB() {
        return MongoClients.create("mongodb://localhost:27017");
    }
    public static MongoCollection<Document> getCollection(MongoClient mongoClient) {
        return mongoClient.getDatabase("paises_db").getCollection("paises");
    }

    // 5.1. Método que selecciona los documentos donde la región sea "Americas"
    public static void consultaRegionAmericas(MongoCollection<Document> collection) {
        Bson filter = Filters.eq("region", "Americas");

        FindIterable<Document> results = collection.find(filter);
        for (Document doc : results) {
            imprimirDocument(doc);
        }
    }

    // 5.2. Método que selecciona los documentos donde la región sea "Americas" y la población sea mayor a 100000000
    public static void consultaRegionAmericasPoblacionMayorA100Millones(MongoCollection<Document> collection) {
        // Crea un filtro compuesto para buscar documentos donde la región sea "Americas" y la población sea mayor a 100 millones
        Bson filter = Filters.and(
                Filters.eq("region", "Americas"),
                Filters.gt("poblacion", 100000000)
        );

        // Realiza la consulta y recorre los resultados para imprimirlos por pantalla
        FindIterable<Document> results = collection.find(filter);
        for (Document doc : results) {
            //System.out.println(doc.toJson());
            imprimirDocument(doc);
        }
    }

    // 5.3. Método que selecciona los documentos donde la región sea distinta de "Africa"
    public static void consultaRegionDistintaDeAfrica(MongoCollection<Document> collection) {
        Bson filter = Filters.ne("region", "Africa");

        FindIterable<Document> results = collection.find(filter);
        for (Document doc : results) {
            imprimirDocument(doc);
        }
    }

    // 5.4. Método que actualiza el documento donde el name sea "Egypt"
    public static void actualizarEgypt(MongoCollection<Document> collection) {
        Bson filter = Filters.eq("nombrePais", "Egypt");
        Bson update = Updates.set("nombrePais", "Egipto");
        UpdateResult result = collection.updateOne(filter, update);
        if (result.getModifiedCount() > 0) {
            System.out.println("Se actualizó el país Egypt a Egipto exitosamente.");
        } else {
            System.out.println("No se encontró ningún país con el nombre Egypt para actualizar.");
        }
    }
    // 5.5. Método que elimina el documento donde el código del país sea 258
    public static void eliminarPaisPorCodigo(MongoCollection<Document> collection, String codigo) {
        Bson filter = Filters.eq("codigoPais", codigo);
        DeleteResult result = collection.deleteOne(filter);
        if (result.getDeletedCount() > 0) {
            System.out.println("El país con código " + codigo + " fue eliminado exitosamente.");
        } else {
            System.out.println("No se encontró ningún país con el código " + codigo + ".");
        }
    }
    // 5.6. Método drop() elimina una colección completa o una base de datos. No se proporciona código ya que solo se solicita la descripción.

    // 5.7. Método que selecciona los documentos donde la población sea mayor a 50000000 y menor a 150000000
    public static void consultaPoblacionEntreRango(MongoCollection<Document> collection) {
        Bson filter = Filters.and(
                Filters.gt("poblacion", 50000000),
                Filters.lt("poblacion", 150000000)
        );

        FindIterable<Document> results = collection.find(filter);
        for (Document doc : results) {
            imprimirDocument(doc);
        }
    }
    // 5.8. Método que selecciona los documentos ordenados por nombre (name) en forma Ascendente
    public static void consultaOrdenadaPorNombre(MongoCollection<Document> collection) {
        FindIterable<Document> results = collection.find().sort(Sorts.ascending("nombrePais"));
        for (Document doc : results) {
            imprimirDocument(doc);
        }
    }

    // 5.9. Método skip() omite los primeros n documentos en una consulta. No se proporciona código ya que solo se solicita la descripción.

    // 5.10. Método que utiliza expresiones regulares en Mongo para simular el uso de la cláusula LIKE de SQL
    public static void consultaPorNombreUsandoRegex(MongoCollection<Document> collection, String regex) {
        Bson filter = Filters.regex("nombrePais", regex);

        FindIterable<Document> results = collection.find(filter);
        for (Document doc : results) {
            imprimirDocument(doc);
        }
    }
    /*Para usar este método, puedes llamarlo con una expresión regular, por ejemplo, para encontrar todos los países cuyo nombre comience con "A":

    consultaPorNombreUsandoRegex(collection, "^A");
    */

    // 5.11. Método que crea un nuevo índice para la colección países asignando el campo código como índice
    public static void crearIndicePorCodigo(MongoCollection<Document> collection) {
        collection.createIndex(new Document("codigoPais", 1));
    }
    /*
    * 5.12. Para realizar un backup de la base de datos paises_db,
    * puedes utilizar la herramienta mongodump de MongoDB desde la línea de comandos.
    * Asegúrate de que el servidor MongoDB esté en ejecución y ejecuta el siguiente comando:
    *
        mongodump --db paises_db --out /ruta/del/backup
    *Esto creará un backup de la base de datos paises_db en la ruta especificada en /ruta/del/backup.
    * Para restaurar el backup, puedes utilizar la herramienta mongorestore de MongoDB:
    *
        mongorestore --db paises_db /ruta/del/backup/paises_db
    *
    * Esto restaurará la base de datos paises_db desde la ruta del backup en /ruta/del/backup/paises_db.
    *
    * */

    public static void imprimirDocument(Document doc) {
        ObjectMapper mapper = new ObjectMapper();
        String jsonFormateado = null;
        try {
            jsonFormateado = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(doc);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(jsonFormateado);
    }
}
