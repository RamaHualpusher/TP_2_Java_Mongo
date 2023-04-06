package org.example;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

public class App {

    private static final String BASE_URL = "https://restcountries.com/v2/callingcode/";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        //consultaRESTInsertMongoDB();
    }

    public static void consultaRESTInsertMongoDB () {
        OkHttpClient client = new OkHttpClient();
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("paises_db");
        MongoCollection<Document> collection = database.getCollection("paises");

        try {
            for (int i = 1; i <= 300; i++) {
                Request request = new Request.Builder()
                        .url(BASE_URL + i)
                        .build();

                Response response = client.newCall(request).execute();
                String responseBody = response.body().string();

                if (!response.isSuccessful()) {
                    System.out.println("Error: " + responseBody);
                    continue;
                }

                JSONArray jsonArray = new JSONArray(responseBody);


                for (int j = 0; j < jsonArray.length(); j++) {
                    JSONObject jsonObject = jsonArray.getJSONObject(j);

                    Pais pais = new Pais();
                    pais.setCodigoPais(jsonObject.optString("alpha3Code", "---"));
                    pais.setNombrePais(jsonObject.optString("name", "unknown"));
                    pais.setCapitalPais(jsonObject.optString("capital", "unknown"));
                    pais.setRegion(jsonObject.optString("region", "unknown"));
                    pais.setPoblacion(jsonObject.optInt("population", 0));
                    pais.setLatitud(jsonObject.optJSONArray("latlng") != null
                            ? Double.parseDouble(jsonObject.optJSONArray("latlng").optString(0, "0"))
                            : 0);
                    pais.setLongitud(jsonObject.optJSONArray("latlng") != null
                            ? Double.parseDouble(jsonObject.optJSONArray("latlng").optString(1, "0"))
                            : 0);

                    Document doc = Document.parse(objectMapper.writeValueAsString(pais));
                    collection.insertOne(doc);
                    System.out.println("Inserted: " + pais.getNombrePais());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            mongoClient.close();
        }
    }

}
