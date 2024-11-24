package milvus;

import io.milvus.param.bulkinsert.BulkInsertParam;
import reactor.util.function.Tuple2;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import couchbase.test.sdk.SIFTLoader;
import couchbase.test.val.siftBigANN.Product1;

import java.io.FileWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.ImportResponse;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;

class Document {
    private int id;
    private float[] emb;
    private int scalar;

    public Document(int id, float[] emb) {
        this.id = id;
        this.emb = emb;
    }
}


public class MClient {

    static Logger logger = LogManager.getLogger(SIFTLoader.class);

    String serverUrl = "http://localhost:19530";
	String apiKey = null;
    MilvusClient milvusclient;

    public MClient(String serverUrl, String apiKey) {
		super();
		this.serverUrl = serverUrl;
		this.apiKey = apiKey;
	}

    public void connect(){
        milvusclient = new MilvusServiceClient(
                ConnectParam.newBuilder()
                        .withHost(serverUrl)
                        .withPort(19530)
                        .build()
        );
        logger.info("Connected to Milvus");
    }

    private static final String COLLECTION_NAME = "demo3"; // Example placeholder

    public void insertDocs(String indexName, List<Tuple2<String, Object>> docs){
        logger.info("Inserting milvus documents: ");
        // logger.info("Inserting milvus docs: " + docs);
        List<Document> jsonDocs = new ArrayList<Document>();
        for (Tuple2<String, Object> doc : docs) {
            String key = doc.getT1();
            Object value = doc.getT2();
            if (value instanceof Product1) {
                Product1 product = (Product1) value;
                // Access fields directly (assuming getter methods exist)
                Document jsonDoc = new Document(
                    product.getId(), // ID from key
                    (product.getEmbedding())
                );
                jsonDocs.add(jsonDoc);
                System.out.println("Key: " + key + ", Value: " + product.getId() + ", " + product.getEmbedding());

            } else {
                System.out.println("Key: " + key + ", Value: " + value);
            }
        }
        
    String fileName = "data.json";
    try (FileWriter writer = new FileWriter(fileName)) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        gson.toJson(docs, writer);
        System.out.println("Successfully wrote documents to " + fileName);
    } catch (IOException e) {
        throw new RuntimeException("Failed to write JSON file: " + e.getMessage(), e);
    }
        BulkInsertParam param = BulkInsertParam.newBuilder()
        .withCollectionName(COLLECTION_NAME)
        .addFile("data.json")
        .build();
        R<ImportResponse> response = milvusclient.bulkInsert(param);

        // Check the response
        if (response.getStatus()!= 0) {
            throw new RuntimeException("Bulk insert failed: " + response.getMessage());
        }
        else {
                File file = new File("data.json");
                if (file.exists() && file.delete()) {
                    System.out.println("Temporary file " + fileName + " deleted.");
                } else {
                    System.err.println("Failed to delete temporary file " + fileName);
                }
        }
    }
}
