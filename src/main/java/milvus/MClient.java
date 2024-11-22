package milvus;

import io.milvus.param.bulkinsert.BulkInsertParam;
import reactor.util.function.Tuple2;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.FileWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.ImportResponse;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;


public class MClient {

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
    }

    private static final String COLLECTION_NAME = "demo3"; // Example placeholder

    public void insert_docs(String indexName, List<Tuple2<String, Object>> docs){
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
