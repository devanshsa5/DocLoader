package milvus;

import io.milvus.param.bulkinsert.BulkInsertParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.response.MutationResultWrapper;
import reactor.util.function.Tuple2;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

import couchbase.test.sdk.SIFTLoader;
import couchbase.test.val.siftBigANN.Product1;

import java.io.FileWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.ImportResponse;
import io.milvus.grpc.MutationResult;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;

class Document {
    private int id;
    private float[] emb;
    private int scalar;

    public Document(int id, float[] emb, int scalar) {
        this.id = id;
        this.emb = emb;
        this.scalar = scalar;
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

    private static final String COLLECTION_NAME = "demo10";

    public void insertDocs(String indexName, List<Tuple2<String, Object>> docs){
        List<Long> ids = new ArrayList<>();
        List<List<Float>> vectors = new ArrayList<>();
        
        for (Tuple2<String, Object> doc : docs) {
            Object value = doc.getT2();
            if (value instanceof Product1) {
                Product1 product = (Product1) value;
        
                // Add ID
                ids.add((long) product.getId()); // Explicit cast to Long
        
                // Convert float[] to List<Float> and add to vectors
                List<Float> vectorList = new ArrayList<>();
                for (float f : product.getEmbedding()) {
                    vectorList.add(f); // Box float to Float
                }
                vectors.add(vectorList); // Add List<Float>
            } else {
                System.out.println("Skipping non-Product1 object: " + doc.getT1());
            }
        }

        System.out.println("ids" + ids);
        System.out.println("vectors" + vectors);
        
        // Insert data by columns
        // List<InsertParam.Field> fields = new ArrayList<>();
        // fields.add(new InsertParam.Field("id", ids));
        // fields.add(new InsertParam.Field("emb", vectors));
        
        // R<MutationResult> response = milvusclient.insert(InsertParam.newBuilder()
        //         .withCollectionName(COLLECTION_NAME)
        //         .withFields(fields)
        //         .build());
        // if (response.getStatus() != R.Status.Success.getCode()) {
        //     System.out.println("Column-wise insert failed: " + response.getMessage());
        // } else {
        //     MutationResultWrapper wrapper = new MutationResultWrapper(response.getData());
        //     System.out.println(wrapper.getInsertCount() + " rows inserted column-wise.");
        // }
        
        // Insert data by rows
        Gson gson = new Gson();
        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            JsonObject row = new JsonObject();
            row.addProperty("id", ids.get(i));
            row.addProperty("scalar", ids.get(i));
            row.add("emb", gson.toJsonTree(vectors.get(i)));
            rows.add(row);
        }
        
        R<MutationResult> response = milvusclient.insert(InsertParam.newBuilder()
                .withCollectionName(COLLECTION_NAME)
                .withRows(rows)
                .build());
        if (response.getStatus() != R.Status.Success.getCode()) {
            System.out.println("Row-wise insert failed: " + response.getMessage());
        } else {
            System.out.println("Rows inserted successfully.");
        }








    //     ####### BULK API ########
    //     logger.info("Inserting milvus documents: ");
    //     List<Document> jsonDocs = new ArrayList<Document>();
    //     for (Tuple2<String, Object> doc : docs) {
    //         String key = doc.getT1();
    //         Object value = doc.getT2();
    //         if (value instanceof Product1) {
    //             Product1 product = (Product1) value;

    //             Document jsonDoc = new Document(
    //                 product.getId(), // ID from key
    //                 product.getEmbedding(),
    //                 1
    //             );
    //             jsonDocs.add(jsonDoc);
    //             System.out.println("Key: " + key + ", Value: " + product.getId() + ", " + product.getEmbedding());

    //         } else {
    //             System.out.println("Key: " + key + ", Value: " + value);
    //         }
    //     }
        
    // String fileName = "data1.json";
    // try (FileWriter writer = new FileWriter(fileName)) {
    //     Gson gson = new GsonBuilder().setPrettyPrinting().create();
    //     Map<String, List<Document>> wrappedDocs = new HashMap<>();
    //     wrappedDocs.put("rows", jsonDocs);

    //     gson.toJson(wrappedDocs, writer);
    //     System.out.println("Successfully wrote documents to " + fileName);
    // } catch (IOException e) {
    //     throw new RuntimeException("Failed to write JSON file: " + e.getMessage(), e);
    // }
    //     BulkInsertParam param = BulkInsertParam.newBuilder()
    //     .withCollectionName(COLLECTION_NAME)
    //     .addFile("data1.json")
    //     .build();
    //     R<ImportResponse> response = milvusclient.bulkInsert(param);
    //     if (response.getStatus()!= 0) {
    //         throw new RuntimeException("Bulk insert failed: " + response.getMessage());
    //     }
    //     else {
    //             File file = new File("data.json");
    //             if (file.exists() && file.delete()) {
    //                 System.out.println("Temporary file " + fileName + " deleted.");
    //             } else {
    //                 System.err.println("Failed to delete temporary file " + fileName);
    //             }
    //     }
    }
}
