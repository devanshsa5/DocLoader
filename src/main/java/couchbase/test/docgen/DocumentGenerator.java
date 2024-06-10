package couchbase.test.docgen;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;
import couchbase.test.key.CircularKey;
import couchbase.test.key.RandomKey;
import couchbase.test.key.RandomSizeKey;
import couchbase.test.key.ReverseKey;
import couchbase.test.key.SimpleKey;
import couchbase.test.val.Hotel;
import couchbase.test.val.Cars;
import couchbase.test.val.NimbusM;
import couchbase.test.val.NimbusP;
import couchbase.test.val.Product;
import couchbase.test.val.SimpleValue;
import couchbase.test.val.Vector;
import couchbase.test.val.anySizeValue;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

abstract class KVGenerator{
    public WorkLoadSettings ws;
    String padding = "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
            + "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";
    protected Object keys;
    protected Object vals;
    private Class<?> keyInstance;
    private Class<?> valInstance;
    protected Method keyMethod;
    protected Method valMethod;
    long startTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

    public KVGenerator(WorkLoadSettings ws, String keyClass, String valClass) throws ClassNotFoundException {
        super();
        this.ws = ws;
        if(keyClass.equals(RandomKey.class.getSimpleName()))
            this.keyInstance = RandomKey.class;
        else if(keyClass.equals(ReverseKey.class.getSimpleName()))
            this.keyInstance = ReverseKey.class;
        else if(keyClass.equals(RandomSizeKey.class.getSimpleName()))
            this.keyInstance = RandomSizeKey.class;
        else if(keyClass.equals(CircularKey.class.getSimpleName()))
            this.keyInstance = CircularKey.class;
        else
            this.keyInstance = SimpleKey.class;

        if(valClass.equals(anySizeValue.class.getSimpleName()))
            this.valInstance = anySizeValue.class;
        else if (valClass.equals(NimbusP.class.getSimpleName()))
            this.valInstance = NimbusP.class;
        else if (valClass.equals(NimbusM.class.getSimpleName()))
            this.valInstance = NimbusM.class;
        else if (valClass.equals(Hotel.class.getSimpleName()))
            this.valInstance = Hotel.class;
        else if (valClass.equals(Cars.class.getSimpleName()))
            this.valInstance = Cars.class;
        else if (valClass.equals(Vector.class.getSimpleName()))
            this.valInstance = Vector.class;
        else if (valClass.equals(Product.class.getSimpleName()))
            this.valInstance = Product.class;
        else
            this.valInstance = SimpleValue.class;

        try {
            this.keys = keyInstance.getConstructor(WorkLoadSettings.class).newInstance(ws);
            this.vals = valInstance.getConstructor(WorkLoadSettings.class).newInstance(ws);
            this.keyMethod = this.keyInstance.getDeclaredMethod("next", long.class);
            this.valMethod = this.valInstance.getDeclaredMethod("next", String.class);
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        }
    }

    public boolean has_next_create() {
        if (this.ws.dr.createItr.get() < this.ws.dr.create_e)
            return true;
        return false;
    }

    public boolean has_next_read() {
        if (this.ws.dr.readItr.get() < this.ws.dr.read_e)
            return true;
        if (this.keyInstance.getSimpleName() == CircularKey.class.getSimpleName()) {
            this.resetRead();
            return true;
        }
        return false;
    }

    public boolean has_next_update() {
        if (this.ws.dr.updateItr.get() < this.ws.dr.update_e)
            return true;
        if (this.keyInstance.getSimpleName() == CircularKey.class.getSimpleName() || TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis())-startTime<ws.mutation_timeout) {
            this.resetUpdate();
            this.ws.mutated+=1;
            return true;
        }
        return false;
    }

    public boolean has_next_expiry() {
        if (this.ws.dr.expiryItr.get() < this.ws.dr.expiry_e)
            return true;
        if (this.keyInstance.getSimpleName() == CircularKey.class.getSimpleName()) {
            this.resetExpiry();
            return true;
        }
        return false;
    }

    public boolean has_next_delete() {
        if (this.ws.dr.deleteItr.get() < this.ws.dr.delete_e)
            return true;
        return false;
    }

    abstract Tuple2<String, Object> next();

    void resetRead() {
        this.ws.dr.readItr =  new AtomicLong(this.ws.dr.read_s);
    }

    void resetExpiry() {
        this.ws.dr.expiryItr.set(this.ws.dr.expiry_s);
    }

    void resetUpdate() {
        this.ws.dr.updateItr.set(this.ws.dr.update_s);
    }
}

public class DocumentGenerator extends KVGenerator{
    boolean targetvB;

    public DocumentGenerator(WorkLoadSettings ws, String keyClass, String valClass) throws ClassNotFoundException {
        super(ws, keyClass, valClass);
    }

    public WorkLoadSettings get_work_load_settings() {
        return this.ws;
    }

    public Tuple2<String, Object> next() {
        long temp = this.ws.dr.createItr.incrementAndGet();
        String k = null;
        Object v = null;
            try {
                k = (String) this.keyMethod.invoke(this.keys, temp);
                v = (Object) this.valMethod.invoke(this.vals, k);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
                e1.printStackTrace();
            }
        return Tuples.of(k, v);
    }

    public Tuple2<String, Object> nextRead() {
        long temp = this.ws.dr.readItr.incrementAndGet();
        String k = null;
        Object v = null;
            try {
                k = (String) this.keyMethod.invoke(this.keys, temp);
                v = (Object) this.valMethod.invoke(this.vals, k);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
                e1.printStackTrace();
            }
        return Tuples.of(k, v);
    }

    public Tuple2<String, Object> nextUpdate() {
        long temp = this.ws.dr.updateItr.incrementAndGet();
        String k = null;
        Object v = null;
            try {
                k = (String) this.keyMethod.invoke(this.keys, temp);
                v = (Object) this.valMethod.invoke(this.vals, k);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
                e1.printStackTrace();
            }
        return Tuples.of(k, v);
    }

    public Tuple2<String, Object> nextExpiry() {
        long temp = this.ws.dr.expiryItr.incrementAndGet();
        String k = null;
        Object v = null;
            try {
                k = (String) this.keyMethod.invoke(this.keys, temp);
                v = (Object) this.valMethod.invoke(this.vals, k);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
                e1.printStackTrace();
            }
        return Tuples.of(k, v);
    }

    public List<Tuple2<String, Object>> nextInsertBatch() {
        List<Tuple2<String, Object>> docs = new ArrayList<Tuple2<String,Object>>();
        int count = 0;
        while (this.has_next_create() && count<ws.batchSize*ws.creates/100) {
            docs.add(this.next());
            count += 1;
        }
        return docs;
    }

    public List<Tuple2<String, Object>> nextReadBatch() {
        List<Tuple2<String, Object>> docs = new ArrayList<Tuple2<String,Object>>();
        int count = 0;
        while (this.has_next_read() && count<ws.batchSize*ws.reads/100) {
            docs.add(this.nextRead());
            count += 1;
        }
        return docs;
    }

    public List<Tuple2<String, Object>> nextUpdateBatch() {
        List<Tuple2<String, Object>> docs = new ArrayList<Tuple2<String,Object>>();
        int count = 0;
        while (this.has_next_update() && count<ws.batchSize*ws.updates/100) {
            docs.add(this.nextUpdate());
            count += 1;
        }
        return docs;
    }

    public List<Tuple2<String, Object>> nextExpiryBatch() {
        List<Tuple2<String, Object>> docs = new ArrayList<Tuple2<String,Object>>();
        int count = 0;
        while (this.has_next_expiry() && count<ws.batchSize*ws.expiry/100) {
            docs.add(this.nextExpiry());
            count += 1;
        }
        return docs;
    }

    public List<String> nextDeleteBatch() {
        int count = 0;
        long temp;
        List<String> docs = new ArrayList<String>();
        while (this.has_next_delete() && count<ws.batchSize*ws.deletes/100) {
            try {
                temp = this.ws.dr.deleteItr.incrementAndGet();
                docs.add((String) this.keyMethod.invoke(this.keys, temp));
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                e.printStackTrace();
            }
            count += 1;
        }
        return docs;
    }
}
