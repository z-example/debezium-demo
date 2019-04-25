package example;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.data.Envelope;
import io.debezium.embedded.EmbeddedEngine;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @author Zero
 * Created on 2019/4/24.
 */
public class MySQL {
    //https://debezium.io/blog/tags/examples/

    public static void main(String[] args) throws InterruptedException {
        // Define the configuration for the embedded and MySQL connector ...
        Configuration config = Configuration.create()
                /* begin engine properties */
//              .with(EmbeddedEngine.CONNECTOR_CLASS, MySqlConnector.class)
                .with("connector.class", "io.debezium.connector.mysql.MySqlConnector")
//              .with(EmbeddedEngine.OFFSET_STORAGE, FileOffsetBackingStore.class)
                .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename", "/path/to/storage/offset.dat")
                .with("offset.flush.interval.ms", 10000)//10s保存一次偏移量, 0表示每次都保存
                /* begin connector properties */

                .with("name", "mysql-connector") //随意
                .with("database.hostname", "localhost")
                .with("database.port", 3306)
                .with("database.user", "root")
                .with("database.password", "")
                .with("database.serverTimezone", "UTC") //高版本的MySQL驱动需要设置
                .with("server.id", 85744) //确保唯一即可
                .with("database.server.name", "products")
                .with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
                .with("database.history.file.filename", "/path/to/storage/dbhistory.dat")
                .with(MySqlConnectorConfig.DATABASE_WHITELIST, "test")//只关注的数据库, 使用MySqlConnectorConfig
                .with("table.whitelist", "users")//只关注的table
                .build();
// Create the engine with this configuration ...
        EmbeddedEngine engine = EmbeddedEngine.create()
                .using(config)
                .notifying(MySQL::handleEvent)
                .build();
        engine.run();//这里直接在主线程运行即可
// Run the engine asynchronously ...
//        Executor executor = Executors.newCachedThreadPool();
//        executor.execute(engine);

// At some later time ...
//        engine.stop();
    }

    private static void handleEvent(List<SourceRecord> sourceRecords, EmbeddedEngine.RecordCommitter recordCommitter) {
        System.out.println(sourceRecords);
        sourceRecords.forEach(record -> {
            try {
                Struct value = (Struct) record.value();
                //Envelope.ALL_FIELD_NAMES
                System.out.println("topic: " + record.topic());
                value.schema().fields().forEach(field -> {
                    System.out.println(field.name());
                    System.out.println(field);
                });
                //products是上面配置中的database.server.name
                if ("products.test.users".equals(record.topic())) {
                    Struct before = value.getStruct("before");//before.getString("username");
                    Struct after = value.getStruct("after");
                    System.out.println("before: " + before);
                    System.out.println("after: " + after);
                    Envelope.Operation op = Envelope.Operation.forCode(value.getString("op"));
                    if (op == Envelope.Operation.UPDATE) {
                        System.out.println("update");
                    }
                } else if ("products".equals(record.topic())) {
                    //value.getString("ddl")
                    //value.getString("databaseName")
                }
                recordCommitter.markProcessed(record);//调用这一步, 才会保存偏移量
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        recordCommitter.markBatchFinished();
    }


}