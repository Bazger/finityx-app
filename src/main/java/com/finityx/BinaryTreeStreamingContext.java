package com.finityx;

import com.finityx.common.SparkStreamingContext;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

public class BinaryTreeStreamingContext<TConfig extends BinaryTreeConfig> extends SparkStreamingContext<TConfig> {

    private static Logger logger = Logger.getLogger("com.finityx.logging.AppLog");

    private static final String NODE_COLUMN = "node";
    private static final String NAME_COLUMN = "_name";
    private static final String VALUE_COLUMN = "_value";

    BinaryTreeStreamingContext(String appName, String master, String configPath) throws IOException {
        super(appName, master, configPath, BinaryTreeConfig::new);
    }

    @Override
    public void processFrame() {
        logger.info("Reading file: " + config.getLocalFileField());

        DataType schema = sparkSession.read().format("com.databricks.spark.xml")
                .option("rowTag", "tree")
                .load(config.getLocalFileField()).schema();
        
        //TODO: Schema field validation

        Dataset<Row> df = sparkSession.read().format("com.databricks.spark.xml")
                .option("rowTag", "tree")
                .schema((StructType) fixSchema(schema))
                .load(config.getLocalFileField())
                .cache();

        //Explode a tree to nodes
        Dataset<Row> flattenDf = df.withColumn("node", explode(col(NODE_COLUMN)));
        List<String> searchAllNodeValues = searchNodeValues(flattenDf, config.getSearchingNodeName());

        logger.info(String.format("Values for node %s: {%s}",
                config.getSearchingNodeName(),
                searchAllNodeValues.stream().collect(Collectors.joining(","))));

        df.unpersist();
    }

    private List<String> searchNodeValues(Dataset<Row> df, String searchingNodeName) {
        List<String> matchedNodes = new ArrayList<>();
        df.cache();
        try {
            df.select(col(NODE_COLUMN + "." + NAME_COLUMN)).show(false);
            if (df.where(col(NODE_COLUMN + "." + NAME_COLUMN).equalTo(searchingNodeName)).count() != 0) {
                matchedNodes = df.where(col(NODE_COLUMN + "." + NAME_COLUMN).equalTo(searchingNodeName))
                        .select(NODE_COLUMN + "." + VALUE_COLUMN)
                        .collectAsList().stream()
                        .map(row -> row.getAs(VALUE_COLUMN).toString())
                        .collect(Collectors.toList());
            }
            try {
                Dataset<Row> explodedDf = df.withColumn(NODE_COLUMN, explode(col(NODE_COLUMN + "." + NODE_COLUMN)));
                matchedNodes.addAll(searchNodeValues(explodedDf, searchingNodeName));
                return matchedNodes;
            } catch (Exception ex) { //Reaching the end of the tree
                return matchedNodes;
            }
        } finally {
            df.unpersist();
        }
    }

    //Fixing all Struct nodes, switching them by Array of Struct
    private DataType fixSchema(DataType dataType) {
        if (dataType instanceof StructType) {
            ArrayList<StructField> fields = new ArrayList<>();
            for (StructField field : ((StructType) dataType).fields()) {
                if (field.name().equals(NODE_COLUMN) && field.dataType() instanceof StructType) {
                    fields.add(field.copy(field.name(), new ArrayType(fixSchema(field.dataType()), false), field.nullable(), field.metadata()));
                } else if (!field.name().equals("_VALUE")) {
                    fields.add(field.copy(field.name(), fixSchema(field.dataType()), field.nullable(), field.metadata()));
                }
            }
            return new StructType(fields.toArray(new StructField[0]));
        } else if (dataType instanceof ArrayType) {
            return new ArrayType(
                    fixSchema(((ArrayType) dataType).elementType()),
                    ((ArrayType) dataType).containsNull());
        } else
            return dataType;

    }
}
