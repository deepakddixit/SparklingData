package io.everydata;

interface TpcSparkConstants
{
    // external configuration keys
    String TPC_SPARK_ARTIFACTS_DIR = "tpc.spark.artifacts.dir";
    String TPC_SPARK_SPARK_MASTER = "tpc.spark.spark.master";
    String TPC_SPARK_SPARK_HOME = "tpc.spark.spark.home";
    String TPC_SPARK_SPARK_DEPLOYEMENT_BASE = "tpc.spark.spark.deployment.base";
    String TPC_SPARK_SPARK_DEPLOYEMENT_BASE_DEFAULT_VALUE = "standalone";
    String TPC_SPARK_SPARK_DEPLOYEMENT_MODE = "tpc.spark.spark.deployment.mode";
    String TPC_SPARK_SPARK_DEPLOYEMENT_MODE_DEFAULT_VALUE = "cluster";

    // DATASET related
    String TPC_SPARK_DATASET_NAME = "tpc.spark.dataset.name";
    String TPC_SPARK_DATASET_SCALE_FACTOR = "tpc.spark.dataset.scale.factor";

    // Internal constant values
    String TPC_SPARK_ARTIFACT_NAME = "tpc-spark.jar";
    String TPC_SPARK_DATAGEN_MAIN_CLASS = "io.everydata.TpcSparkDataGen";
    String TPC_SPARK_DATAGEN_WORK_DIR = "/tmp/DataGenerator/";
}
