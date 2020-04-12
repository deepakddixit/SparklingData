package io.everydata;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Hello world!
 */
public class TpcSparkApp
{
    private static final Logger logger = LoggerFactory.getLogger(TpcSparkApp.class);

    static List<String> mandatoryProperties = new ArrayList<>();

    static {
        mandatoryProperties.add(TpcSparkConstants.TPC_SPARK_ARTIFACTS_DIR);
        mandatoryProperties.add(TpcSparkConstants.TPC_SPARK_SPARK_MASTER);
        mandatoryProperties.add(TpcSparkConstants.TPC_SPARK_SPARK_HOME);
        mandatoryProperties.add(TpcSparkConstants.TPC_SPARK_DATASET_NAME);
        mandatoryProperties.add(TpcSparkConstants.TPC_SPARK_DATASET_SCALE_FACTOR);
    }

    public static void main(String[] args)
    {
        try {
            if (args == null || (args.length == 0)) {
                throw new IllegalArgumentException(
                        "Minimum 1 argument [configuration file path] required.");
            }

            String configFilePath = args[0].trim();

            File file = new File(configFilePath);
            if (!file.exists()) {

                throw new IllegalArgumentException("Configuration file path provided not exists.");
            }

            Properties properties = new Properties();
            properties.load(new FileReader(file));

            logger.info("Properties passed {} ", properties);

            validateMandatoryProps(properties);

            String artifactsPath = properties.getProperty(TpcSparkConstants.TPC_SPARK_ARTIFACTS_DIR);
            File artifactsDir = new File(artifactsPath);
            if (!artifactsDir.exists() && !artifactsDir.isDirectory()) {
                String
                        msg =
                        String.format("Artifacts dir path [%s] provided does not exists",
                                artifactsDir.getAbsolutePath());
                throw new IllegalArgumentException(msg);
            }

            File artifactPath = new File(artifactsDir, TpcSparkConstants.TPC_SPARK_ARTIFACT_NAME);

            if (!artifactPath.exists()) {
                String
                        msg =
                        String.format("Artifact path [%s] does not exists", artifactPath.getAbsolutePath());
                throw new IllegalArgumentException(msg);
            }

            String
                    applicationName =
                    getApplicationName(properties.getProperty(TpcSparkConstants.TPC_SPARK_DATASET_NAME),
                            properties.getProperty(TpcSparkConstants.TPC_SPARK_DATASET_SCALE_FACTOR));

            SparkLauncher sparkAppLauncher = new SparkLauncher()
                    .setMaster(properties.getProperty(TpcSparkConstants.TPC_SPARK_SPARK_MASTER))
                    .setAppName(
                            applicationName)
                    .setAppResource(artifactPath.getAbsolutePath())
                    .setMainClass(TpcSparkConstants.TPC_SPARK_DATAGEN_MAIN_CLASS)
                    .setSparkHome(properties.getProperty(TpcSparkConstants.TPC_SPARK_SPARK_HOME))
                    .setDeployMode(properties.getProperty(TpcSparkConstants.TPC_SPARK_SPARK_DEPLOYEMENT_MODE,
                            TpcSparkConstants.TPC_SPARK_SPARK_DEPLOYEMENT_MODE_DEFAULT_VALUE))
                    .redirectOutput(getAppOutputPath(applicationName))
                    .redirectError(getAppErrorPath(applicationName));

            // add deps jars
            addJars(properties, sparkAppLauncher, artifactsDir);

            // add property file as argument
            StringWriter propStrWriter = new StringWriter();

            properties.store(propStrWriter, "Spark Arguments");
            String sparkJobArg = propStrWriter.toString();

            sparkAppLauncher.addAppArgs(sparkJobArg);

            try {
                SparkAppHandle sparkAppHandle = sparkAppLauncher.startApplication();
                logger.info("Launched spark job Current State: {}", sparkAppHandle.getState());

                sparkAppHandle.addListener(new SparkAppHandle.Listener()
                {
                    @Override
                    public void stateChanged(SparkAppHandle sparkAppHandle)
                    {
                        SparkAppHandle.State state = sparkAppHandle.getState();
                        logger.info("Spark Job Id: {} | state : {}", sparkAppHandle.getAppId(),
                                state);

                        switch (state) {
                            case UNKNOWN:
                                break;
                            case CONNECTED:
                                break;
                            case SUBMITTED:
                                break;
                            case RUNNING:
                                break;
                            case FINISHED:
                                logger.error("DataGen completed successfully.");
                                break;
                            case FAILED:
                                logger.error("DataGen Failed");
                                break;
                            case KILLED:
                                logger.error("DataGen Failed");
                                break;
                            case LOST:
                                logger.error("DataGen Failed");
                                break;
                        }
                    }

                    @Override
                    public void infoChanged(SparkAppHandle sparkAppHandle)
                    {
                        logger.info("Spark Job Id: {} | state : {}", sparkAppHandle.getAppId(),
                                sparkAppHandle.getState());
                    }
                });
            }
            catch (IOException e) {
                String emsg = "Error in launching spark job";
                logger.error(emsg, e);
                throw new DataGenException(emsg, e);
            }
        }
        catch (Throwable t) {
            System.out.println(t.getMessage());
            System.exit(1);
        }
    }

    private static void addJars(Properties properties, SparkLauncher sparkAppLauncher,
            File artifactsDir)
    {
        String
                sparkDeploymentBase =
                properties.getProperty(TpcSparkConstants.TPC_SPARK_SPARK_DEPLOYEMENT_BASE,
                        TpcSparkConstants.TPC_SPARK_SPARK_DEPLOYEMENT_BASE_DEFAULT_VALUE);

        if (sparkDeploymentBase
                .equals(TpcSparkConstants.TPC_SPARK_SPARK_DEPLOYEMENT_BASE_DEFAULT_VALUE)) {
            String classPath = artifactsDir.getAbsolutePath() + "/*";
            logger.info("Setting driver and executor class path to {}", classPath);
            sparkAppLauncher.setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH,
                    classPath);
            sparkAppLauncher.setConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH,
                    classPath);
        }
        else {
            logger.info("Adding dependency jars");
            File[] files = artifactsDir.listFiles(new FileFilter()
            {
                @Override
                public boolean accept(File file)
                {
                    return file.getName().endsWith(".jar");
                }
            });
            Arrays.asList(files).forEach(jarFile -> {
                sparkAppLauncher.addJar(jarFile.getAbsolutePath());
            });
        }
    }

    private static File getAppErrorPath(String applicationName)
    {
        File file = makeWorkDir();
        return new File(file, applicationName + ".err");
    }

    private static File makeWorkDir()
    {
        File file = new File(TpcSparkConstants.TPC_SPARK_DATAGEN_WORK_DIR);
        if (!file.exists()) {
            file.mkdirs();
            file.setExecutable(true);
            file.setReadable(true);
            file.setWritable(true);
        }
        return file;
    }

    private static File getAppOutputPath(String applicationName)
    {
        File file = makeWorkDir();
        return new File(file, applicationName + ".out");
    }

    private static String getApplicationName(String... nameParts)
    {
        return "DataGenerator-" + Arrays.asList(nameParts).stream().collect(Collectors.joining("-"));
    }

    private static void validateMandatoryProps(Properties properties)
    {
        List<String> missingProperties = mandatoryProperties.stream().filter(prop -> {
            if (!(properties.containsKey(prop) && properties.getProperty(prop) != null && !properties
                    .getProperty(prop).isEmpty())) {
                return true;
            }
            return false;
        }).collect(Collectors.toList());

        if (!missingProperties.isEmpty()) {
            String
                    msg =
                    String.format("Missing properties [%s] from provided configuration file",
                            missingProperties);
            throw new IllegalArgumentException(msg);
        }
    }
}
