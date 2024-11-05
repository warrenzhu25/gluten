# Create DPGCE Cluster with Native binaries installed

## Internal quick start
```shell
gcloud dataproc clusters create <NAME> \
  --enable-component-gateway \
  --region us-east1 \
  --subnet dataproc-spark-sense-nqe \
  --no-address \
  --master-machine-type n2-standard-16 \
  --master-boot-disk-type pd-balanced \
  --master-boot-disk-size 500 \
  --num-master-local-ssds 2 \
  --master-local-ssd-interface NVME \
  --num-workers 9 \
  --worker-machine-type n2-standard-16 \
  --worker-boot-disk-type pd-balanced \
  --worker-boot-disk-size 500 \
  --num-worker-local-ssds 2 \
  --worker-local-ssd-interface NVME \
  --image-version 2.2-ubuntu22 \
  --initialization-actions 'gs://dataproc-experimental/setup-nqe-dpgce-2.2-ubuntu22.sh' \
  --project google.com:hadoop-cloud-dev
```

## Configurations to add
Ensure correct memory.
```
  --conf spark.executor.memory=<x>g \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=<6x>g \
  --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager \
  --conf spark.driver.defaultJavaOptions='-Dio.netty.tryReflectionSetAccessible=true -XX:+ErrorFileToStderr' \
  --conf spark.executor.defaultJavaOptions='-Dio.netty.tryReflectionSetAccessible=true -XX:+ErrorFileToStderr' \
  --conf spark.plugins=org.apache.gluten.GlutenPlugin \
  --conf spark.gluten.sql.native.writer.enabled=false \
  --conf spark.gluten.sql.columnar.backend.velox.flushablePartialAggregation=false \
  --conf spark.gluten.loadLibFromJar=true \
  --conf spark.gluten.sql.columnar.backend.lib=velox
```
