from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, input_file_name, regexp_extract, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import glob
import time

def process_files(file_path: str):
    start_total_time = time.time()
    
    # 🚀 CONFIGURAÇÃO ULTRA-OTIMIZADA PARA 4 CORES
    spark = SparkSession.builder \
        .appName("ClickHouse Ultra-Fast - 4Core Max Performance") \
        .config("spark.jars", "clickhouse-jdbc-0.3.2-patch9-all.jar") \
        .config("spark.master", "local[4]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m") \
        .config("spark.sql.shuffle.partitions", "16") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.cores", "4") \
        .config("spark.sql.files.maxPartitionBytes", "128m") \
        .config("spark.default.parallelism", "16") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.files.openCostInBytes", "1048576") \
        .config("spark.executor.instances", "1") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "64m") \
        .config("spark.sql.files.ignoreCorruptFiles", "false") \
        .config("spark.sql.files.ignoreMissingFiles", "false") \
        .config("spark.sql.parquet.columnarReaderBatchSize", "8192") \
        .config("spark.sql.inMemoryColumnarStorage.batchSize", "20000") \
        .getOrCreate()

    # 🔥 CONFIGURAÇÕES CLICKHOUSE ULTRA-OTIMIZADAS
    url = "jdbc:clickhouse://clickhouse:8123/default"
    properties = {
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        "user": "default",
        "password": "genetic123",
        # Timeouts otimizados
        "socket_timeout": "3600000",  # 1 hora
        "connection_timeout": "300000",  # 5 minutos
        "max_execution_time": "3600",  # 1 hora
        # Compressão e buffers otimizados
        "compress": "1",
        "decompress": "1",
        "compression": "lz4",
        "buffer_size": "1048576",  # 1MB
        "max_buffer_size": "10485760",  # 10MB
        # Performance crítica
        "max_threads": "4",
        "max_memory_usage": "4000000000",  # 4GB
        "max_bytes_before_external_group_by": "2000000000",  # 2GB
        "max_bytes_before_external_sort": "2000000000",  # 2GB
        # Inserção otimizada
        "max_insert_block_size": "1000000",  # 1M rows
        "min_insert_block_size_rows": "500000",  # 500K rows
        "min_insert_block_size_bytes": "268435456",  # 256MB
        "max_insert_threads": "4",
        # Configurações de rede
        "send_progress_in_http_headers": "1",
        "http_connection_timeout": "300000",
        "http_send_timeout": "3600000",
        "http_receive_timeout": "3600000",
        # Otimizações específicas
        "async_insert": "0",  # Inserção síncrona para controle total
        "wait_for_async_insert": "0",
        "optimize_on_insert": "0",  # Não otimizar durante inserção
        "fsync_metadata": "0",  # Não forçar sync de metadata
        "ssl": "false"
    }

    print("🚀 MODO ULTRA-PERFORMANCE - 4 CORES MÁXIMA VELOCIDADE")
    print(f"💻 Configuração: 4 cores ativos, inserção em lotes de 1M registros")
    
    # Schema otimizado
    schema = StructType([
        StructField("snp_id", StringType(), False),
        StructField("chromosome", StringType(), False), 
        StructField("position", StringType(), False),
        StructField("genotype", StringType(), False)
    ])

    # Descobrir arquivos
    samples_dir = ""
    csv_pattern = os.path.join(samples_dir, "*.csv")
    csv_files = glob.glob(csv_pattern)
    
    total_size_mb = sum(os.path.getsize(f) / (1024 * 1024) for f in csv_files)
    print(f"📁 {len(csv_files)} arquivos, {total_size_mb:.1f} MB total")
    
    if not csv_files:
        print("❌ Nenhum arquivo encontrado")
        spark.stop()
        return

    print("⚡ LEITURA ULTRA-RÁPIDA EM 4 CORES...")
    read_start = time.time()
    
    # Leitura otimizada para máxima velocidade
    df = spark.read \
        .option("sep", "\t") \
        .option("header", "false") \
        .option("comment", "#") \
        .option("mode", "FAILFAST") \
        .option("multiline", "false") \
        .option("escape", "") \
        .option("quote", "") \
        .option("ignoreLeadingWhiteSpace", "false") \
        .option("ignoreTrailingWhiteSpace", "false") \
        .option("maxCharsPerColumn", "1000") \
        .schema(schema) \
        .csv(csv_pattern)
    
    read_time = time.time() - read_start
    print(f"✅ Leitura: {read_time:.1f}s")

    print("⚡ TRANSFORMAÇÕES ULTRA-RÁPIDAS...")
    transform_start = time.time()
    
    # Pipeline ultra-otimizado
    df = df.withColumn("filename", input_file_name()) \
        .withColumn("individual_id", regexp_extract("filename", r"([^/]+)\.csv$", 1)) \
        .withColumn("position", col("position").cast(IntegerType())) \
        .select("individual_id", "snp_id", "chromosome", "position", "genotype") \
        .filter(col("individual_id") != "")
    
    # Reparticionamento otimizado para 4 cores (16 partições = 4 por core)
    df = df.repartition(16, "individual_id")
    
    transform_time = time.time() - transform_start
    print(f"✅ Transformações: {transform_time:.1f}s")

    print("⚡ CONTAGEM ULTRA-RÁPIDA...")
    count_start = time.time()
    df.cache()
    total_records = df.count()
    count_time = time.time() - count_start
    print(f"✅ {total_records:,} registros em {count_time:.1f}s")

    print("🔥 INSERÇÃO ULTRA-RÁPIDA EM 4 CORES - LOTES DE 1M...")
    insert_start = time.time()
    
    try:
        # Inserção ultra-otimizada com lotes gigantes
        df.write \
            .mode("append") \
            .option("batchsize", "1000000") \
            .option("numPartitions", "16") \
            .option("isolationLevel", "NONE") \
            .option("truncate", "false") \
            .option("rewriteBatchedStatements", "true") \
            .option("queryTimeout", "3600") \
            .option("loginTimeout", "300") \
            .jdbc(url=url, table="genotypes_raw", properties=properties)
        
        insert_time = time.time() - insert_start
        print(f"✅ Inserção: {insert_time:.1f}s")
        
        # Verificação final
        print("⚡ Verificação...")
        verify_start = time.time()
        final_count = spark.read.jdbc(url=url, table="genotypes_raw", properties=properties).count()
        verify_time = time.time() - verify_start
        
        total_time = time.time() - start_total_time
        
        print(f"\n🎉 PROCESSAMENTO ULTRA-RÁPIDO CONCLUÍDO!")
        print(f"📊 ESTATÍSTICAS FINAIS:")
        print(f"   ⚡ Tempo total: {total_time:.1f}s ({total_time/60:.1f} minutos)")
        print(f"   📈 Registros processados: {total_records:,}")
        print(f"   🚀 Taxa: {total_records/total_time:,.0f} registros/segundo")
        print(f"   💾 Total na tabela: {final_count:,}")
        print(f"   📁 Throughput: {total_size_mb/total_time:.1f} MB/s")
        print(f"   💻 Utilização: 4/4 cores (100%)")
        
        # Análise de performance
        expected_min_rate = 50000  # 50K registros/segundo mínimo esperado
        if total_records/total_time >= expected_min_rate:
            print(f"🏆 EXCELENTE! Taxa acima de {expected_min_rate:,} registros/segundo!")
        else:
            print(f"⚠️  Taxa abaixo do esperado ({expected_min_rate:,} registros/segundo)")
        
        # Breakdown detalhado
        print(f"\n⏱️ BREAKDOWN DE TEMPOS:")
        print(f"   📖 Leitura: {read_time:.1f}s ({read_time/total_time*100:.1f}%)")
        print(f"   🔄 Transformações: {transform_time:.1f}s ({transform_time/total_time*100:.1f}%)")
        print(f"   🔢 Contagem: {count_time:.1f}s ({count_time/total_time*100:.1f}%)")
        print(f"   💾 Inserção: {insert_time:.1f}s ({insert_time/total_time*100:.1f}%)")
        print(f"   ✅ Verificação: {verify_time:.1f}s ({verify_time/total_time*100:.1f}%)")
        
        # Análise de eficiência ultra-detalhada
        cpu_efficiency = (total_records / total_time) / 4  # registros por segundo por core
        print(f"\n💻 EFICIÊNCIA ULTRA-DETALHADA:")
        print(f"   🔥 {cpu_efficiency:,.0f} registros/segundo/core")
        print(f"   📊 {total_size_mb/total_time/4:.1f} MB/s/core")
        print(f"   ⚡ {1000000/cpu_efficiency:.1f} microssegundos/registro/core")
        
        # Comparação com expectativas
        if total_time <= 180:  # 3 minutos
            print(f"🏆 ULTRA-RÁPIDO! Processamento em menos de 3 minutos!")
        elif total_time <= 300:  # 5 minutos
            print(f"🚀 RÁPIDO! Processamento em menos de 5 minutos!")
        elif total_time <= 600:  # 10 minutos
            print(f"👍 BOM! Dentro do tempo aceitável!")
        else:
            print(f"⚠️  LENTO! Precisa de mais otimização!")
        
        # Dicas de otimização
        print(f"\n💡 ANÁLISE DE PERFORMANCE:")
        if insert_time/total_time > 0.7:
            print(f"   ⚠️  Inserção é o gargalo ({insert_time/total_time*100:.1f}% do tempo)")
            print(f"   💡 Considere aumentar batchsize ou otimizar ClickHouse")
        if read_time/total_time > 0.3:
            print(f"   ⚠️  Leitura está lenta ({read_time/total_time*100:.1f}% do tempo)")
            print(f"   💡 Considere usar formato Parquet ou otimizar I/O")
        
        print(f"\n🔧 MONITORAMENTO RECOMENDADO:")
        print(f"   • htop: todos os 4 cores devem estar ~100%")
        print(f"   • iotop: verificar I/O de disco")
        print(f"   • nethogs: verificar tráfego de rede para ClickHouse")
        
    except Exception as e:
        print(f"❌ Erro durante inserção: {str(e)}")
        print(f"💡 Dica: Verifique se ClickHouse está configurado para receber lotes grandes")
        raise
    finally:
        df.unpersist()

    spark.stop()
    print("🏁 SparkSession finalizada - 4 cores liberados") 