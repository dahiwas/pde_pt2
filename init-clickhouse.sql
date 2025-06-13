-- 🚀 CONFIGURAÇÃO ULTRA-OTIMIZADA PARA MÁXIMA PERFORMANCE
-- Configurações globais para processamento de 10M+ registros

-- Configurações básicas de performance
SET max_threads = 4;
SET max_memory_usage = 4000000000; -- 4GB
SET max_execution_time = 3600; -- 1 hora

-- Configurações de inserção otimizadas
SET max_insert_block_size = 1000000; -- 1M rows por bloco
SET min_insert_block_size_rows = 500000; -- 500K rows mínimo
SET min_insert_block_size_bytes = 268435456; -- 256MB mínimo
SET max_insert_threads = 4; -- 4 threads para inserção

-- Configurações de compressão
SET network_compression_method = 'lz4';
SET max_compress_block_size = 1048576; -- 1MB
SET min_compress_block_size = 65536; -- 64KB

-- Configurações de timeout
SET send_timeout = 3600; -- 1 hora
SET receive_timeout = 3600; -- 1 hora

-- Otimizações específicas para inserção
SET async_insert = 0; -- Inserção síncrona para controle total
SET wait_for_async_insert = 0;

-- Criar database se não existir
CREATE DATABASE IF NOT EXISTS default;

-- Usar database default
USE default;

-- 🔥 TABELA PRINCIPAL ULTRA-OTIMIZADA PARA INSERÇÃO EM MASSA
-- Usando String para chromosome para consistência com dados do Spark
CREATE TABLE genotypes_raw (
    individual_id String,
    snp_id String,
    chromosome String,
    position Int32,
    genotype String
) ENGINE = MergeTree()
ORDER BY (individual_id, chromosome, position)
PARTITION BY individual_id
SETTINGS index_granularity = 8192;

-- Verificar se tabela foi criada
SHOW TABLES;

-- 💡 COMANDOS DE MONITORAMENTO ÚTEIS:
-- SELECT count() FROM genotypes_raw; -- Contar registros
-- SELECT individual_id, count() FROM genotypes_raw GROUP BY individual_id; -- Por indivíduo
-- SELECT chromosome, count() FROM genotypes_raw GROUP BY chromosome ORDER BY chromosome; -- Por cromossomo
-- SHOW PROCESSLIST; -- Ver queries em execução
-- SELECT * FROM system.merges; -- Ver merges em andamento
-- SELECT * FROM system.parts WHERE table = 'genotypes_raw'; -- Ver partes da tabela
-- SELECT formatReadableSize(sum(bytes_on_disk)) as size FROM system.parts WHERE table = 'genotypes_raw'; -- Tamanho da tabela
