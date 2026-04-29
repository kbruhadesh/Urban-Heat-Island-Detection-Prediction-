#!/bin/bash
# ── Kafka Setup Script (KRaft mode - no Zookeeper needed) ──────────────────
# Downloads and configures Apache Kafka for local UHI project demo

KAFKA_VERSION="3.7.0"
SCALA_VERSION="2.13"
KAFKA_DIR="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
KAFKA_TGZ="${KAFKA_DIR}.tgz"
KAFKA_URL="https://downloads.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}"

echo "=============================================="
echo "  Kafka Setup for UHI Project"
echo "=============================================="

# ── 1. Download Kafka ──────────────────────────────────────────────────────
if [ -d "$KAFKA_DIR" ]; then
    echo "✅ Kafka already downloaded: $KAFKA_DIR"
else
    echo "📥 Downloading Kafka ${KAFKA_VERSION}..."
    wget -q "$KAFKA_URL" -O "$KAFKA_TGZ"
    if [ $? -ne 0 ]; then
        echo "❌ Download failed. Trying mirror..."
        KAFKA_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}"
        wget -q "$KAFKA_URL" -O "$KAFKA_TGZ"
    fi
    tar -xzf "$KAFKA_TGZ"
    rm -f "$KAFKA_TGZ"
    echo "✅ Kafka extracted to: $KAFKA_DIR"
fi

KAFKA_HOME="$(pwd)/$KAFKA_DIR"

# ── 2. Generate KRaft cluster ID ──────────────────────────────────────────
echo ""
echo "🔧 Configuring KRaft mode..."
CLUSTER_ID=$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)
echo "   Cluster ID: $CLUSTER_ID"

# ── 3. Format storage ─────────────────────────────────────────────────────
$KAFKA_HOME/bin/kafka-storage.sh format \
    -t $CLUSTER_ID \
    -c $KAFKA_HOME/config/kraft/server.properties \
    --ignore-formatted 2>/dev/null

echo "✅ Storage formatted"

# ── 4. Start Kafka (KRaft mode) ───────────────────────────────────────────
echo ""
echo "🚀 Starting Kafka broker..."
$KAFKA_HOME/bin/kafka-server-start.sh -daemon \
    $KAFKA_HOME/config/kraft/server.properties

sleep 5

# ── 5. Create topic ───────────────────────────────────────────────────────
echo "📋 Creating topic: uhi-data"
$KAFKA_HOME/bin/kafka-topics.sh --create \
    --topic uhi-data \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists 2>/dev/null

echo ""
echo "✅ Kafka is running!"
echo ""
echo "── Quick Reference ──"
echo "  Start:  $KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/kraft/server.properties"
echo "  Stop:   $KAFKA_HOME/bin/kafka-server-stop.sh"
echo "  Topics: $KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server localhost:9092"
echo ""
echo "── Next Steps ──"
echo "  1. Run producer:  python3 kafka/producer.py"
echo "  2. Run consumer:  python3 kafka/consumer.py"
