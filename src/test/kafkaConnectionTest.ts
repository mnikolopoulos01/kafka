import { defaultKafkaConfig } from "../config/kafkaConfig.js";

console.log("🔌 Kafka Connection Test Starting...");
console.log("📡 Broker configuration:", defaultKafkaConfig.connectionConfig.brokers);

async function testKafkaConnection() {
  try {
    console.log("📦 Importing KafkaJS...");
    const { Kafka } = await import("kafkajs");
    console.log("✅ KafkaJS imported successfully");
    
    console.log("🔧 Creating Kafka client...");
    const kafka = new Kafka(defaultKafkaConfig.connectionConfig);
    console.log("✅ Kafka client created");
    
    console.log("👑 Creating admin client...");
    const admin = kafka.admin();
    console.log("✅ Admin client created");
    
    console.log("🔗 Attempting to connect to Kafka...");
    await admin.connect();
    console.log("✅ Connected to Kafka successfully!");
    
    console.log("📋 Fetching topics...");
    const topics = await admin.listTopics();
    console.log(`✅ Found ${topics.length} topics:`, topics);
    
    console.log("🔌 Disconnecting...");
    await admin.disconnect();
    console.log("✅ Disconnected successfully");
    
    console.log("🎉 Kafka connection test PASSED!");
    
  } catch (error) {
    console.error("❌ Kafka connection test FAILED:");
    console.error("Error details:", error);
    
    if (error.message?.includes('ECONNREFUSED')) {
      console.log("\n🔧 Connection refused troubleshooting:");
      console.log("   1. Is Kafka running?");
      console.log("   2. Is the broker address correct?", defaultKafkaConfig.connectionConfig.brokers);
      console.log("   3. Is port 9092 accessible?");
      console.log("   4. Try: telnet 192.168.100.164 9092");
    }
    
    if (error.message?.includes('timeout')) {
      console.log("\n⏰ Timeout troubleshooting:");
      console.log("   1. Network connectivity issues");
      console.log("   2. Firewall blocking the connection");
      console.log("   3. Kafka broker not responding");
    }
  }
}

testKafkaConnection();