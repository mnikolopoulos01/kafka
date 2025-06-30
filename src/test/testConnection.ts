import { KafkaAdminService } from "../services/KafkaAdminService.js";
import { createKafkaConfig } from "../config/kafkaConfig.js";

async function testKafkaConnection() {
  console.log("Testing Kafka connection...");

  // Update these values to match your Kafka setup
  const config = createKafkaConfig(
    ["192.168.100.164:9092"],
    "test-client",
    "test-group"
  );

  const adminService = new KafkaAdminService(config);

  try {
    console.log("Attempting to connect to Kafka...");

    // Test connection by getting all topics
    const allTopics = await adminService.getAllTopics();
    console.log("✅ Successfully connected to Kafka!");
    console.log(`Found ${allTopics.length} topics:`, allTopics);

    // Test getting flow topics
    const flowTopics = await adminService.getFlowTopics();
    console.log(`Found ${flowTopics.length} flow topics:`, flowTopics);
  } catch (error) {
    console.error("❌ Failed to connect to Kafka:", error);
  } finally {
    await adminService.disconnect();
    console.log("Test completed");
    process.exit(0);
  }
}

testKafkaConnection();
