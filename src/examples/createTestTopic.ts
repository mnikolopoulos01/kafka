import { KafkaAdminService } from "../services/KafkaAdminService.js";
import { KafkaProducerService } from "../services/KafkaProducerService.js";
import { defaultKafkaConfig } from "../config/kafkaConfig.js";

async function createTestTopicAndSendMessages() {
  const adminService = new KafkaAdminService(defaultKafkaConfig);
  const producerService = new KafkaProducerService(defaultKafkaConfig);
  
  try {
    console.log("🧪 Creating test flow topic and sending messages...");
    
    // Test topic name following the pattern
    const testOrgUsrNode = "test-user-flow";
    const testTopic = `${testOrgUsrNode}-topic`;
    
    console.log(`📝 Test topic: ${testTopic}`);
    
    // Check if topic already exists
    const allTopics = await adminService.getAllTopics();
    if (!allTopics.includes(testTopic)) {
      console.log("⚠️  Topic doesn't exist. You'll need to create it manually in Kafka.");
      console.log(`   Topic name: ${testTopic}`);
      console.log("   Or use Kafka CLI:");
      console.log(`   kafka-topics.sh --create --topic ${testTopic} --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1`);
    } else {
      console.log("✅ Topic already exists");
    }
    
    // Send test messages
    console.log("📤 Sending test messages...");
    
    const testMessages = [
      { message: "Hello from NiFi flow!", timestamp: new Date().toISOString() },
      { message: "Processing data...", data: { id: 1, value: "test" } },
      { message: "Flow completed successfully", status: "success" }
    ];
    
    for (let i = 0; i < testMessages.length; i++) {
      const success = await producerService.sendToFlowTopic(
        testOrgUsrNode,
        testMessages[i],
        `test-key-${i}`
      );
      
      if (success) {
        console.log(`✅ Message ${i + 1} sent successfully`);
      } else {
        console.log(`❌ Failed to send message ${i + 1}`);
      }
      
      // Wait a bit between messages
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    console.log("🎉 Test messages sent! Check your monitor for outputs.");
    
  } catch (error) {
    console.error("❌ Error in test:", error);
  } finally {
    await producerService.disconnect();
    await adminService.disconnect();
  }
}

createTestTopicAndSendMessages();