const {Kafka} = require('kafkajs');

class KafkaService {
  constructor() {
    this.kafka = new Kafka({
      clientId: "shinobi-kafka",
      brokers: ["localhost:9092"],
      // retry: 0,
      // requestTimeout: 100
    });
    this.producer = this.kafka.producer();
    this.consumer = this.kafka.consumer({ groupId: "test-group" });
    this.producer.connect();
  }

  async produce(topic, messages) {
    try {
      // await this.producer.connect();
      await this.producer.send({
        topic: topic,
        messages: [{ value: messages }]
      });
      console.log('Message sent successfully');
    } catch (error) {
      console.error(error);
    } finally {
      // await this.producer.disconnect();
    }
  }
  async produceBuffer(topic, bufferData) {
    try {
      await this.producer.connect();
  
      // Convert buffer data to a string
      const dataString = bufferData.toString('base64');
  
      await this.producer.send({
        topic: topic,
        messages: [{ value: dataString }]
      });
  
      console.log('Message sent successfully');
    } catch (error) {
      console.error('Error producing message:', error);
    } finally {
      await this.producer.disconnect();
    }
  }

  async consume(topic, callback) {
    try {
      await this.consumer.connect();
      await this.consumer.subscribe({ topic: topic, fromBeginning: true });
      await this.consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const value = message.value.toString();
          callback(value);
        },
      });
    } catch (error) {
      console.error(error);
    }
  }
}

module.exports = {KafkaService};