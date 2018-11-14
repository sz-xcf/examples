
#include "kafka_consumer.h"

int main (int argc, char **argv) {

	ConsumerRebalanceCb rebalance_cb;
	KafkaEventCb event_cb;
	MessageConsumer msg_consumer("localhost", "5", rebalance_cb, event_cb);
	msg_consumer.set_topics("test3");

	signal(SIGINT, sigterm);
	signal(SIGTERM, sigterm);
	msg_consumer.consume_loop(keep_loop);



	RdKafka::wait_destroyed(5000);

	return 0;
}
