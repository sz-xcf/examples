
#include "kafka_producer.h"


///////////////////////////////////////////////////////////////////////////////////////////
// class MessageDeliveryReportCb
///////////////////////////////////////////////////////////////////////////////////////////

void MessageDeliveryReportCb::dr_cb (RdKafka::Message &message) {
	// std::cout << "Message delivery for (" << message.len() << " bytes): " <<
		// message.errstr() << std::endl;
	std::cout << "message deliverid: " << static_cast<char*>(message.payload()) << std::endl;
	if (message.key())
		std::cout << "Key: " << *(message.key()) << ";" << std::endl;
}

///////////////////////////////////////////////////////////////////////////////////////////
// class KafkaEventCb
///////////////////////////////////////////////////////////////////////////////////////////

void KafkaEventCb::event_cb (RdKafka::Event &event) {
	switch (event.type())
	{
		case RdKafka::Event::EVENT_ERROR:
			std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
				event.str() << std::endl;
			if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
				// run = false;
				;
			break;

		case RdKafka::Event::EVENT_STATS:
			std::cerr << "\"STATS\": " << event.str() << std::endl;
			break;

		case RdKafka::Event::EVENT_LOG:
			fprintf(stderr, "LOG-%i-%s: %s\n",
					event.severity(), event.fac().c_str(), event.str().c_str());
			break;

		default:
			std::cerr << "EVENT " << event.type() <<
				" (" << RdKafka::err2str(event.err()) << "): " <<
				event.str() << std::endl;
			break;
	}
}


///////////////////////////////////////////////////////////////////////////////////////////
// class KafkaProducer
///////////////////////////////////////////////////////////////////////////////////////////

KafkaProducer::KafkaProducer(std::string brokers, KafkaEventCb& event_cb, MessageDeliveryReportCb& deliver_report_cb):
	partition(RdKafka::Topic::PARTITION_UA),
	topic(nullptr)
{
	std::string errstr;
	conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

	RdKafka::Conf::ConfResult res;
	// queue.buffering.max.ms = 50
	// queue.buffering.max.messages = 50000
	// queue.enqueue.timeout.ms = -1
	// queue.enqueue.timeout.ms = 50
	// request.timeout.ms = 5000
	// message.timeout.ms = 300000
	// res = conf->set("queue.buffering.max.ms", optarg, errstr);
	// res = conf->set("message.send.max.retries", "10000000", errstr);
	// if (res != RdKafka::Conf::CONF_OK) {
		// std::cerr << errstr << std::endl;
		// exit(1);
	// }
	// res = conf->set("request.timeout.ms", "900000", errstr);
	// if (res != RdKafka::Conf::CONF_OK) {
		// std::cerr << errstr << std::endl;
		// exit(1);
	// }
	res = tconf->set("message.timeout.ms", "0", errstr);
	if (res != RdKafka::Conf::CONF_OK) {
		std::cerr << errstr << std::endl;
		exit(1);
	}
	res = conf->set("metadata.broker.list", brokers, errstr);
	if (res != RdKafka::Conf::CONF_OK) {
		std::cerr << errstr << std::endl;
		exit(1);
	}

	conf->set("event_cb", &event_cb, errstr);
	conf->set("dr_cb", &deliver_report_cb, errstr);

	producer = RdKafka::Producer::create(conf, errstr);
	if (!producer) {
		std::cerr << "Failed to create producer: " << errstr << std::endl;
		exit(1);
	}

}

KafkaProducer::~KafkaProducer()
{
	if (topic)
		delete topic;
	delete producer;
}

void KafkaProducer::set_topic(std::string topic_str){

	if (topic)
		delete topic;

	std::string errstr;
	topic = RdKafka::Topic::create(producer, topic_str, tconf, errstr);
	if (!topic) {
		std::cerr << "Failed to create topic: " << errstr << std::endl;
		exit(1);
	}
}

void KafkaProducer::send_msg(std::string msg){
	RdKafka::ErrorCode resp =
		producer->produce(topic, partition,
				RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
				const_cast<char *>(msg.c_str()), msg.size(),
				NULL, NULL);
	if (resp != RdKafka::ERR_NO_ERROR)
		std::cerr << "% Produce failed: " <<
			RdKafka::err2str(resp) << std::endl;
	else
		std::cerr << "% Produced message (" << msg.size() << " bytes)" <<
			std::endl;
}

