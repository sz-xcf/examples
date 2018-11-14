
#include "kafka_consumer.h"


////////////////////////////////////////////////////////////////////////////////////////////////////////
// class KafkaEventCb
////////////////////////////////////////////////////////////////////////////////////////////////////////

void KafkaEventCb::print_time () {
#ifndef _MSC_VER
	struct timeval tv;
	char buf[64];
	gettimeofday(&tv, NULL);
	strftime(buf, sizeof(buf) - 1, "%Y-%m-%d %H:%M:%S", localtime(&tv.tv_sec));
	fprintf(stderr, "%s.%03d: ", buf, (int)(tv.tv_usec / 1000));
#else
	std::wcerr << CTime::GetCurrentTime().Format(_T("%Y-%m-%d %H:%M:%S")).GetString()
		<< ": ";
#endif
}

void KafkaEventCb::event_cb (RdKafka::Event &event) {

	print_time();
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

		case RdKafka::Event::EVENT_THROTTLE:
			std::cerr << "THROTTLED: " << event.throttle_time() << "ms by " <<
				event.broker_name() << " id " << (int)event.broker_id() << std::endl;
			break;

		default:
			std::cerr << "EVENT " << event.type() <<
				" (" << RdKafka::err2str(event.err()) << "): " <<
				event.str() << std::endl;
			break;
	}
}


////////////////////////////////////////////////////////////////////////////////////////////////////////
// class ConsumerRebalanceCb
////////////////////////////////////////////////////////////////////////////////////////////////////////

void ConsumerRebalanceCb::part_list_print (const std::vector<RdKafka::TopicPartition*>&partitions){
	for (unsigned int i = 0 ; i < partitions.size() ; i++)
		std::cerr << partitions[i]->topic() <<
			"[" << partitions[i]->partition() << "], ";
	std::cerr << "\n";
}

void ConsumerRebalanceCb::rebalance_cb (RdKafka::KafkaConsumer *consumer,
		RdKafka::ErrorCode err,
		std::vector<RdKafka::TopicPartition*> &partitions) {
	
	// I set these variable here to decouple the codes, and leave the aftering code as it is
	int eof_cnt = 0;          // this is useless since i have set exit_eof to false
	int partition_cnt = 0;    // this is useless since i have set exit_eof to false

	std::cerr << "RebalanceCb: " << RdKafka::err2str(err) << ": ";

	part_list_print(partitions);

	if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
		consumer->assign(partitions);
		partition_cnt = (int)partitions.size();
	} else {
		consumer->unassign();
		partition_cnt = 0;
	}
	eof_cnt = 0;
}


////////////////////////////////////////////////////////////////////////////////////////////////////////
// class MessageConsumer
////////////////////////////////////////////////////////////////////////////////////////////////////////

void MessageConsumer::msg_show(RdKafka::Message* message, void* opaque) {
	// I set these variable here to decouple the codes, and leave the aftering code as it is
	int verbosity = 1;        // I fix verbosity here
	bool exit_eof = false;    // I don't use eof yet
	int eof_cnt = 0;          // this is useless since i have set exit_eof to false
	int partition_cnt = 0;    // this is useless since i have set exit_eof to false
	switch (message->err()) {
		case RdKafka::ERR__TIMED_OUT:
			break;

		case RdKafka::ERR_NO_ERROR:
			/* Real message */
			// msg_cnt++;
			// msg_bytes += message->len();
			if (verbosity >= 3)
				std::cerr << "Read msg at offset " << message->offset() << std::endl;
			RdKafka::MessageTimestamp ts;
			ts = message->timestamp();
			if (verbosity >= 2 &&
					ts.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
				std::string tsname = "?";
				if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME)
					tsname = "create time";
				else if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_LOG_APPEND_TIME)
					tsname = "log append time";
				std::cout << "Timestamp: " << tsname << " " << ts.timestamp << std::endl;
			}
			if (verbosity >= 2 && message->key()) {
				std::cout << "Key: " << *message->key() << std::endl;
			}
			if (verbosity >= 1) {
				printf("%.*s\n",
						static_cast<int>(message->len()),
						static_cast<const char *>(message->payload()));
			}
			break;

		case RdKafka::ERR__PARTITION_EOF:
			/* Last message */
			if (exit_eof && ++eof_cnt == partition_cnt) {
				std::cerr << "%% EOF reached for all " << partition_cnt <<
					" partition(s)" << std::endl;
				// run = false;
			}
			break;

		case RdKafka::ERR__UNKNOWN_TOPIC:
		case RdKafka::ERR__UNKNOWN_PARTITION:
			std::cerr << "Consume failed: " << message->errstr() << std::endl;
			keep_loop = false;
			break;

		default:
			/* Errors */
			std::cerr << "Consume failed: " << message->errstr() << std::endl;
			keep_loop = false;
	}
}

MessageConsumer::MessageConsumer(std::string brokerslist, std::string groupid,
		ConsumerRebalanceCb& rebalance_cb,
		KafkaEventCb& event_cb):
	brokers(brokerslist),
	run(true)

{
	std::string errstr;
	RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

	conf->set("rebalance_cb", &rebalance_cb, errstr);

	if (conf->set("group.id",  groupid, errstr) != RdKafka::Conf::CONF_OK) {
		std::cerr << errstr << std::endl;
		exit(1);
	}

	if (conf->set("metadata.broker.list", brokers, errstr) != RdKafka::Conf::CONF_OK) {
		std::cerr << errstr << std::endl;
		exit(1);
	}

	conf->set("event_cb", &event_cb, errstr);

	conf->set("default_topic_conf", tconf, errstr);
	delete tconf;

	consumer = RdKafka::KafkaConsumer::create(conf, errstr);
	if (!consumer) {
		std::cerr << "Failed to create consumer: " << errstr << std::endl;
		exit(1);
	}
	delete conf;
}

MessageConsumer::~MessageConsumer()
{
	consumer->close();
	delete consumer;
};

// topic_list: words seperated by " ", e.g. "topic1 topic2"
void MessageConsumer::set_topics(std::string topic_list){
	std::vector<std::string> topics;

	SplitString(topic_list, topics, " ");
	std::cout << "topics: ";
	for(int i=0; i<topics.size(); i++)
		std::cout << topics[i] << " ";
	std::cout << std::endl;

	RdKafka::ErrorCode err = consumer->subscribe(topics);
	if (err) {
		std::cerr << "Failed to subscribe to " << topics.size() << " topics: "
			<< RdKafka::err2str(err) << std::endl;
		exit(1);
	}
}

void MessageConsumer::consume_loop(bool& keep_loop){
	while (keep_loop) {
		RdKafka::Message *msg = consumer->consume(1000);
		msg_show(msg, NULL);
		delete msg;
	}
}

void MessageConsumer::set_run(bool keep_run){
	run = keep_run;
}

void MessageConsumer::SplitString(const std::string& s, std::vector<std::string>& v, const std::string& c)
{
  std::string::size_type pos1, pos2;
  pos2 = s.find(c);
  pos1 = 0;
  while(std::string::npos != pos2)
  {
	v.push_back(s.substr(pos1, pos2-pos1));
 
	pos1 = pos2 + c.size();
	pos2 = s.find(c, pos1);
  }
  if(pos1 != s.length())
	v.push_back(s.substr(pos1));
}

